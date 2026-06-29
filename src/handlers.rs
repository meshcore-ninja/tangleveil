use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path, Query, State, WebSocketUpgrade, ws::{CloseFrame, Message, Utf8Bytes, WebSocket}},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    admin,
    config::DedupPolicy,
    connection_state::ConnectionState,
    dedup::DedupCache,
    frame::RawFrame,
    jaq_filter::Program as JaqProgram,
    multiplex::MultiplexFrame,
    state::AppState,
    telemetry,
};

#[derive(Debug, Deserialize)]
struct MultiplexQuery {
    #[serde(default)]
    source: Vec<String>,
    #[serde(default)]
    sources: Option<String>,
    /// Comma-separated MeshCore payload types to keep (e.g. `ADVERT,REQ`).
    /// Absent or empty means no payload-type filtering. Matched case-insensitively.
    #[serde(default, rename = "payloadTypes")]
    payload_types: Option<String>,
    /// When set, drop frames whose CoreScope content hash was already sent to
    /// this client within the dedup window (collapses the same packet seen by
    /// many observers/sources into one).
    #[serde(default, rename = "dedupByContent", deserialize_with = "de_bool_flag")]
    dedup_by_content: bool,
    /// Override the dedup window length (seconds); clamped to a sane range.
    #[serde(default, rename = "dedupWindowSecs")]
    dedup_window_secs: Option<u64>,
    /// Experimental: a jq program run over each frame's JSON projection. No
    /// output drops the frame; each output value is sent as its own message.
    #[serde(default)]
    jaq: Option<String>,
    #[serde(default, deserialize_with = "de_bool_flag")]
    binary: bool,
}

/// Accept the usual truthy spellings for a query flag: `?binary=1`, `binary=true`,
/// or a bare `?binary` (empty value).
fn de_bool_flag<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(match raw.as_deref() {
        None => false,
        Some(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "" | "1" | "true" | "yes" | "on"),
    })
}

impl MultiplexQuery {
    fn requested_source_ids(&self) -> Vec<String> {
        let mut ids = self.source.clone();
        if let Some(csv) = &self.sources {
            ids.extend(
                csv.split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(str::to_owned),
            );
        }

        let mut seen = HashSet::new();
        ids.into_iter()
            .filter(|id| seen.insert(id.clone()))
            .collect()
    }

    /// The set of payload types to keep, upper-cased for case-insensitive
    /// matching against [`MultiplexFrame::payload_type`]. `None` when no filter
    /// was requested (or only blanks were given), so the hot path skips the
    /// check entirely.
    fn requested_payload_types(&self) -> Option<HashSet<String>> {
        let csv = self.payload_types.as_deref()?;
        let types: HashSet<String> = csv
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| part.to_ascii_uppercase())
            .collect();
        (!types.is_empty()).then_some(types)
    }

    /// A fresh dedup cache when `?dedupByContent` is set, else `None`. The window
    /// defaults to the configured `dedup_window_secs` and a client-supplied
    /// `?dedupWindowSecs` is clamped to `1..=dedup_max_window_secs`.
    fn dedup_cache(&self, policy: &DedupPolicy) -> Option<DedupCache> {
        if !self.dedup_by_content {
            return None;
        }
        let max = policy.max_window.as_secs().max(1);
        let default = policy.default_window.as_secs().clamp(1, max);
        let secs = self
            .dedup_window_secs
            .map(|requested| requested.clamp(1, max))
            .unwrap_or(default);
        Some(DedupCache::new(Duration::from_secs(secs)))
    }
}

#[derive(Debug, Serialize)]
struct SourceStatus {
    id: String,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mapping: Option<String>,
    state: ConnectionState,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(telemetry::index_page))
        .route("/health", get(health))
        .route("/metrics", get(prometheus_metrics))
        .route("/sources", get(list_sources))
        .route("/ws/telemetry", get(telemetry::telemetry_ws))
        .route("/ws", get(multiplex_ws))
        .route("/ws/{source}", get(source_ws))
        .nest("/admin", admin::router())
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    // Process metrics (CPU, memory, fds, threads) are sampled only on scrape,
    // not on a one-second polling loop.
    state.metrics.process.collect();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        state.metrics.prometheus.render(),
    )
}

async fn list_sources(State(state): State<AppState>) -> Json<Vec<SourceStatus>> {
    let sources_guard = state.sources.read().expect("sources lock poisoned");
    let mut sources = sources_guard
        .iter()
        .map(|(id, runtime)| SourceStatus {
            id: id.clone(),
            url: runtime.url(),
            mapping: runtime.mapping(),
            state: runtime.state(),
        })
        .collect::<Vec<_>>();
    sources.sort_by(|a, b| a.id.cmp(&b.id));
    Json(sources)
}

async fn multiplex_ws(
    ws: WebSocketUpgrade,
    Query(query): Query<MultiplexQuery>,
    State(state): State<AppState>,
) -> Response {
    let binary = query.binary;
    let payload_type_filter = query.requested_payload_types();
    let dedup_cache = {
        let policy = state.dedup.read().expect("dedup policy lock poisoned");
        query.dedup_cache(&policy)
    };

    // Compile the experimental jq program up front so a bad program is rejected
    // with 400 at handshake time rather than silently dropping every frame. The
    // compiled program is `Send` (jaq-json `sync` feature), so it can move into
    // the websocket task. This runs synchronously, before any `.await`, so the
    // non-`Send` parts of compilation never cross an await point.
    let jaq_program = match query.jaq.as_deref().filter(|p| !p.trim().is_empty()) {
        Some(program) => match JaqProgram::compile(program) {
            Ok(program) => Some(Arc::new(program)),
            Err(err) => {
                return (StatusCode::BAD_REQUEST, format!("invalid jaq program: {err}"))
                    .into_response();
            }
        },
        None => None,
    };

    let requested = query.requested_source_ids();
    let source_filter = if requested.is_empty() {
        None
    } else {
        let sources = state.sources.read().expect("sources lock poisoned");
        let mut unknown = Vec::new();
        for id in &requested {
            if !sources.contains_key(id) {
                unknown.push(id.as_str());
            }
        }
        if !unknown.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                format!("unknown source(s): {}", unknown.join(", ")),
            )
                .into_response();
        }
        Some(requested.into_iter().collect::<HashSet<_>>())
    };

    ws.on_upgrade(move |socket| {
        serve_multiplex_client(
            socket,
            state.multiplex_tx.subscribe(),
            source_filter,
            payload_type_filter,
            dedup_cache,
            jaq_program,
            binary,
        )
    })
}

async fn source_ws(
    Path(source): Path<String>,
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    let runtime = {
        let sources = state.sources.read().expect("sources lock poisoned");
        sources.get(&source).cloned()
    };
    let Some(runtime) = runtime else {
        return (StatusCode::NOT_FOUND, "unknown source").into_response();
    };

    ws.on_upgrade(move |socket| serve_raw_client(socket, source, runtime.raw_tx.subscribe()))
}

async fn serve_multiplex_client(
    mut socket: WebSocket,
    mut rx: broadcast::Receiver<Arc<MultiplexFrame>>,
    source_filter: Option<HashSet<String>>,
    payload_type_filter: Option<HashSet<String>>,
    mut dedup_cache: Option<DedupCache>,
    jaq_program: Option<Arc<JaqProgram>>,
    binary: bool,
) {
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(frame) => {
                        if let Some(filter) = &source_filter {
                            let Some(source_id) = frame.source_id() else {
                                continue;
                            };
                            if !filter.contains(source_id) {
                                continue;
                            }
                        }

                        if let Some(filter) = &payload_type_filter {
                            // `payload_type()` parses once per frame and caches the
                            // result, so every filtering client reuses the same work.
                            match frame.payload_type() {
                                Some(payload_type) if filter.contains(payload_type) => {}
                                _ => continue,
                            }
                        }

                        if let Some(dedup) = &mut dedup_cache {
                            // Frames without a content hash can't be deduped, so
                            // they pass through unchanged.
                            if let Some(hash) = frame.content_hash()
                                && !dedup.admit(hash)
                            {
                                continue;
                            }
                        }

                        if let Some(program) = &jaq_program {
                            // jaq always emits JSON. Run it on the shared, cached
                            // projection; the program may yield zero values (drop
                            // the frame) or several (one message each).
                            let Some(json) = frame.json() else {
                                continue;
                            };
                            match program.run_json(json.as_str()) {
                                Ok(outputs) => {
                                    for out in outputs {
                                        if socket.send(Message::Text(Utf8Bytes::from(out))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                // A per-frame runtime error (e.g. a type error)
                                // shouldn't kill the client; skip just this frame.
                                Err(err) => warn!(%err, "jaq program error on frame"),
                            }
                            continue;
                        }

                        let message = if binary {
                            Message::Binary(frame.binary())
                        } else {
                            // Computed once per frame and shared across all JSON
                            // clients; skip frames that can't be projected rather
                            // than dropping the whole client.
                            match frame.json() {
                                Some(json) => Message::Text(json),
                                None => continue,
                            }
                        };

                        if socket.send(message).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "disconnecting slow multiplex client");
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code: 1013,
                            reason: Utf8Bytes::from(format!("consumer too slow; skipped {skipped} messages")),
                        }))).await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

async fn serve_raw_client(
    mut socket: WebSocket,
    source: String,
    mut rx: broadcast::Receiver<RawFrame>,
) {
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(frame) => {
                        if socket.send(frame.into_axum_message()).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(%source, skipped, "disconnecting slow source client");
                        let _ = socket.send(Message::Close(Some(CloseFrame {
                            code: 1013,
                            reason: Utf8Bytes::from(format!("consumer too slow; skipped {skipped} messages")),
                        }))).await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}
