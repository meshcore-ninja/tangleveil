use std::collections::HashSet;

use axum::{
    Json, Router,
    extract::{Path, Query, State, WebSocketUpgrade, ws::{CloseFrame, Message, Utf8Bytes, WebSocket}},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    admin,
    connection_state::ConnectionState,
    frame::RawFrame,
    multiplex::source_id_from_multiplex_frame,
    state::AppState,
    telemetry,
};

#[derive(Debug, Deserialize)]
struct MultiplexQuery {
    #[serde(default)]
    source: Vec<String>,
    #[serde(default)]
    sources: Option<String>,
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
}

#[derive(Debug, Serialize)]
struct SourceStatus {
    id: String,
    url: String,
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
        serve_multiplex_client(socket, state.multiplex_tx.subscribe(), source_filter)
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
    mut rx: broadcast::Receiver<Bytes>,
    source_filter: Option<HashSet<String>>,
) {
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(frame) => {
                        if let Some(filter) = &source_filter {
                            let Some(source_id) = source_id_from_multiplex_frame(&frame) else {
                                continue;
                            };
                            if !filter.contains(source_id) {
                                continue;
                            }
                        }

                        if socket.send(Message::Binary(frame)).await.is_err() {
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
