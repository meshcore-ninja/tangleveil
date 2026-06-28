use std::{
    collections::HashMap,
    sync::Arc,
    time::Instant,
};

use axum::{
    extract::{
        State,
        ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
    },
    response::{Html, Response},
};
use futures_util::StreamExt;
use serde::Serialize;
use tokio::time::{self, Duration};
use tracing::info;

use crate::state::{AppState, SourceRuntime};

const TELEMETRY_INTERVAL: Duration = Duration::from_secs(1);
const TELEMETRY_CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Serialize)]
pub struct SourceTelemetry {
    pub id: String,
    pub url: String,
    pub state: crate::connection_state::ConnectionState,
    pub packets: f64,
    pub bytes: f64,
    pub connected_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TelemetrySnapshot {
    pub sources: Vec<SourceTelemetry>,
    pub pps: f64,
    pub kbps: f64,
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub clients: usize,
}

pub fn channel() -> (
    tokio::sync::broadcast::Sender<Arc<str>>,
    tokio::sync::broadcast::Receiver<Arc<str>>,
) {
    tokio::sync::broadcast::channel(TELEMETRY_CHANNEL_CAPACITY)
}

pub fn spawn_broadcaster(state: AppState) {
    tokio::spawn(async move {
        let mut last_packets = HashMap::new();
        let mut last_bytes = HashMap::new();
        let mut last_sample = Instant::now();
        let mut last_cpu_seconds = metrics_process::collector::collect()
            .cpu_seconds_total
            .unwrap_or(0.0);
        let mut interval = time::interval(TELEMETRY_INTERVAL);
        interval.tick().await;

        loop {
            interval.tick().await;

            let now = Instant::now();
            let elapsed_secs = now.duration_since(last_sample).as_secs_f64().max(0.001);
            last_sample = now;

            let started = Instant::now();
            let snapshot = build_snapshot(
                &state,
                &mut last_packets,
                &mut last_bytes,
                &mut last_cpu_seconds,
                elapsed_secs,
            );
            let generated_in = started.elapsed();

            let serialize_started = Instant::now();
            let Ok(payload) = serde_json::to_string(&snapshot) else {
                continue;
            };
            let serialized_in = serialize_started.elapsed();
            let shared = Arc::<str>::from(payload);

            if state.verbose {
                info!(
                    subscribers = state.telemetry_tx.receiver_count(),
                    sources = snapshot.sources.len(),
                    payload_bytes = shared.len(),
                    generate_us = generated_in.as_micros(),
                    serialize_us = serialized_in.as_micros(),
                    total_us = started.elapsed().as_micros(),
                    "telemetry snapshot broadcast"
                );
            }

            if let Ok(mut latest) = state.latest_telemetry.write() {
                *latest = Some(Arc::clone(&shared));
            }

            let _ = state.telemetry_tx.send(shared);
        }
    });
}

pub async fn index_page(State(state): State<AppState>) -> Html<String> {
    let html = state.static_html.read().expect("static html lock poisoned").to_string();
    Html(html)
}

pub async fn telemetry_ws(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| serve_telemetry_client(socket, state))
}

async fn serve_telemetry_client(mut socket: WebSocket, state: AppState) {
    let initial = state
        .latest_telemetry
        .read()
        .ok()
        .and_then(|guard| guard.clone());

    if let Some(payload) = initial
        && socket
            .send(Message::Text(Utf8Bytes::from(payload.as_ref())))
            .await
            .is_err()
    {
        return;
    }

    let mut rx = state.telemetry_tx.subscribe();

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(payload) => {
                        if socket.send(Message::Text(Utf8Bytes::from(payload.as_ref()))).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.next() => {
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

fn build_snapshot(
    state: &AppState,
    last_packets: &mut HashMap<String, u64>,
    last_bytes: &mut HashMap<String, u64>,
    last_cpu_seconds: &mut f64,
    elapsed_secs: f64,
) -> TelemetrySnapshot {
    let sources_guard = state.sources.read().expect("sources lock poisoned");
    let mut sources = sources_guard
        .iter()
        .map(|(id, runtime)| source_telemetry(id, runtime, last_packets, last_bytes, elapsed_secs))
        .collect::<Vec<_>>();
    sources.sort_by(|a, b| a.id.cmp(&b.id));

    let source_clients = sources_guard
        .values()
        .map(|runtime| runtime.raw_tx.receiver_count())
        .sum::<usize>();
    drop(sources_guard);

    let pps = sources.iter().map(|source| source.packets).sum();
    // `bytes` is already a per-second rate; convert to kilobits per second.
    let kbps = sources.iter().map(|source| source.bytes).sum::<f64>() * 8.0 / 1000.0;

    let process = metrics_process::collector::collect();
    let cpu_seconds_total = process.cpu_seconds_total.unwrap_or(*last_cpu_seconds);
    let cpu_percent = ((cpu_seconds_total - *last_cpu_seconds) / elapsed_secs * 100.0).max(0.0);
    *last_cpu_seconds = cpu_seconds_total;

    TelemetrySnapshot {
        sources,
        pps,
        kbps,
        cpu_percent,
        memory_bytes: process.resident_memory_bytes.unwrap_or(0),
        clients: state.multiplex_tx.receiver_count() + source_clients,
    }
}

fn source_telemetry(
    id: &str,
    runtime: &SourceRuntime,
    last_packets: &mut HashMap<String, u64>,
    last_bytes: &mut HashMap<String, u64>,
    elapsed_secs: f64,
) -> SourceTelemetry {
    let total_packets = runtime.total_packets();
    let total_bytes = runtime.total_bytes();

    let packet_delta = last_packets
        .insert(id.to_owned(), total_packets)
        .map(|previous| total_packets.saturating_sub(previous))
        .unwrap_or(0);
    let byte_delta = last_bytes
        .insert(id.to_owned(), total_bytes)
        .map(|previous| total_bytes.saturating_sub(previous))
        .unwrap_or(0);

    SourceTelemetry {
        id: id.to_owned(),
        url: runtime.url(),
        state: runtime.state(),
        packets: packet_delta as f64 / elapsed_secs,
        bytes: byte_delta as f64 / elapsed_secs,
        connected_secs: runtime.connected_secs(),
    }
}
