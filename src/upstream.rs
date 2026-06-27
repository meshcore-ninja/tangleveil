use std::{
    sync::{
        Arc,
        atomic::Ordering,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use axum::extract::ws::Utf8Bytes;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{sync::broadcast, time::sleep};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message as UpstreamMessage,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};
use tracing::{info, warn};

use crate::{
    config::SourceConfig,
    frame::RawFrame,
    metrics::ThroughputMetrics,
    multiplex::{encode_multiplex_frame, unix_time_ms},
    state::SourceRuntime,
};

const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub async fn run_source_forever(
    source: SourceConfig,
    runtime: Arc<SourceRuntime>,
    multiplex_tx: broadcast::Sender<Bytes>,
    throughput: Arc<ThroughputMetrics>,
) {
    let mut reconnect_delay = Duration::from_secs(1);

    loop {
        match connect_source(&source).await {
            Ok(stream) => {
                runtime.connected.store(true, Ordering::Release);
                reconnect_delay = Duration::from_secs(1);
                info!(source = %source.id, "connected to upstream");

                if let Err(error) =
                    forward_source(&source, &runtime, &multiplex_tx, &throughput, stream).await
                {
                    warn!(source = %source.id, %error, "upstream disconnected");
                }
            }
            Err(error) => {
                warn!(source = %source.id, %error, "could not connect to upstream");
            }
        }

        runtime.connected.store(false, Ordering::Release);
        sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn connect_source(
    source: &SourceConfig,
) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let mut request = source
        .url
        .as_str()
        .into_client_request()
        .with_context(|| format!("invalid WebSocket URL for source {}", source.id))?;

    for (name, value) in &source.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid header name {name:?} for source {}", source.id))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid header value for source {}", source.id))?;
        request.headers_mut().insert(name, value);
    }

    let (stream, _response) = connect_async(request)
        .await
        .with_context(|| format!("WebSocket handshake failed for source {}", source.id))?;
    Ok(stream)
}

async fn forward_source<S>(
    source: &SourceConfig,
    runtime: &SourceRuntime,
    multiplex_tx: &broadcast::Sender<Bytes>,
    throughput: &ThroughputMetrics,
    stream: tokio_tungstenite::WebSocketStream<S>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut writer, mut reader) = stream.split();

    while let Some(message) = reader.next().await {
        match message? {
            UpstreamMessage::Text(text) => {
                let frame = RawFrame::Text(Utf8Bytes::from(text.as_str()));
                publish_frame(source, runtime, multiplex_tx, throughput, frame);
            }
            UpstreamMessage::Binary(binary) => {
                let frame = RawFrame::Binary(binary);
                publish_frame(source, runtime, multiplex_tx, throughput, frame);
            }
            UpstreamMessage::Ping(payload) => {
                writer.send(UpstreamMessage::Pong(payload)).await?;
            }
            UpstreamMessage::Pong(_) => {}
            UpstreamMessage::Close(frame) => {
                info!(source = %source.id, ?frame, "upstream closed WebSocket");
                return Ok(());
            }
            UpstreamMessage::Frame(_) => {}
        }
    }

    Ok(())
}

fn publish_frame(
    source: &SourceConfig,
    runtime: &SourceRuntime,
    multiplex_tx: &broadcast::Sender<Bytes>,
    throughput: &ThroughputMetrics,
    frame: RawFrame,
) {
    let payload_bytes = frame.payload().len() as u64;
    throughput.record_packet(payload_bytes);

    let sequence = runtime.sequence.fetch_add(1, Ordering::Relaxed);
    let received_at_ms = unix_time_ms();

    // Exact source stream. A send error only means there are currently no subscribers.
    let _ = runtime.raw_tx.send(frame.clone());

    // Combined stream. The envelope is encoded once and shared by all downstream clients.
    let multiplexed = encode_multiplex_frame(
        &source.id,
        sequence,
        received_at_ms,
        frame.kind(),
        frame.payload(),
    );
    let _ = multiplex_tx.send(multiplexed);
}
