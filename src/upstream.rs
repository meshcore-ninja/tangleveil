use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use anyhow::{Context, Result};
use axum::extract::ws::Utf8Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{sync::broadcast, time::sleep};
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::{
        Message as UpstreamMessage,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    config::ReconnectPolicy,
    connection_state::ConnectionState,
    frame::RawFrame,
    metrics::ThroughputMetrics,
    multiplex::{MultiplexFrame, encode_multiplex_frame, unix_time_ms},
    state::SourceRuntime,
};

pub async fn run_source_forever(
    runtime: Arc<SourceRuntime>,
    multiplex_tx: broadcast::Sender<Arc<MultiplexFrame>>,
    throughput: Arc<ThroughputMetrics>,
    reconnect: Arc<std::sync::RwLock<ReconnectPolicy>>,
    user_agent: Arc<std::sync::RwLock<String>>,
    ignore_ssl_certificate_errors: Arc<std::sync::RwLock<bool>>,
    cancel: CancellationToken,
) {
    let mut reconnect_delay = reconnect
        .read()
        .expect("reconnect policy lock poisoned")
        .initial_delay;
    let mut offline_since: Option<Instant> = None;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        let source = runtime.config_snapshot();
        if source.disabled {
            runtime.set_disabled();
            wait_for_cancel_or_config(&cancel).await;
            continue;
        }

        match connect_source(
            &source,
            &runtime,
            &user_agent,
            &ignore_ssl_certificate_errors,
            &cancel,
        )
        .await
        {
            Some(Ok(stream)) => {
                reconnect_delay = reconnect
                    .read()
                    .expect("reconnect policy lock poisoned")
                    .initial_delay;
                info!(source = %source.id, "connected to upstream");

                if let Err(error) = forward_source(
                    &source,
                    &runtime,
                    &multiplex_tx,
                    &throughput,
                    stream,
                    &cancel,
                )
                .await
                {
                    warn!(source = %source.id, %error, "upstream disconnected");
                    runtime.record_error();
                }

                runtime.mark_disconnected();
                runtime.set_state(ConnectionState::Disconnected);
                offline_since = Some(Instant::now());
            }
            Some(Err(error)) => {
                warn!(source = %source.id, %error, "could not connect to upstream");
                runtime.record_error();
                if offline_since.is_none() {
                    offline_since = Some(Instant::now());
                }
            }
            None => return,
        }

        if cancel.is_cancelled() {
            return;
        }

        let policy = reconnect
            .read()
            .expect("reconnect policy lock poisoned")
            .clone();
        let degraded =
            offline_since.is_some_and(|since| since.elapsed() >= policy.offline_threshold);

        runtime.set_state(if degraded {
            ConnectionState::ReconnectWaitDegraded
        } else {
            ConnectionState::ReconnectWait
        });
        runtime.record_reconnect();

        let wait = if degraded {
            policy.offline_delay
        } else {
            reconnect_delay
        };

        if sleep_or_cancel(wait, &cancel).await {
            return;
        }

        if wait == reconnect_delay {
            let max_delay = reconnect
                .read()
                .expect("reconnect policy lock poisoned")
                .max_delay;
            reconnect_delay = (reconnect_delay * 2).min(max_delay);
        }
    }
}

async fn wait_for_cancel_or_config(cancel: &CancellationToken) {
    if cancel.is_cancelled() {
        return;
    }
    sleep_or_cancel(std::time::Duration::from_secs(1), cancel).await;
}

async fn sleep_or_cancel(duration: std::time::Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = sleep(duration) => false,
    }
}

async fn connect_source(
    source: &crate::config::SourceConfig,
    runtime: &SourceRuntime,
    user_agent: &Arc<std::sync::RwLock<String>>,
    ignore_ssl_certificate_errors: &Arc<std::sync::RwLock<bool>>,
    cancel: &CancellationToken,
) -> Option<
    Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
> {
    if cancel.is_cancelled() {
        return None;
    }

    runtime.set_state(ConnectionState::Connecting);

    let mut request = match source.url.as_str().into_client_request() {
        Ok(request) => request,
        Err(error) => {
            runtime.mark_disconnected();
            runtime.set_state(ConnectionState::Disconnected);
            return Some(Err(error.into()));
        }
    };

    if let Ok(ua) = user_agent.read() {
        if !ua.is_empty() {
            match HeaderValue::from_str(ua.as_str()) {
                Ok(value) => {
                    request
                        .headers_mut()
                        .insert(HeaderName::from_static("user-agent"), value);
                }
                Err(error) => {
                    runtime.mark_disconnected();
                    runtime.set_state(ConnectionState::Disconnected);
                    return Some(Err(error.into()));
                }
            }
        }
    }

    for (name, value) in &source.headers {
        let name = match HeaderName::from_bytes(name.as_bytes()) {
            Ok(name) => name,
            Err(error) => {
                runtime.mark_disconnected();
                runtime.set_state(ConnectionState::Disconnected);
                return Some(Err(error.into()));
            }
        };
        let value = match HeaderValue::from_str(value) {
            Ok(value) => value,
            Err(error) => {
                runtime.mark_disconnected();
                runtime.set_state(ConnectionState::Disconnected);
                return Some(Err(error.into()));
            }
        };
        request.headers_mut().insert(name, value);
    }

    runtime.set_state(ConnectionState::Handshaking);
    let ignore_ssl_certificate_errors = ignore_ssl_certificate_errors
        .read()
        .map(|value| *value)
        .unwrap_or(true);
    let connector = ignore_ssl_certificate_errors.then(crate::tls::insecure_connector);

    let result = if let Some(proxy) = source.proxy.as_deref() {
        crate::proxy::connect_via_proxy(request, proxy, ignore_ssl_certificate_errors)
            .await
            .with_context(|| {
                format!(
                    "WebSocket handshake failed for source {} via proxy",
                    source.id
                )
            })
    } else {
        connect_async_tls_with_config(request, None, false, connector)
            .await
            .with_context(|| format!("WebSocket handshake failed for source {}", source.id))
    };

    match result {
        Ok((stream, _response)) => {
            runtime.set_state(ConnectionState::Connected);
            runtime.mark_connected();
            Some(Ok(stream))
        }
        Err(error) => {
            runtime.mark_disconnected();
            runtime.set_state(ConnectionState::Disconnected);
            Some(Err(error))
        }
    }
}

async fn forward_source<S>(
    source: &crate::config::SourceConfig,
    runtime: &SourceRuntime,
    multiplex_tx: &broadcast::Sender<Arc<MultiplexFrame>>,
    throughput: &ThroughputMetrics,
    stream: tokio_tungstenite::WebSocketStream<S>,
    cancel: &CancellationToken,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut writer, mut reader) = stream.split();

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            message = reader.next() => {
                match message {
                    Some(Ok(UpstreamMessage::Text(text))) => {
                        let frame = RawFrame::Text(Utf8Bytes::from(text.as_str()));
                        publish_frame(source, runtime, multiplex_tx, throughput, frame);
                    }
                    Some(Ok(UpstreamMessage::Binary(binary))) => {
                        let frame = RawFrame::Binary(binary);
                        publish_frame(source, runtime, multiplex_tx, throughput, frame);
                    }
                    Some(Ok(UpstreamMessage::Ping(payload))) => {
                        writer.send(UpstreamMessage::Pong(payload)).await?;
                    }
                    Some(Ok(UpstreamMessage::Pong(_))) => {}
                    Some(Ok(UpstreamMessage::Close(frame))) => {
                        info!(source = %source.id, ?frame, "upstream closed WebSocket");
                        return Ok(());
                    }
                    Some(Ok(UpstreamMessage::Frame(_))) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => return Ok(()),
                }
            }
        }
    }
}

fn publish_frame(
    source: &crate::config::SourceConfig,
    runtime: &SourceRuntime,
    multiplex_tx: &broadcast::Sender<Arc<MultiplexFrame>>,
    throughput: &ThroughputMetrics,
    frame: RawFrame,
) {
    let payload_bytes = frame.payload().len() as u64;
    throughput.record_packet(payload_bytes);
    runtime.record_packet(payload_bytes);

    let sequence = runtime.sequence.fetch_add(1, Ordering::Relaxed);
    let received_at_ms = unix_time_ms();

    let _ = runtime.raw_tx.send(frame.clone());

    let multiplexed = encode_multiplex_frame(
        &source.id,
        sequence,
        received_at_ms,
        frame.kind(),
        frame.payload(),
    );
    let _ = multiplex_tx.send(Arc::new(MultiplexFrame::new(multiplexed)));
}
