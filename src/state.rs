use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};

use metrics::{Counter, Gauge, counter, gauge};
use tokio::sync::{Mutex, broadcast};

use crate::{
    config::{DedupPolicy, ReconnectPolicy, SourceConfig},
    connection_state::ConnectionState,
    frame::RawFrame,
    metrics::{TelemetryMetrics, ThroughputMetrics},
    multiplex::{MultiplexFrame, unix_time_ms},
    sources::SourceSupervisor,
};

/// Cumulative Prometheus handles for one source. Created once per source id;
/// updating them is then just a counter/gauge write, not a label rebuild.
pub struct SourceMetrics {
    pub packets: Counter,
    pub bytes: Counter,
    pub connected: Gauge,
    pub reconnects: Counter,
    pub errors: Counter,
    pub last_packet_timestamp: Gauge,
}

impl SourceMetrics {
    fn new(id: &str) -> Self {
        Self {
            packets: counter!("corescope_relay_packets_total", "source" => id.to_owned()),
            bytes: counter!("corescope_relay_bytes_total", "source" => id.to_owned()),
            connected: gauge!("corescope_relay_source_connected", "source" => id.to_owned()),
            reconnects: counter!("corescope_relay_reconnects_total", "source" => id.to_owned()),
            errors: counter!("corescope_relay_source_errors_total", "source" => id.to_owned()),
            last_packet_timestamp: gauge!(
                "corescope_relay_last_packet_timestamp_seconds",
                "source" => id.to_owned()
            ),
        }
    }
}

pub struct SourceRuntime {
    config: RwLock<SourceConfig>,
    pub raw_tx: broadcast::Sender<RawFrame>,
    state: AtomicU8,
    pub sequence: AtomicU64,
    packets: AtomicU64,
    bytes: AtomicU64,
    connected_since_ms: AtomicU64,
    last_packet_ms: AtomicU64,
    metrics: SourceMetrics,
}

impl SourceRuntime {
    pub fn new(source: SourceConfig, channel_capacity: usize) -> Self {
        let (raw_tx, _) = broadcast::channel(channel_capacity);
        let metrics = SourceMetrics::new(&source.id);
        Self {
            config: RwLock::new(source),
            raw_tx,
            state: AtomicU8::new(ConnectionState::Disconnected as u8),
            sequence: AtomicU64::new(0),
            packets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            connected_since_ms: AtomicU64::new(0),
            last_packet_ms: AtomicU64::new(0),
            metrics,
        }
    }

    pub fn id(&self) -> String {
        self.config
            .read()
            .expect("source config lock poisoned")
            .id
            .clone()
    }

    pub fn url(&self) -> String {
        self.config
            .read()
            .expect("source config lock poisoned")
            .url
            .clone()
    }

    pub fn mapping(&self) -> Option<String> {
        self.config
            .read()
            .expect("source config lock poisoned")
            .mapping
            .clone()
    }

    pub fn config_snapshot(&self) -> SourceConfig {
        self.config
            .read()
            .expect("source config lock poisoned")
            .clone()
    }

    pub fn update_config(&self, source: SourceConfig) {
        *self.config.write().expect("source config lock poisoned") = source;
    }

    pub fn set_disabled(&self) {
        self.mark_disconnected();
        self.set_state(ConnectionState::Disabled);
    }

    pub fn set_state(&self, state: ConnectionState) {
        self.state.store(state as u8, Ordering::Release);
    }

    pub fn state(&self) -> ConnectionState {
        ConnectionState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn record_packet(&self, payload_bytes: u64) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(payload_bytes, Ordering::Relaxed);

        self.metrics.packets.increment(1);
        self.metrics.bytes.increment(payload_bytes);

        let now_ms = unix_time_ms();
        self.last_packet_ms.store(now_ms, Ordering::Release);
        self.metrics
            .last_packet_timestamp
            .set(now_ms as f64 / 1000.0);
    }

    pub fn total_packets(&self) -> u64 {
        self.packets.load(Ordering::Relaxed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn mark_connected(&self) {
        self.connected_since_ms
            .store(unix_time_ms(), Ordering::Release);
        self.metrics.connected.set(1.0);
    }

    pub fn mark_disconnected(&self) {
        self.connected_since_ms.store(0, Ordering::Release);
        self.metrics.connected.set(0.0);
    }

    pub fn record_reconnect(&self) {
        self.metrics.reconnects.increment(1);
    }

    pub fn record_error(&self) {
        self.metrics.errors.increment(1);
    }

    pub fn connected_secs(&self) -> Option<u64> {
        let since = self.connected_since_ms.load(Ordering::Acquire);
        if since == 0 || self.state() != ConnectionState::Connected {
            return None;
        }
        Some((unix_time_ms().saturating_sub(since)) / 1000)
    }

    pub fn last_packet_secs_ago(&self) -> Option<u64> {
        let last_ms = self.last_packet_ms.load(Ordering::Acquire);
        if last_ms == 0 {
            return None;
        }
        Some((unix_time_ms().saturating_sub(last_ms)) / 1000)
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config_path: Arc<RwLock<String>>,
    pub sources_path: Arc<RwLock<PathBuf>>,
    pub static_path: Arc<RwLock<PathBuf>>,
    pub static_html: Arc<RwLock<Arc<str>>>,
    pub listen: Arc<RwLock<String>>,
    pub channel_capacity: Arc<RwLock<usize>>,
    pub reconnect: Arc<RwLock<ReconnectPolicy>>,
    pub dedup: Arc<RwLock<DedupPolicy>>,
    pub multiplex_tx: broadcast::Sender<Arc<MultiplexFrame>>,
    pub telemetry_tx: broadcast::Sender<Arc<str>>,
    pub latest_telemetry: Arc<RwLock<Option<Arc<str>>>>,
    pub sources: Arc<RwLock<HashMap<String, Arc<SourceRuntime>>>>,
    pub supervisor: Arc<Mutex<SourceSupervisor>>,
    pub admin_token: Arc<RwLock<String>>,
    pub user_agent: Arc<RwLock<String>>,
    pub ignore_ssl_certificate_errors: Arc<RwLock<bool>>,
    pub throughput: Arc<ThroughputMetrics>,
    pub metrics: TelemetryMetrics,
    pub verbose: bool,
}
