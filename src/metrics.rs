use std::{sync::atomic::{AtomicU64, Ordering}, time::Duration};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_process::Collector;
use tokio::time;

#[derive(Default)]
pub struct ThroughputMetrics {
    packets: AtomicU64,
    bytes: AtomicU64,
}

impl ThroughputMetrics {
    pub fn record_packet(&self, bytes: u64) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn interval_delta(&self, last_packets: &mut u64, last_bytes: &mut u64) -> (u64, u64) {
        let packets = self.packets.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let delta_packets = packets - *last_packets;
        let delta_bytes = bytes - *last_bytes;
        *last_packets = packets;
        *last_bytes = bytes;
        (delta_packets, delta_bytes)
    }
}

/// Prometheus exporter state, separate from the one-second WebSocket telemetry feed.
/// Carries cumulative counters/gauges; rates are computed by Prometheus, not here.
#[derive(Clone)]
pub struct TelemetryMetrics {
    pub prometheus: PrometheusHandle,
    pub process: Collector,
}

pub fn install() -> TelemetryMetrics {
    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    let process = Collector::default();
    process.describe();

    TelemetryMetrics { prometheus, process }
}

/// `install_recorder` only installs the recorder; it doesn't start the exporter's
/// background maintenance (e.g. decaying idle distributions). Spawn once after `install()`.
pub fn spawn_upkeep(prometheus: PrometheusHandle) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            prometheus.run_upkeep();
        }
    });
}
