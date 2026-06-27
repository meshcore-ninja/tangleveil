use std::{
    sync::atomic::Ordering,
    time::Duration,
};

use tokio::time;
use tracing::info;

use crate::state::AppState;

const STATUS_INTERVAL_SECS: f64 = 10.0;

pub fn spawn_status_logger(state: AppState) {
    tokio::spawn(async move {
        let mut last_packets = 0;
        let mut last_bytes = 0;
        let mut interval = time::interval(Duration::from_secs(STATUS_INTERVAL_SECS as u64));
        interval.tick().await;

        loop {
            interval.tick().await;

            let (packets, bytes) = state
                .throughput
                .interval_delta(&mut last_packets, &mut last_bytes);
            let packets_per_s = packets as f64 / STATUS_INTERVAL_SECS;
            let bytes_per_s = bytes as f64 / STATUS_INTERVAL_SECS;
            let clients = connected_clients(&state);
            let connected_analyzers = connected_analyzers(&state);
            let total_analyzers = state.sources.len();

            info!(
                pps = packets_per_s,
                bps = bytes_per_s,
                clients,
                analyzers = format!("{connected_analyzers}/{total_analyzers}"),
                "status"
            );
        }
    });
}

fn connected_clients(state: &AppState) -> usize {
    let multiplex_clients = state.multiplex_tx.receiver_count();
    let source_clients = state
        .sources
        .values()
        .map(|runtime| runtime.raw_tx.receiver_count())
        .sum::<usize>();

    multiplex_clients + source_clients
}

fn connected_analyzers(state: &AppState) -> usize {
    state
        .sources
        .values()
        .filter(|runtime| runtime.connected.load(Ordering::Relaxed))
        .count()
}
