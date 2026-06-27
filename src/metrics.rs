use std::sync::atomic::{AtomicU64, Ordering};

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
