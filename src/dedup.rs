use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Per-client, time-windowed dedup of packet content hashes for `?dedupByContent`.
///
/// Dedup is intrinsically per-client: a globally shared "seen" set would starve
/// clients that connect mid-window (they'd never receive a packet first seen
/// before they arrived). Each client therefore keeps its own small set, bounded
/// in memory by a sliding time window rather than by count.
///
/// The window is implemented as two generations that rotate every `window`:
/// `current` collects new hashes, `previous` retains the prior generation. A
/// hash counts as seen if it's in either set, so an entry survives for at least
/// `window` and at most `2 * window` before it can be admitted again. Rotation
/// is O(1) amortized (swap + clear) and prunes the whole stale generation at
/// once, so there is no per-entry expiry bookkeeping.
pub struct DedupCache {
    window: Duration,
    rotated_at: Instant,
    current: HashSet<Box<str>>,
    previous: HashSet<Box<str>>,
}

impl DedupCache {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            rotated_at: Instant::now(),
            current: HashSet::new(),
            previous: HashSet::new(),
        }
    }

    /// Returns `true` if `key` hasn't been seen within the window (and records
    /// it), or `false` if it's a duplicate that should be suppressed.
    pub fn admit(&mut self, key: &str) -> bool {
        let now = Instant::now();
        if now.duration_since(self.rotated_at) >= self.window {
            // Drop the older generation and start a fresh one. A single elapsed
            // window rotates once; a long idle gap is collapsed to one rotation,
            // which is correct because everything older than `window` expires.
            std::mem::swap(&mut self.current, &mut self.previous);
            self.current.clear();
            self.rotated_at = now;
        }

        if self.current.contains(key) || self.previous.contains(key) {
            return false;
        }
        self.current.insert(Box::from(key));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_first_suppresses_duplicate() {
        let mut cache = DedupCache::new(Duration::from_secs(60));
        assert!(cache.admit("abc"));
        assert!(!cache.admit("abc"));
        assert!(cache.admit("def"));
        assert!(!cache.admit("def"));
    }

    #[test]
    fn readmits_after_two_windows() {
        let mut cache = DedupCache::new(Duration::from_millis(20));
        assert!(cache.admit("abc"));
        assert!(!cache.admit("abc"));
        // First rotation: "abc" moves to `previous`, still seen.
        std::thread::sleep(Duration::from_millis(25));
        assert!(!cache.admit("abc"));
        // Second rotation: the generation holding "abc" is dropped.
        std::thread::sleep(Duration::from_millis(25));
        assert!(cache.admit("abc"));
    }
}
