use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use bytes::Bytes;
use tokio::sync::broadcast;

use crate::{frame::RawFrame, metrics::ThroughputMetrics};

pub struct SourceRuntime {
    pub raw_tx: broadcast::Sender<RawFrame>,
    pub connected: AtomicBool,
    pub sequence: AtomicU64,
}

#[derive(Clone)]
pub struct AppState {
    pub multiplex_tx: broadcast::Sender<Bytes>,
    pub sources: Arc<HashMap<String, Arc<SourceRuntime>>>,
    pub throughput: Arc<ThroughputMetrics>,
}
