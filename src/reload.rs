use anyhow::Result;
use tracing::warn;

use crate::{
    sources::reload_from_disk,
    state::AppState,
};

pub async fn trigger_reload(state: &AppState) -> Result<()> {
    let mut supervisor = state.supervisor.lock().await;
    reload_from_disk(state, &mut supervisor).await
}

#[cfg(unix)]
pub fn spawn_signal_listener(state: AppState) {
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sighup = match signal(SignalKind::hangup()) {
            Ok(sighup) => sighup,
            Err(error) => {
                warn!(%error, "could not install SIGHUP reload handler");
                return;
            }
        };

        loop {
            if sighup.recv().await.is_none() {
                break;
            }

            if let Err(error) = trigger_reload(&state).await {
                warn!(%error, "configuration reload failed");
            }
        }
    });
}

#[cfg(not(unix))]
pub fn spawn_signal_listener(_state: AppState) {}
