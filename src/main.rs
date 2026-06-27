use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
};

use anyhow::{Context, Result};
use tokio::{net::TcpListener, sync::broadcast};
use tracing::info;

use tangleveil::{
    config::{load_config, validate_config},
    handlers,
    metrics::ThroughputMetrics,
    state::{AppState, SourceRuntime},
    status,
    upstream,
};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tangleveil=info,tower_http=info".into()),
        )
        .init();

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_owned());
    let config = load_config(&config_path).await?;
    validate_config(&config)?;

    let (multiplex_tx, _) = broadcast::channel(config.channel_capacity);
    let throughput = Arc::new(ThroughputMetrics::default());
    let mut runtimes = HashMap::with_capacity(config.sources.len());

    for source in &config.sources {
        let (raw_tx, _) = broadcast::channel(config.channel_capacity);
        runtimes.insert(
            source.id.clone(),
            Arc::new(SourceRuntime {
                raw_tx,
                connected: AtomicBool::new(false),
                sequence: AtomicU64::new(0),
            }),
        );
    }

    let state = AppState {
        multiplex_tx,
        sources: Arc::new(runtimes),
        throughput: Arc::clone(&throughput),
    };

    status::spawn_status_logger(state.clone());

    for source in config.sources.clone() {
        let runtime = Arc::clone(
            state
                .sources
                .get(&source.id)
                .expect("source runtime created during startup"),
        );
        let multiplex_tx = state.multiplex_tx.clone();
        let throughput = Arc::clone(&state.throughput);

        tokio::spawn(async move {
            upstream::run_source_forever(source, runtime, multiplex_tx, throughput).await;
        });
    }

    let app = handlers::router(state);

    let address: SocketAddr = config
        .listen
        .parse()
        .with_context(|| format!("invalid listen address: {}", config.listen))?;
    let listener = TcpListener::bind(address).await?;

    info!(%address, "Tangleveil listening");
    axum::serve(listener, app).await?;
    Ok(())
}
