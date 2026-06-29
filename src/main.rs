use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use clap::Parser;
use tokio::{net::TcpListener, sync::broadcast};
use tracing::info;

use tangleveil::{
    cli::Cli,
    config::{load_config, validate_config},
    handlers,
    metrics::{self, ThroughputMetrics},
    reload,
    sources::SourceSupervisor,
    state::AppState,
    status,
    telemetry,
};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let cli = Cli::parse();
    let config_path = cli.config.to_str().with_context(|| {
        format!(
            "configuration path {} is not valid UTF-8",
            cli.config.display()
        )
    })?;
    let loaded = load_config(config_path).await?;
    let verbose = cli.verbose || loaded.config.verbose;

    let default_filter = if verbose {
        "tangleveil=debug,tower_http=info"
    } else {
        "tangleveil=info,tower_http=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .init();

    validate_config(&loaded.config)?;

    let static_html = tangleveil::config::load_static_html(&loaded.static_path).await?;
    let static_html = tangleveil::config::render_static_html(&static_html, &loaded.config.hostname);

    let telemetry_metrics = metrics::install();
    metrics::spawn_upkeep(telemetry_metrics.prometheus.clone());

    let (multiplex_tx, _) = broadcast::channel(loaded.config.channel_capacity);
    let (telemetry_tx, _) = telemetry::channel();

    let state = AppState {
        config_path: Arc::new(RwLock::new(config_path.to_owned())),
        sources_path: Arc::new(RwLock::new(loaded.sources_path.clone())),
        static_path: Arc::new(RwLock::new(loaded.static_path.clone())),
        static_html: Arc::new(RwLock::new(Arc::from(static_html))),
        listen: Arc::new(RwLock::new(loaded.config.listen.clone())),
        channel_capacity: Arc::new(RwLock::new(loaded.config.channel_capacity)),
        reconnect: Arc::new(RwLock::new(loaded.config.reconnect.clone())),
        multiplex_tx,
        telemetry_tx,
        latest_telemetry: Arc::new(RwLock::new(None)),
        sources: Arc::new(RwLock::new(HashMap::new())),
        supervisor: Arc::new(tokio::sync::Mutex::new(SourceSupervisor::new())),
        admin_token: Arc::new(RwLock::new(loaded.config.admin_token.clone())),
        user_agent: Arc::new(RwLock::new(loaded.config.user_agent.clone())),
        throughput: Arc::new(ThroughputMetrics::default()),
        metrics: telemetry_metrics,
        verbose,
    };

    {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.apply(&state, &loaded.config)?;
    }

    status::spawn_status_logger(state.clone());
    telemetry::spawn_broadcaster(state.clone());
    reload::spawn_signal_listener(state.clone());

    let app = handlers::router(state);

    let address: SocketAddr = loaded
        .config
        .listen
        .parse()
        .with_context(|| format!("invalid listen address: {}", loaded.config.listen))?;
    let listener = TcpListener::bind(address).await?;

    info!(%address, "Tangleveil listening");
    axum::serve(listener, app).await?;
    Ok(())
}
