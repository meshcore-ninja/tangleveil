use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    config::{Config, SourceConfig},
    state::{AppState, SourceRuntime},
    upstream,
};

struct ManagedTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

pub struct SourceSupervisor {
    tasks: HashMap<String, ManagedTask>,
}

impl SourceSupervisor {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub fn start(state: &AppState, config: &Config) -> Result<Self> {
        let mut supervisor = Self::new();
        supervisor.apply(state, config)?;
        Ok(supervisor)
    }

    pub fn apply(&mut self, state: &AppState, config: &Config) -> Result<()> {
        crate::config::validate_config(config)?;

        {
            let mut reconnect = state
                .reconnect
                .write()
                .expect("reconnect policy lock poisoned");
            *reconnect = config.reconnect.clone();
        }

        {
            let mut user_agent = state.user_agent.write().expect("user agent lock poisoned");
            *user_agent = config.user_agent.clone();
        }

        let ignore_ssl_certificate_errors_changed = {
            let mut ignore_ssl_certificate_errors = state
                .ignore_ssl_certificate_errors
                .write()
                .expect("ignore SSL certificate errors lock poisoned");
            let changed = *ignore_ssl_certificate_errors != config.ignore_ssl_certificate_errors;
            *ignore_ssl_certificate_errors = config.ignore_ssl_certificate_errors;
            changed
        };

        let current_capacity = *state
            .channel_capacity
            .read()
            .expect("channel capacity lock poisoned");
        if config.channel_capacity != current_capacity {
            *state
                .channel_capacity
                .write()
                .expect("channel capacity lock poisoned") = config.channel_capacity;
            info!(
                channel_capacity = config.channel_capacity,
                "channel_capacity updated for newly added sources"
            );
        }

        let desired: HashMap<_, _> = config
            .sources
            .iter()
            .map(|source| (source.id.clone(), source.clone()))
            .collect();

        for id in self
            .tasks
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|id| !desired.contains_key(id))
        {
            self.remove_source(state, &id);
            info!(source = %id, "removed analyzer after config reload");
        }

        {
            let mut sources = state.sources.write().expect("sources lock poisoned");
            sources.retain(|id, _| desired.contains_key(id));
        }

        for (id, source) in desired {
            if let Some(runtime) = state
                .sources
                .read()
                .expect("sources lock poisoned")
                .get(&id)
                .cloned()
            {
                let previous = runtime.config_snapshot();
                runtime.update_config(source.clone());

                if source.disabled {
                    self.stop_task(&id);
                    runtime.set_disabled();
                    continue;
                }

                if !previous.disabled
                    && (source_needs_reconnect(&previous, &source)
                        || ignore_ssl_certificate_errors_changed)
                {
                    self.restart_task(state, &id, runtime);
                    info!(source = %id, "restarted analyzer after config reload");
                } else if previous.disabled || !self.tasks.contains_key(&id) {
                    self.spawn_task(state, &id, runtime);
                    info!(source = %id, "started analyzer after config reload");
                }
            } else {
                self.add_source(state, &source)?;
                info!(source = %id, "added analyzer after config reload");
            }
        }

        Ok(())
    }

    fn add_source(&mut self, state: &AppState, source: &SourceConfig) -> Result<()> {
        let channel_capacity = *state
            .channel_capacity
            .read()
            .expect("channel capacity lock poisoned");
        let runtime = Arc::new(SourceRuntime::new(source.clone(), channel_capacity));

        {
            let mut sources = state.sources.write().expect("sources lock poisoned");
            sources.insert(source.id.clone(), Arc::clone(&runtime));
        }

        if source.disabled {
            runtime.set_disabled();
            return Ok(());
        }

        self.spawn_task(state, &source.id, runtime);
        Ok(())
    }

    fn remove_source(&mut self, state: &AppState, id: &str) {
        self.stop_task(id);
        let mut sources = state.sources.write().expect("sources lock poisoned");
        sources.remove(id);
    }

    fn stop_task(&mut self, id: &str) {
        if let Some(task) = self.tasks.remove(id) {
            task.cancel.cancel();
            task.handle.abort();
        }
    }

    fn restart_task(&mut self, state: &AppState, id: &str, runtime: Arc<SourceRuntime>) {
        self.stop_task(id);
        self.spawn_task(state, id, runtime);
    }

    fn spawn_task(&mut self, state: &AppState, id: &str, runtime: Arc<SourceRuntime>) {
        self.stop_task(id);
        let cancel = CancellationToken::new();
        let handle = spawn_upstream_task(state, runtime, cancel.clone());
        self.tasks
            .insert(id.to_owned(), ManagedTask { cancel, handle });
    }
}

fn spawn_upstream_task(
    state: &AppState,
    runtime: Arc<SourceRuntime>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let multiplex_tx = state.multiplex_tx.clone();
    let throughput = Arc::clone(&state.throughput);
    let reconnect = Arc::clone(&state.reconnect);
    let user_agent = Arc::clone(&state.user_agent);
    let ignore_ssl_certificate_errors = Arc::clone(&state.ignore_ssl_certificate_errors);

    tokio::spawn(async move {
        upstream::run_source_forever(
            runtime,
            multiplex_tx,
            throughput,
            reconnect,
            user_agent,
            ignore_ssl_certificate_errors,
            cancel,
        )
        .await;
    })
}

fn source_needs_reconnect(previous: &SourceConfig, next: &SourceConfig) -> bool {
    previous.url != next.url || previous.headers != next.headers || previous.proxy != next.proxy
}

pub async fn reload_from_disk(state: &AppState, supervisor: &mut SourceSupervisor) -> Result<()> {
    let config_path = state
        .config_path
        .read()
        .expect("config path lock poisoned")
        .clone();
    let loaded = crate::config::load_config(&config_path).await?;

    {
        let mut sources_path = state
            .sources_path
            .write()
            .expect("sources path lock poisoned");
        *sources_path = loaded.sources_path.clone();
    }

    {
        let mut static_path = state
            .static_path
            .write()
            .expect("static path lock poisoned");
        *static_path = loaded.static_path.clone();
    }

    match crate::config::load_static_html(&loaded.static_path).await {
        Ok(html) => {
            let html = crate::config::render_static_html(&html, &loaded.config.hostname);
            *state
                .static_html
                .write()
                .expect("static html lock poisoned") = Arc::from(html);
        }
        Err(error) => {
            warn!(%error, "could not reload static html; keeping previous version");
        }
    }

    if loaded.config.listen != *state.listen.read().expect("listen lock poisoned") {
        warn!(
            new_listen = %loaded.config.listen,
            "listen address changes require a process restart; ignoring"
        );
    }

    {
        let mut admin_token = state
            .admin_token
            .write()
            .expect("admin token lock poisoned");
        *admin_token = loaded.config.admin_token.clone();
    }

    {
        let mut dedup = state.dedup.write().expect("dedup policy lock poisoned");
        *dedup = loaded.config.dedup.clone();
    }

    supervisor.apply(state, &loaded.config)?;
    info!("configuration reloaded");
    Ok(())
}
