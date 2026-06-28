use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 4096;
const DEFAULT_RECONNECT_INITIAL_DELAY_SECS: u64 = 1;
const DEFAULT_RECONNECT_MAX_DELAY_SECS: u64 = 30;
const DEFAULT_RECONNECT_OFFLINE_THRESHOLD_SECS: u64 = 60;
const DEFAULT_RECONNECT_OFFLINE_DELAY_SECS: u64 = 300;
const DISABLED_ADMIN_TOKEN: &str = "change-me";

pub fn admin_enabled(token: &str) -> bool {
    !token.is_empty() && token != DISABLED_ADMIN_TOKEN
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default = "default_channel_capacity")]
    channel_capacity: usize,
    #[serde(default = "default_sources_file")]
    sources_file: String,
    #[serde(default = "default_static_file")]
    static_file: String,
    #[serde(default)]
    hostname: String,
    #[serde(default = "default_reconnect_initial_delay_secs")]
    reconnect_initial_delay_secs: u64,
    #[serde(default = "default_reconnect_max_delay_secs")]
    reconnect_max_delay_secs: u64,
    #[serde(default = "default_reconnect_offline_threshold_secs")]
    reconnect_offline_threshold_secs: u64,
    #[serde(default = "default_reconnect_offline_delay_secs")]
    reconnect_offline_delay_secs: u64,
    #[serde(default)]
    verbose: bool,
    #[serde(default)]
    admin_token: String,
}

#[derive(Debug, Deserialize)]
struct SourcesFile {
    sources: Vec<SourceConfig>,
}

#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub offline_threshold: Duration,
    pub offline_delay: Duration,
}

#[derive(Debug)]
pub struct Config {
    pub listen: String,
    pub channel_capacity: usize,
    pub sources: Vec<SourceConfig>,
    pub reconnect: ReconnectPolicy,
    pub verbose: bool,
    pub admin_token: String,
    pub hostname: String,
}

#[derive(Debug)]
pub struct LoadedConfig {
    pub config: Config,
    pub sources_path: PathBuf,
    pub static_path: PathBuf,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
pub struct SourceConfig {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub proxy: Option<String>,
}

impl std::fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceConfig")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("headers", &self.headers)
            .field("disabled", &self.disabled)
            .field(
                "proxy",
                &self.proxy.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

pub async fn load_config(path: &str) -> Result<LoadedConfig> {
    let config_path = Path::new(path);
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("could not read configuration file {path}"))?;
    let file: ConfigFile = toml::from_str(std::str::from_utf8(&data).with_context(|| {
        format!("configuration file {path} is not valid UTF-8")
    })?)
    .with_context(|| format!("could not parse configuration file {path}"))?;

    let base_dir = config_path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let sources_path = base_dir.join(&file.sources_file);
    let sources = load_sources_file(&sources_path).await?;
    let static_path = base_dir.join(&file.static_file);

    Ok(LoadedConfig {
        config: Config {
            listen: file.listen,
            channel_capacity: file.channel_capacity,
            sources,
            reconnect: ReconnectPolicy {
                initial_delay: Duration::from_secs(file.reconnect_initial_delay_secs),
                max_delay: Duration::from_secs(file.reconnect_max_delay_secs),
                offline_threshold: Duration::from_secs(file.reconnect_offline_threshold_secs),
                offline_delay: Duration::from_secs(file.reconnect_offline_delay_secs),
            },
            verbose: file.verbose,
            admin_token: file.admin_token,
            hostname: file.hostname,
        },
        sources_path,
        static_path,
    })
}

pub async fn load_static_html(path: &Path) -> Result<String> {
    let data = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("could not read static html file {}", path.display()))?;
    Ok(data)
}

/// Fills in `{{HOSTNAME}}` and `{{VERSION}}` placeholders in the status page.
/// `{{HOSTNAME}}` falls back to the literal word "host" when unconfigured.
/// `{{VERSION}}` is always the binary's own compiled-in crate version.
pub fn render_static_html(html: &str, hostname: &str) -> String {
    let hostname = if hostname.is_empty() { "host" } else { hostname };
    html.replace("{{HOSTNAME}}", hostname)
        .replace("{{VERSION}}", env!("CARGO_PKG_VERSION"))
}

async fn load_sources_file(path: &Path) -> Result<Vec<SourceConfig>> {
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("could not read sources file {}", path.display()))?;
    let file: SourcesFile = toml::from_str(std::str::from_utf8(&data).with_context(|| {
        format!("sources file {} is not valid UTF-8", path.display())
    })?)
    .with_context(|| format!("could not parse sources file {}", path.display()))?;

    Ok(file.sources)
}

pub fn validate_config(config: &Config) -> Result<()> {
    if config.sources.is_empty() {
        bail!("sources file must contain at least one source");
    }
    if config.channel_capacity == 0 {
        bail!("channel_capacity must be greater than zero");
    }

    let reconnect = &config.reconnect;
    if reconnect.initial_delay.is_zero() {
        bail!("reconnect_initial_delay_secs must be greater than zero");
    }
    if reconnect.max_delay < reconnect.initial_delay {
        bail!("reconnect_max_delay_secs must be >= reconnect_initial_delay_secs");
    }
    if reconnect.offline_threshold.is_zero() {
        bail!("reconnect_offline_threshold_secs must be greater than zero");
    }
    if reconnect.offline_delay.is_zero() {
        bail!("reconnect_offline_delay_secs must be greater than zero");
    }

    let mut ids = HashSet::new();
    for source in &config.sources {
        if source.id.is_empty() {
            bail!("source id cannot be empty");
        }
        if source.id.len() > u16::MAX as usize {
            bail!("source id is too long: {}", source.id);
        }
        if !ids.insert(&source.id) {
            bail!("duplicate source id: {}", source.id);
        }
        if let Some(proxy) = &source.proxy {
            if proxy.trim().is_empty() {
                bail!("source {}: proxy cannot be empty", source.id);
            }
            validate_proxy_url(proxy)?;
        }
    }

    Ok(())
}

fn validate_proxy_url(proxy: &str) -> Result<()> {
    let parsed = url::Url::parse(proxy)
        .with_context(|| format!("invalid proxy URL {proxy}"))?;
    if parsed.scheme() != "http" {
        bail!("proxy URL must use http:// scheme");
    }
    if parsed.host_str().is_none() {
        bail!("proxy URL missing hostname");
    }
    Ok(())
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_owned()
}

fn default_sources_file() -> String {
    "sources.toml".to_owned()
}

fn default_static_file() -> String {
    "static/index.html".to_owned()
}

const fn default_channel_capacity() -> usize {
    DEFAULT_CHANNEL_CAPACITY
}

const fn default_reconnect_initial_delay_secs() -> u64 {
    DEFAULT_RECONNECT_INITIAL_DELAY_SECS
}

const fn default_reconnect_max_delay_secs() -> u64 {
    DEFAULT_RECONNECT_MAX_DELAY_SECS
}

const fn default_reconnect_offline_threshold_secs() -> u64 {
    DEFAULT_RECONNECT_OFFLINE_THRESHOLD_SECS
}

const fn default_reconnect_offline_delay_secs() -> u64 {
    DEFAULT_RECONNECT_OFFLINE_DELAY_SECS
}
