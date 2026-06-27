use std::{
    collections::{BTreeMap, HashSet},
    path::Path as FsPath,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 4096;

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default = "default_channel_capacity")]
    channel_capacity: usize,
    #[serde(default = "default_sources_file")]
    sources_file: String,
}

#[derive(Debug, Deserialize)]
struct SourcesFile {
    sources: Vec<SourceConfig>,
}

#[derive(Debug)]
pub struct Config {
    pub listen: String,
    pub channel_capacity: usize,
    pub sources: Vec<SourceConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SourceConfig {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

pub async fn load_config(path: &str) -> Result<Config> {
    let config_path = FsPath::new(path);
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
        .unwrap_or_else(|| FsPath::new("."));
    let sources_path = base_dir.join(&file.sources_file);
    let sources = load_sources_file(&sources_path).await?;

    Ok(Config {
        listen: file.listen,
        channel_capacity: file.channel_capacity,
        sources,
    })
}

async fn load_sources_file(path: &FsPath) -> Result<Vec<SourceConfig>> {
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
    }

    Ok(())
}

fn default_listen() -> String {
    "127.0.0.1:8080".to_owned()
}

fn default_sources_file() -> String {
    "sources.toml".to_owned()
}

const fn default_channel_capacity() -> usize {
    DEFAULT_CHANNEL_CAPACITY
}
