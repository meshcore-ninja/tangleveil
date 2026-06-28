use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "tangleveil", about = "Global proxy for CoreScope instances")]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "config.toml", value_name = "FILE")]
    pub config: PathBuf,

    /// Enable debug logging and telemetry timing output.
    #[arg(short, long)]
    pub verbose: bool,
}
