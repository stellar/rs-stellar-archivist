pub mod mirror;
pub mod scan;

use crate::storage::StorageConfig;
use crate::{self as stellar_archivist, mirror_operation, scan_operation};
use clap::{Parser, Subcommand};
use std::ffi::OsString;
use std::io;
use std::time::Duration;
use thiserror::Error;
use tracing_subscriber::{fmt, EnvFilter};

/// CLI errors
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Clap(#[from] clap::Error),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    ScanOperation(#[from] scan_operation::Error),

    #[error(transparent)]
    MirrorOperation(#[from] mirror_operation::Error),

    #[error("{0}")]
    Other(String),
}

impl From<stellar_archivist::Error> for Error {
    fn from(err: stellar_archivist::Error) -> Self {
        match err {
            stellar_archivist::Error::Io(e) => Error::Io(e),
            stellar_archivist::Error::ScanOperation(e) => Error::ScanOperation(e),
            stellar_archivist::Error::MirrorOperation(e) => Error::MirrorOperation(e),
            stellar_archivist::Error::Other(s) => Error::Other(s),
        }
    }
}

#[derive(Parser)]
#[command(name = "stellar-archivist")]
#[command(about = "Stellar History Archive tools and utilities", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Number of checkpoints to process concurrently
    #[arg(short, long, global = true, default_value_t = 32)]
    concurrency: usize,

    /// Skip optional SCP files
    #[arg(long, global = true)]
    skip_optional: bool,

    /// Enable debug logging
    #[arg(long, global = true)]
    debug: bool,

    /// Enable trace logging
    #[arg(long, global = true)]
    trace: bool,

    /// Maximum number of retry attempts for failed requests
    #[arg(long, global = true, default_value_t = 3)]
    max_retries: usize,

    /// Maximum concurrent I/O operations (per storage backend)
    #[arg(long, global = true, default_value_t = 64)]
    max_concurrent: usize,

    /// Request timeout in seconds
    #[arg(long, global = true, default_value_t = 30)]
    timeout_secs: u64,

    /// Bandwidth limit in bytes per second (0 = unlimited)
    #[arg(long, global = true, default_value_t = 0)]
    bandwidth_limit: u32,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Mirror files from source archive to destination
    Mirror(mirror::MirrorCmd),
    /// Scan archive and verify integrity
    Scan(scan::ScanCmd),
}

/// Global arguments shared by all commands
#[derive(Debug, Clone)]
pub struct GlobalArgs {
    pub concurrency: usize,
    pub skip_optional: bool,
    pub storage_config: StorageConfig,
}

/// Run the CLI with the given arguments
pub async fn run<I, T>(args: I) -> Result<(), Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(args)?;

    // Determine log level from CLI flags
    let level = if cli.trace {
        "trace"
    } else if cli.debug {
        "debug"
    } else {
        "info"
    };

    // Build filter: set default level and suppress noisy HTTP/2 and networking crates
    let filter = EnvFilter::new(format!(
        "{},opendal={},h2=warn,hyper=warn,hyper_util=warn,reqwest=warn,tokio=warn,tower=warn,rustls=warn",
        level, level
    ));

    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| Error::Other(format!("Failed to initialize logging: {}", e)))?;

    let global_args = GlobalArgs {
        concurrency: cli.concurrency,
        skip_optional: cli.skip_optional,
        storage_config: StorageConfig {
            max_retries: cli.max_retries,
            max_concurrent: cli.max_concurrent,
            timeout: Duration::from_secs(cli.timeout_secs),
            io_timeout: Duration::from_secs(cli.timeout_secs * 5), // 5x for large file I/O
            bandwidth_limit: cli.bandwidth_limit,
            ..Default::default()
        },
    };

    match cli.command {
        Commands::Mirror(cmd) => cmd.run(global_args).await,
        Commands::Scan(cmd) => cmd.run(global_args).await,
    }
}
