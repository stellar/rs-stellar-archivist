pub mod mirror;
pub mod scan;

use crate::{self as stellar_archivist, mirror_operation, scan_operation};
use clap::{Parser, Subcommand};
use std::ffi::OsString;
use std::io;
use thiserror::Error;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

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

    /// Number of files to process concurrently
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

    /// Maximum number of HTTP retry attempts
    #[arg(long, global = true, default_value_t = 3)]
    max_retries: u32,

    /// Initial backoff in milliseconds for HTTP retries
    #[arg(long, global = true, default_value_t = 100)]
    initial_backoff_ms: u64,
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
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
}

/// Run the CLI with the given arguments
pub async fn run<I, T>(args: I) -> Result<(), Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(args)?;

    let log_level = if cli.trace {
        Level::TRACE
    } else if cli.debug {
        Level::DEBUG
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| Error::Other(format!("Failed to initialize logging: {}", e)))?;

    let global_args = GlobalArgs {
        concurrency: cli.concurrency,
        skip_optional: cli.skip_optional,
        max_retries: cli.max_retries,
        initial_backoff_ms: cli.initial_backoff_ms,
    };

    match cli.command {
        Commands::Mirror(cmd) => cmd.run(global_args).await,
        Commands::Scan(cmd) => cmd.run(global_args).await,
    }
}
