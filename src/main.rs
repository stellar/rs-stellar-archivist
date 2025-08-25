use stellar_archivist::{
    mirror_operation::{self, MirrorOperation},
    pipeline::{self, Pipeline, PipelineConfig},
    scan_operation::{self, ScanOperation},
};

use clap::{Parser, Subcommand};
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// CLI errors
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ScanOperation(#[from] scan_operation::Error),

    #[error(transparent)]
    MirrorOperation(#[from] mirror_operation::Error),

    #[error("{0}")]
    Other(String),
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

#[derive(Subcommand)]
enum Commands {
    /// Mirror files from source archive to destination
    Mirror {
        /// Source archive URL (http://, https://, file://)
        src: String,
        /// Destination path (must be file://)
        dst: String,

        /// Mirror starting from this ledger (will round down to nearest checkpoint)
        #[arg(long)]
        low: Option<u32>,

        /// Mirror up to this ledger only (will round up to nearest checkpoint)
        #[arg(long)]
        high: Option<u32>,

        /// Overwrite existing files within the mirrored range (if not set, mirror will skip over existing files)
        #[arg(long)]
        overwrite: bool,

        /// Allow mirroring even when it would create gaps in the destination archive
        #[arg(long, default_value_t = false)]
        allow_mirror_gaps: bool,
    },
    /// Scan archive and verify integrity
    Scan {
        /// Archive URL to scan (http://, https://, file://)
        archive: String,

        /// Scan starting from this ledger (will round to nearest checkpoint)
        #[arg(long)]
        low: Option<u32>,

        /// Scan up to this checkpoint only
        #[arg(long)]
        high: Option<u32>,
    },
}

#[tokio::main]
async fn main() {
    let result = run().await;
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Error> {
    let cli = Cli::parse();

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

    match cli.command {
        Commands::Mirror {
            src,
            dst,
            low,
            high,
            overwrite,
            allow_mirror_gaps,
        } => {
            run_mirror(
                src,
                dst,
                cli.concurrency,
                cli.skip_optional,
                low,
                high,
                overwrite,
                allow_mirror_gaps,
                cli.max_retries,
                cli.initial_backoff_ms,
            )
            .await?;
        }
        Commands::Scan { archive, low, high } => {
            run_scan(
                archive,
                cli.concurrency,
                cli.skip_optional,
                low,
                high,
                cli.max_retries,
                cli.initial_backoff_ms,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_scan(
    archive: String,
    concurrency: usize,
    skip_optional: bool,
    low: Option<u32>,
    high: Option<u32>,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<(), Error> {
    info!("Starting scan of {}", archive);

    if let Some(low) = low {
        info!("Scanning from ledger {} onwards", low);
    }
    if let Some(high) = high {
        info!("Scanning up to checkpoint {}", high);
    }

    // Create the scan operation
    let operation = ScanOperation::new(low, high).await?;

    // Configure the pipeline with low/high bounds and retry config
    let pipeline_config = PipelineConfig {
        source: archive.clone(),
        concurrency,
        skip_optional,
        max_retries,
        initial_backoff_ms,
    };

    // Create and run the pipeline
    let pipeline = Arc::new(
        Pipeline::new(operation, pipeline_config)
            .await
            .map_err(|e| match e {
                pipeline::Error::ScanOperation(scan_err) => Error::ScanOperation(scan_err),
                pipeline::Error::Io(io_err) => Error::Io(io_err),
                other => Error::Other(format!("Internal error: {}", other)),
            })?,
    );
    pipeline
        .run()
        .await
        .map_err(|e| match e {
            pipeline::Error::ScanOperation(scan_err) => Error::ScanOperation(scan_err),
            pipeline::Error::Io(io_err) => Error::Io(io_err),
            other => Error::Other(format!("Internal error: {}", other)),
        })?;

    Ok(())
}

async fn run_mirror(
    src: String,
    dst: String,
    concurrency: usize,
    skip_optional: bool,
    low: Option<u32>,
    high: Option<u32>,
    overwrite: bool,
    allow_mirror_gaps: bool,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<(), Error> {
    info!(
        "Starting mirror from {} to {} with {} workers",
        src, dst, concurrency
    );

    let operation = MirrorOperation::new(&dst, overwrite, low, high, allow_mirror_gaps).await?;
    let pipeline_config = PipelineConfig {
        source: src.clone(),
        concurrency,
        skip_optional,
        max_retries,
        initial_backoff_ms,
    };

    let pipeline = Arc::new(
        Pipeline::new(operation, pipeline_config)
            .await
            .map_err(|e| match e {
                pipeline::Error::MirrorOperation(mirror_err) => Error::MirrorOperation(mirror_err),
                pipeline::Error::Io(io_err) => Error::Io(io_err),
                other => Error::Other(format!("Internal error: {}", other)),
            })?,
    );
    pipeline
        .run()
        .await
        .map_err(|e| match e {
            pipeline::Error::MirrorOperation(mirror_err) => Error::MirrorOperation(mirror_err),
            pipeline::Error::Io(io_err) => Error::Io(io_err),
            other => Error::Other(format!("Internal error: {}", other)),
        })?;

    Ok(())
}
