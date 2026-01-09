use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use crate::history_format::{self, HistoryFileState};
use crate::pipeline;
use crate::storage::StorageRef;

/// Utils module errors - just wraps errors from other modules
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("History format error: {0}")]
    HistoryFormat(#[from] crate::history_format::Error),

    #[error("No available checkpoints: archive's latest checkpoint {latest_checkpoint} (0x{latest_checkpoint:08x}) (ledger {latest_ledger}) is below requested low {low_checkpoint} (0x{low_checkpoint:08x}) (ledger {low_ledger})")]
    NoAvailableCheckpoints {
        latest_checkpoint: u32,
        latest_ledger: u32,
        low_checkpoint: u32,
        low_ledger: u32,
    },

    #[error("Invalid checkpoint range: low checkpoint {low_checkpoint} (0x{low_checkpoint:08x}) is greater than high checkpoint {high_checkpoint} (0x{high_checkpoint:08x})")]
    InvalidCheckpointRange {
        low_checkpoint: u32,
        high_checkpoint: u32,
    },
}

/// Helper function to map pipeline errors to library errors
pub fn map_pipeline_error(err: pipeline::Error) -> crate::Error {
    match err {
        pipeline::Error::ScanOperation(scan_err) => crate::Error::ScanOperation(scan_err),
        pipeline::Error::MirrorOperation(mirror_err) => crate::Error::MirrorOperation(mirror_err),
        pipeline::Error::Io(io_err) => crate::Error::Io(io_err),
        other => crate::Error::Other(other.to_string()),
    }
}

/// Shared statistics tracking for archive operations
/// for consistent reporting across scan and mirror operations
pub struct ArchiveStats {
    // Successfully processed files
    pub successful_files: AtomicU64,

    // Failed files (any type of failure)
    pub failed_files: AtomicU64,

    // Skipped files (already exist in mirror mode)
    pub skipped_files: AtomicU64,

    // Number of retry attempts (not unique files, but total retries)
    pub retry_count: AtomicU64,

    pub missing_required: AtomicU64,
    pub missing_history: AtomicU64,
    pub missing_ledger: AtomicU64,
    pub missing_transactions: AtomicU64,
    pub missing_results: AtomicU64,
    pub missing_buckets: AtomicU64,
    pub missing_scp: AtomicU64,

    // List of all failed/missing files for detailed reporting
    pub failed_list: Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl ArchiveStats {
    pub fn new() -> Self {
        Self {
            successful_files: AtomicU64::new(0),
            failed_files: AtomicU64::new(0),
            skipped_files: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            missing_required: AtomicU64::new(0),
            missing_history: AtomicU64::new(0),
            missing_ledger: AtomicU64::new(0),
            missing_transactions: AtomicU64::new(0),
            missing_results: AtomicU64::new(0),
            missing_buckets: AtomicU64::new(0),
            missing_scp: AtomicU64::new(0),
            failed_list: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    pub fn record_success(&self, _path: &str) {
        self.successful_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a skipped file (already exists in mirror mode)
    pub fn record_skipped(&self, _path: &str) {
        self.skipped_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry attempt
    pub fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_failure(&self, path: &str) {
        self.failed_files.fetch_add(1, Ordering::Relaxed);

        if path.contains("history") {
            self.missing_history.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("ledger") {
            self.missing_ledger.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("transactions") {
            self.missing_transactions.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("results") {
            self.missing_results.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("bucket") {
            self.missing_buckets.fetch_add(1, Ordering::Relaxed);
        } else if path.contains("scp") {
            self.missing_scp.fetch_add(1, Ordering::Relaxed);
        }

        if !path.contains("scp") {
            self.missing_required.fetch_add(1, Ordering::Relaxed);
        }

        let mut failed_list = self.failed_list.lock().await;
        failed_list.push(path.to_string());
    }

    /// Generate and log a complete report of the operation results
    pub async fn report(&self, operation: &str) {
        let successful = self.successful_files.load(Ordering::Relaxed);
        let failed = self.failed_files.load(Ordering::Relaxed);
        let skipped = self.skipped_files.load(Ordering::Relaxed);
        let retries = self.retry_count.load(Ordering::Relaxed);

        if operation == "mirror" {
            info!(
                "Mirror completed: {} files copied, {} failed, {} skipped",
                successful, failed, skipped
            );
        } else {
            let missing_required = self.missing_required.load(Ordering::Relaxed);
            info!(
                "Scan complete: {} files found, {} missing ({} required)",
                successful, failed, missing_required
            );
        }

        // Debug-level stats summary
        debug!(
            "Stats: {} successful, {} failed, {} skipped, {} retries",
            successful, failed, skipped, retries
        );

        if failed == 0 {
            return;
        }

        let missing_history = self.missing_history.load(Ordering::Relaxed);
        let missing_ledger = self.missing_ledger.load(Ordering::Relaxed);
        let missing_transactions = self.missing_transactions.load(Ordering::Relaxed);
        let missing_results = self.missing_results.load(Ordering::Relaxed);
        let missing_buckets = self.missing_buckets.load(Ordering::Relaxed);
        let missing_scp = self.missing_scp.load(Ordering::Relaxed);

        if missing_history > 0 {
            error!("Missing {} history files", missing_history);
        }
        if missing_ledger > 0 {
            error!("Missing {} ledger files", missing_ledger);
        }
        if missing_transactions > 0 {
            error!("Missing {} transactions files", missing_transactions);
        }
        if missing_results > 0 {
            error!("Missing {} results files", missing_results);
        }
        if missing_buckets > 0 {
            error!("Missing {} buckets", missing_buckets);
        }
        if missing_scp > 0 {
            warn!("Missing {} optional scp files", missing_scp);
        }
    }

    pub fn has_failures(&self) -> bool {
        self.failed_files.load(Ordering::Relaxed) > 0
    }
}

/// Tracks retry state with exponential backoff
pub struct RetryState {
    pub attempt: u32,
    pub backoff_ms: u64,
    pub max_retries: u32,
}

impl RetryState {
    pub fn new(max_retries: u32, initial_backoff_ms: u64) -> Self {
        Self {
            attempt: 0,
            backoff_ms: initial_backoff_ms,
            max_retries,
        }
    }

    /// Record an attempt and return true if we should retry, false if max retries exceeded
    pub fn record_attempt(&mut self) -> bool {
        self.attempt += 1;
        self.attempt <= self.max_retries
    }

    /// Wait for the backoff period and increase it for next time
    pub async fn backoff(&mut self) {
        tokio::time::sleep(tokio::time::Duration::from_millis(self.backoff_ms)).await;
        self.backoff_ms = (self.backoff_ms * 2).min(5000); // Cap at 5 seconds
    }
}

/// Fetch and validate .well-known/stellar-history.json from store
pub async fn fetch_well_known_history_file(
    store: &StorageRef,
    max_retries: u32,
    initial_backoff_ms: u64,
) -> Result<HistoryFileState, Error> {
    use crate::history_format::ROOT_WELL_KNOWN_PATH;
    use crate::storage::ErrorClass;
    use tokio::io::AsyncReadExt;

    debug!("Fetching .well-known from path: {}", ROOT_WELL_KNOWN_PATH);

    let mut retry = RetryState::new(max_retries, initial_backoff_ms);

    loop {
        // Try to open and read the file
        let result: Result<Vec<u8>, (ErrorClass, String)> = async {
            let mut reader = store.open_reader(ROOT_WELL_KNOWN_PATH).await.map_err(|e| {
                (
                    e.class,
                    format!("Failed to open {}: {}", ROOT_WELL_KNOWN_PATH, e),
                )
            })?;

            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await.map_err(|e| {
                // IO errors during read are typically retryable
                (
                    ErrorClass::Retry,
                    format!("Reading {}: {}", ROOT_WELL_KNOWN_PATH, e),
                )
            })?;

            Ok(buffer)
        }
        .await;

        match result {
            Ok(buffer) => {
                // Parse the JSON
                let state: HistoryFileState = serde_json::from_slice(&buffer).map_err(|e| {
                    crate::history_format::Error::InvalidJson {
                        path: ROOT_WELL_KNOWN_PATH.to_string(),
                        error: e.to_string(),
                    }
                })?;

                // Validate the .well-known format
                state.validate()?;

                return Ok(state);
            }
            Err((error_class, msg)) => {
                if matches!(error_class, ErrorClass::Retry) && retry.record_attempt() {
                    debug!(
                        "Retrying {} (attempt {}/{}): {}, backing off {}ms",
                        ROOT_WELL_KNOWN_PATH,
                        retry.attempt,
                        retry.max_retries,
                        msg,
                        retry.backoff_ms
                    );
                    retry.backoff().await;
                    continue;
                }

                return Err(std::io::Error::new(std::io::ErrorKind::Other, msg).into());
            }
        }
    }
}

/// Compute checkpoint bounds using a pre-fetched source checkpoint
pub fn compute_checkpoint_bounds(
    source_checkpoint: u32,
    low: Option<u32>,
    high: Option<u32>,
) -> Result<(u32, u32), Error> {
    // If low is not provided, default to genesis checkpoint
    let low_checkpoint = if let Some(low) = low {
        let low_checkpoint = history_format::round_to_lower_checkpoint(low);

        // Check if the source's current checkpoint is below requested low
        if source_checkpoint < low_checkpoint {
            return Err(Error::NoAvailableCheckpoints {
                latest_checkpoint: source_checkpoint,
                latest_ledger: source_checkpoint,
                low_checkpoint,
                low_ledger: low,
            });
        }

        low_checkpoint
    } else {
        history_format::GENESIS_CHECKPOINT_LEDGER
    };

    let high_checkpoint = if let Some(high) = high {
        let high_checkpoint = history_format::round_to_upper_checkpoint(high);

        // Warn if the user passed a high ledger that is above what we see in source
        // We don't fail, but use the source's current checkpoint as the high bound
        if source_checkpoint < high_checkpoint {
            warn!(
                "Source checkpoint {} (0x{:08x}) is below requested high {} (0x{:08x}) (ledger {}), will process up to latest available",
                source_checkpoint, source_checkpoint, high_checkpoint, high_checkpoint, high
            );
            source_checkpoint
        } else {
            high_checkpoint
        }
    } else {
        source_checkpoint
    };

    // Make sure we have at least one checkpoint in range
    if low_checkpoint > high_checkpoint {
        return Err(Error::InvalidCheckpointRange {
            low_checkpoint,
            high_checkpoint,
        });
    }

    let total_count = history_format::count_checkpoints_in_range(low_checkpoint, high_checkpoint);
    info!(
        "Processing {} checkpoints from {} (0x{:08x}) to {} (0x{:08x})",
        total_count, low_checkpoint, low_checkpoint, high_checkpoint, high_checkpoint
    );

    Ok((low_checkpoint, high_checkpoint))
}
