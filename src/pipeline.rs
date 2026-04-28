//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! Retry, concurrency limiting, timeout handling, and bandwidth throttling are delegated
//! to the storage layer (`OpenDAL`).

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::{cleanup_partial_file, download_buffer, StorageConfig, StorageRef},
    utils::{self, ArchiveStats},
};
use futures_util::{
    future::{join, join3, join_all},
    stream, StreamExt,
};
use lru::LruCache;
use opendal::Buffer;
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// Pipeline errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("History format error: {0}")]
    HistoryFormatError(#[from] crate::history_format::Error),

    #[error("Mirror operation error: {0}")]
    MirrorOperation(#[from] crate::mirror_operation::Error),

    #[error("Scan operation error: {0}")]
    ScanOperation(#[from] crate::scan_operation::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

/// Maximum number of bucket entries to cache in the LRU for deduplication
const BUCKET_LRU_CACHE_SIZE: usize = 1_000_000;

/// How often to report progress (every N checkpoints)
const PROGRESS_REPORTING_FREQUENCY: usize = 100;

// ============================================================================
// Operation trait
// ============================================================================

/// Operation trait for scan, mirror, and repair.
///
/// Pipeline owns `src_store`, `dst_store` (optional), `stats`, and handles
/// buffer writing and stats recording. Operations implement domain-specific
/// logic: checkpoint bounds, file processing, verification, and finalization.
#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    /// Get the checkpoint bounds for this operation.
    /// Returns (`lower_bound`, `upper_bound`) checkpoints to process.
    async fn get_checkpoint_bounds(&self, source: &StorageRef) -> Result<(u32, u32), Error>;

    /// Process a file from the source archive.
    /// Each operation decides how to handle it (existence check, verify, copy, etc.).
    async fn process_object(
        &self,
        path: &str,
        _src_store: &StorageRef,
    ) -> Result<(), crate::storage::Error>;

    /// Called when all work is complete. Pipeline passes stats for operations
    /// that need to check failure counts or record additional failures
    /// (e.g., cross-validation).
    async fn finalize(&self, highest_checkpoint: u32, stats: &ArchiveStats) -> Result<(), Error>;

    /// Pre-check before hitting the source. Allows operations to skip files
    /// without making a source request (e.g., if destination already exists).
    ///
    /// Returns:
    /// - `Some(Ok(()))` to skip (file already handled)
    /// - `Some(Err(e))` if pre-check failed
    /// - `None` to proceed with fetching from source
    async fn pre_check(&self, _path: &str) -> Option<Result<(), crate::storage::Error>> {
        None
    }

    /// Called when all files for a checkpoint have been processed.
    fn finalize_checkpoint(&self, _checkpoint: u32) {}

    /// Fetch the history buffer for a checkpoint.
    ///
    /// Default implementation downloads from `src_store` (scan/mirror behavior).
    /// Repair overrides this to read from destination first, falling back to source.
    async fn fetch_history_buffer(
        &self,
        history_path: &str,
        src_store: &StorageRef,
        _dst_store: Option<&StorageRef>,
    ) -> Result<Buffer, crate::storage::Error> {
        download_buffer(src_store, history_path).await
    }
}

// ============================================================================
// Pipeline config and struct
// ============================================================================

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of concurrent checkpoint workers
    pub concurrency: usize,
    /// Whether to skip optional SCP files
    pub skip_optional: bool,
    /// Storage layer configuration (retry, timeout, bandwidth, etc.)
    pub storage_config: StorageConfig,
}

pub struct Pipeline<Op: Operation> {
    operation: Op,
    config: PipelineConfig,
    src_store: StorageRef,
    dst_store: Option<StorageRef>,
    stats: ArchiveStats,
    bucket_lru: Mutex<LruCache<String, ()>>,
}

impl<Op: Operation> Pipeline<Op> {
    /// Create a new pipeline.
    /// Callers create storage backends and pass them in directly.
    pub fn new(
        operation: Op,
        config: PipelineConfig,
        src_store: StorageRef,
        dst_store: Option<StorageRef>,
    ) -> Self {
        let bucket_lru = Mutex::new(LruCache::new(
            std::num::NonZeroUsize::new(BUCKET_LRU_CACHE_SIZE).unwrap(),
        ));

        Self {
            operation,
            config,
            src_store,
            dst_store,
            stats: ArchiveStats::new(),
            bucket_lru,
        }
    }

    pub async fn run(&self) -> Result<(), Error> {
        let (lower_bound, upper_bound) = self
            .operation
            .get_checkpoint_bounds(&self.src_store)
            .await?;

        let total_count = history_format::count_checkpoints_in_range(lower_bound, upper_bound);
        if total_count == 0 {
            info!("No checkpoints to process");
            return Ok(());
        }

        let num_completed = std::sync::atomic::AtomicUsize::new(0);
        let completed_ref = &num_completed;
        let checkpoints =
            (lower_bound..=upper_bound).step_by(history_format::CHECKPOINT_FREQUENCY as usize);

        stream::iter(checkpoints)
            .for_each_concurrent(self.config.concurrency, |ck| async move {
                self.process_checkpoint(ck).await;
                let done = completed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if done.is_multiple_of(PROGRESS_REPORTING_FREQUENCY) || done == total_count {
                    info!("Progress: {}/{} checkpoints processed", done, total_count);
                }
            })
            .await;

        self.operation.finalize(upper_bound, &self.stats).await?;

        Ok(())
    }

    /// Process a single checkpoint: history+buckets concurrently with the
    /// per-checkpoint files (ledger, transactions, results, optional scp).
    ///
    /// Public so external callers (e.g. repair's retry phase, which builds a
    /// `Pipeline<MirrorOperation>` to re-mirror failed checkpoints) can drive
    /// the pipeline machinery directly without `Pipeline::run`'s full bounds +
    /// iteration loop.
    pub async fn process_checkpoint(&self, checkpoint: u32) {
        let hist = self.process_history_and_buckets(checkpoint);
        let cats = join_all(["ledger", "transactions", "results"].map(|cat| {
            let path = checkpoint_path(cat, checkpoint);
            self.process_file(path)
        }));

        if self.config.skip_optional {
            let _ = join(hist, cats).await;
        } else {
            let path = checkpoint_path("scp", checkpoint);
            let scp = self.process_file(path);
            let _ = join3(hist, cats, scp).await;
        }

        self.operation.finalize_checkpoint(checkpoint);
    }

    /// Process a history file for a checkpoint: download, parse, write buffer
    /// to destination (if present), and process buckets.
    async fn process_history_and_buckets(&self, checkpoint: u32) {
        let history_path = checkpoint_path("history", checkpoint);

        // Download history buffer (operation can override sourcing)
        let Ok(buffer) = utils::with_retries(
            self.config.storage_config.max_retries as u32,
            self.config.storage_config.retry_min_delay.as_millis() as u64,
            "download history",
            &history_path,
            || {
                self.operation.fetch_history_buffer(
                    &history_path,
                    &self.src_store,
                    self.dst_store.as_ref(),
                )
            },
        )
        .await
        else {
            warn!(
                "Bucket references for checkpoint {} (0x{:08x}) were not checked \
                 because {} could not be downloaded",
                checkpoint, checkpoint, history_path
            );
            self.stats.record_failure(&history_path).await;
            return;
        };

        // Parse and validate
        match history_format::parse_history(&buffer, &history_path) {
            Ok(state) => {
                // Write buffer to destination (if present) and process buckets concurrently
                let write_history_buffer = self.process_buffer(&history_path, buffer);
                let buckets = self.process_buckets(state);
                let _ = join(write_history_buffer, buckets).await;
            }
            Err(e) => {
                error!(
                    "Failed to parse history JSON for checkpoint {} (0x{:08x}): {}",
                    checkpoint, checkpoint, e
                );
                warn!(
                    "Bucket references for checkpoint {} (0x{:08x}) were not checked \
                     because {} could not be parsed",
                    checkpoint, checkpoint, history_path
                );
                self.stats.record_failure(&history_path).await;
            }
        }
    }

    /// Write the history buffer to the destination store (if present).
    /// For scan (no dst_store), just records success.
    async fn process_buffer(&self, path: &str, buffer: Buffer) {
        if let Some(ref dst) = self.dst_store {
            match dst.write(path, buffer).await {
                Ok(()) => self.stats.record_success(path),
                Err(e) => {
                    if let Err(cleanup_err) = cleanup_partial_file(dst, path).await {
                        error!("Failed to cleanup partial file {}: {}", path, cleanup_err);
                    }
                    error!("Failed to write history file {}: {}", path, e);
                    self.stats.record_failure(path).await;
                }
            }
        } else {
            // Scan mode: no destination, just record success
            self.stats.record_success(path);
        }
    }

    async fn process_buckets(&self, state: HistoryFileState) {
        let bucket_futures: Vec<_> = {
            let mut cache = self.bucket_lru.lock().unwrap();
            state
                .buckets()
                .iter()
                .filter_map(|bucket| {
                    if cache.put(bucket.clone(), ()).is_none() {
                        bucket_path(bucket).ok().map(|path| self.process_file(path))
                    } else {
                        None
                    }
                })
                .collect()
        };

        join_all(bucket_futures).await;
    }

    /// Process a single file (bucket, ledger, transactions, results, scp).
    async fn process_file(&self, path: String) {
        // Pre-check: allow operation to skip without querying source
        if let Some(result) = self.operation.pre_check(&path).await {
            match result {
                Ok(()) => {
                    debug!("Skipping: {}", path);
                    self.stats.record_skipped(&path);
                }
                Err(e) => {
                    error!("Pre-check failed for {}: {}", path, e);
                    self.stats.record_failure(&path).await;
                }
            }
            return;
        }

        let result = utils::with_retries(
            self.config.storage_config.max_retries as u32,
            self.config.storage_config.retry_min_delay.as_millis() as u64,
            "process",
            &path,
            || self.operation.process_object(&path, &self.src_store),
        )
        .await;

        match result {
            Ok(()) => {
                debug!("Processed: {}", path);
                self.stats.record_success(&path);
            }
            Err(_) => {
                self.stats.record_failure(&path).await;
            }
        }
    }
}

pub use async_trait::async_trait;
