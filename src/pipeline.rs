//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! Retry, concurrency limiting, timeout handling, and bandwidth throttling are delegated
//! to the storage layer (`OpenDAL`).

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::{download_buffer, StorageConfig, StorageRef},
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

    #[error("Repair operation error: {0}")]
    RepairOperation(#[from] crate::repair_operation::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

/// Max entries in the bucket-dedup LRU. Buckets are content-addressed and the
/// same hash often appears in many checkpoints' history files; the LRU lets
/// `process_buckets` skip repeat sightings within one pipeline run.
const BUCKET_LRU_CACHE_SIZE: usize = 1_000_000;

/// How often to report progress (every N checkpoints)
const PROGRESS_REPORTING_FREQUENCY: usize = 100;

// ============================================================================
// Operation trait
// ============================================================================

/// Non-error outcome of `Operation::process_object` / `process_buffer`.
///
/// Returned inside `Ok(_)`; the pipeline maps it to the matching
/// `ArchiveStats` recorder: `Processed` → `record_success`,
/// `Skipped` → `record_skipped`. Errors are signaled via `Err(_)` in the
/// surrounding `Result` and recorded by the pipeline as `record_failure`
/// (after retries exhaust).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The operation did its work successfully.
    Processed,
    /// The operation deliberately did no work (e.g. mirror found the file
    /// already present at dst).
    Skipped,
}

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

    /// Process a single file. Returns:
    /// - `Ok(Processed)` — work succeeded.
    /// - `Ok(Skipped)` — operation chose not to do the work (e.g. mirror when
    ///   dst already has the file).
    /// - `Err(_)` — wrapped by `with_retries`; final failure after retries
    ///   exhaust is recorded by the pipeline.
    ///
    /// `checkpoint` is the context cp; for buckets, the discovering cp.
    async fn process_object(
        &self,
        checkpoint: u32,
        path: &str,
        _src_store: &StorageRef,
    ) -> Result<ProcessOutcome, crate::storage::Error>;

    /// Called when all work is complete. Pipeline passes stats for operations
    /// that need to check failure counts or record additional failures
    /// (e.g., cross-validation).
    async fn finalize(&self, highest_checkpoint: u32, stats: &ArchiveStats) -> Result<(), Error>;

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

    /// Write a fetched buffer (currently the history file) to the operation's
    /// destination.
    ///
    /// Implementations diverge by mode: scan returns `Processed` without
    /// writing; mirror writes via `storage::write_buffer_with_cleanup`;
    /// repair skips the write in dry-run mode and otherwise writes like mirror.
    async fn process_buffer(
        &self,
        checkpoint: u32,
        path: &str,
        buffer: Buffer,
    ) -> Result<ProcessOutcome, crate::storage::Error>;
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
    /// Hash → () presence cache used by `process_buckets` to skip buckets
    /// already seen elsewhere in this pipeline run.
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
        // History work: fetch + parse, then concurrently write the buffer to
        // dst and process the buckets it references. Fetch/parse failures are
        // logged and recorded inside `fetch_history_file_state`; on failure
        // this future just returns early so the rest of the checkpoint's files
        // still get processed.
        let history_work = async {
            let Some((state, buffer)) = self.fetch_history_file_state(checkpoint).await else {
                return;
            };
            let history_path = checkpoint_path("history", checkpoint);
            let write_history_buffer = self.process_history_file(checkpoint, &history_path, buffer);
            let buckets = self.process_buckets(checkpoint, state);
            let _ = join(write_history_buffer, buckets).await;
        };

        let cats = join_all(["ledger", "transactions", "results"].map(|cat| {
            let path = checkpoint_path(cat, checkpoint);
            self.process_file(checkpoint, path)
        }));

        if self.config.skip_optional {
            let _ = join(history_work, cats).await;
        } else {
            let scp_path = checkpoint_path("scp", checkpoint);
            let scp = self.process_file(checkpoint, scp_path);
            let _ = join3(history_work, cats, scp).await;
        }

        self.operation.finalize_checkpoint(checkpoint);
    }

    /// Fetch and parse a checkpoint's history file. On download or parse
    /// failure, logs the error and records a failure into `stats`, returning
    /// `None`. On success, returns the parsed state alongside the raw buffer
    /// (the caller needs the buffer to write it to dst).
    async fn fetch_history_file_state(
        &self,
        checkpoint: u32,
    ) -> Option<(HistoryFileState, Buffer)> {
        let history_path = checkpoint_path("history", checkpoint);

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
            self.stats.record_failure(checkpoint, &history_path).await;
            return None;
        };

        match history_format::parse_history(&buffer, &history_path) {
            Ok(state) => Some((state, buffer)),
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
                self.stats.record_failure(checkpoint, &history_path).await;
                None
            }
        }
    }

    /// Process every bucket referenced by `state`, deduped against the
    /// pipeline-wide `bucket_lru`. The lock is held only across the filter-map
    /// → collect (CPU-only — `LruCache::put` returns `None` iff the key is
    /// new); each surviving `process_file` future then runs concurrently
    /// outside the lock.
    async fn process_buckets(&self, checkpoint: u32, state: HistoryFileState) {
        let bucket_futures: Vec<_> = {
            let mut cache = self.bucket_lru.lock().unwrap();
            state
                .buckets()
                .iter()
                .filter_map(|bucket| {
                    if cache.put(bucket.clone(), ()).is_none() {
                        bucket_path(bucket)
                            .ok()
                            .map(|path| self.process_file(checkpoint, path))
                    } else {
                        None
                    }
                })
                .collect()
        };

        join_all(bucket_futures).await;
    }

    /// Dispatch a parsed history buffer to the operation and record the
    /// outcome into stats. Mirrors `process_file`'s match pattern but for
    /// `process_buffer`. Retries write failures on the destination side just
    /// like `process_object` retries fetch failures on the source side;
    /// `Buffer` is `Bytes`-backed so each attempt clones cheaply.
    async fn process_history_file(&self, checkpoint: u32, path: &str, buffer: Buffer) {
        let result = utils::with_retries(
            self.config.storage_config.max_retries as u32,
            self.config.storage_config.retry_min_delay.as_millis() as u64,
            "process",
            path,
            || {
                self.operation
                    .process_buffer(checkpoint, path, buffer.clone())
            },
        )
        .await;

        match result {
            Ok(ProcessOutcome::Processed) => {
                debug!("Processed: {}", path);
                self.stats.record_success(path);
            }
            Ok(ProcessOutcome::Skipped) => {
                debug!("Skipped: {}", path);
                self.stats.record_skipped(path);
            }
            Err(_) => {
                self.stats.record_failure(checkpoint, path).await;
            }
        }
    }

    /// Process a single file (bucket, ledger, transactions, results, scp).
    /// `checkpoint` is the cp context (for buckets, the discovering cp).
    async fn process_file(&self, checkpoint: u32, path: String) {
        let result = utils::with_retries(
            self.config.storage_config.max_retries as u32,
            self.config.storage_config.retry_min_delay.as_millis() as u64,
            "process",
            &path,
            || {
                self.operation
                    .process_object(checkpoint, &path, &self.src_store)
            },
        )
        .await;

        match result {
            Ok(ProcessOutcome::Processed) => {
                debug!("Processed: {}", path);
                self.stats.record_success(&path);
            }
            Ok(ProcessOutcome::Skipped) => {
                debug!("Skipped: {}", path);
                self.stats.record_skipped(&path);
            }
            Err(_) => {
                self.stats.record_failure(checkpoint, &path).await;
            }
        }
    }
}

pub use async_trait::async_trait;
