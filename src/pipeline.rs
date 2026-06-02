//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! Retry, concurrency limiting, timeout handling, and bandwidth throttling are delegated
//! to the storage layer (`OpenDAL`).

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::{download_buffer, StorageConfig, StorageRef},
    utils::{self, ArchiveStats},
    xdr_verify::XdrVerificationManager,
};
use futures_util::{
    future::{join, join3, join_all, OptionFuture},
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
    /// `manager` is `Some` iff the pipeline was constructed with
    /// `PipelineConfig::verify = true` — operations should branch on it
    /// to decide between verify-and-record vs existence-only / plain-copy
    /// paths, and call `manager.record_*` to feed per-file XDR data into
    /// the pipeline's manager.
    async fn process_object(
        &self,
        path: &str,
        _src_store: &StorageRef,
        manager: Option<&XdrVerificationManager>,
    ) -> Result<ProcessOutcome, crate::storage::Error>;

    /// Called by `Pipeline::finish` after the manager (if any) has been
    /// drained into `stats.failures`. The operation is responsible for
    /// reporting and deciding `has_failures`-gated success.
    async fn finalize(&self, highest_checkpoint: u32, stats: &ArchiveStats) -> Result<(), Error>;

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
    /// When true, `process_checkpoint` skips `process_history_and_buckets`
    /// entirely — no HAS fetch, no bucket discovery, no bucket fetches.
    /// Used by repair's `retry_failed_checkpoints` inner pipeline, where
    /// chain repair only needs the per-cp xdr files (ledger/tx/results/scp)
    /// and bucket / HAS work is either redundant (`retry_failed_files`
    /// covers bucket repair via `failures.buckets` and HISTORY-flagged
    /// entries) or unnecessary (chain checks don't read bucket content).
    pub skip_history_and_buckets: bool,
    /// Whether to construct the pipeline's XDR verification manager. When
    /// true, `process_object` receives `Some(&XdrVerificationManager)` and
    /// the pipeline drives `verify_and_release` per cp + chain check + drain
    /// at the end of `run_checkpoints`.
    pub verify: bool,
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
    /// XDR verification manager — `Some` iff `config.verify` is true.
    /// Pipeline drives the full manager lifecycle (record-per-file via the
    /// `process_object` parameter, `verify_and_release` per cp in
    /// `process_checkpoint`, `verify_checkpoint_chain` + `record_all_errors`
    /// drain at the end of `run_checkpoints`).
    verification_manager: Option<XdrVerificationManager>,
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
        let verification_manager = config.verify.then(XdrVerificationManager::new);
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
            verification_manager,
        }
    }

    /// Read-only handle to the pipeline's owned `ArchiveStats`. Use this when
    /// a caller (e.g. repair's retry phase) needs to report or inspect the
    /// pipeline's stats after `run_checkpoints` returns.
    pub fn stats(&self) -> &ArchiveStats {
        &self.stats
    }

    /// Full pipeline run — consumes the pipeline.
    ///
    /// Computes checkpoint bounds via the operation, drives the per-cp
    /// loop, then hands off to [`Self::finish`].
    pub async fn run(self) -> Result<(), Error> {
        let (lower_bound, upper_bound) = self
            .operation
            .get_checkpoint_bounds(&self.src_store)
            .await?;

        let total_count = history_format::count_checkpoints_in_range(lower_bound, upper_bound);
        if total_count != 0 {
            let checkpoints =
                (lower_bound..=upper_bound).step_by(history_format::CHECKPOINT_FREQUENCY as usize);
            self.run_checkpoints(checkpoints).await?;
        } else {
            info!("No checkpoints to process");
        }
        self.finish(upper_bound).await
    }

    /// Run the pipeline over an explicit set of checkpoints (possibly
    /// disjoint or non-consecutive). Each checkpoint is dispatched through
    /// `process_checkpoint` and bounded by `config.concurrency`.
    ///
    /// Does **not** call `operation.finalize` — that's [`Self::finish`]'s job.
    /// Callers that want the full lifecycle should use [`Self::run`] (or
    /// pair `run_checkpoints` with an explicit `finish`).
    /// An empty input is a no-op.
    pub async fn run_checkpoints<I>(&self, cps: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = u32>,
    {
        let cps: Vec<u32> = cps.into_iter().collect();
        let total = cps.len();
        if total == 0 {
            return Ok(());
        }

        let num_completed = std::sync::atomic::AtomicUsize::new(0);
        let completed_ref = &num_completed;

        stream::iter(cps)
            .for_each_concurrent(self.config.concurrency, |ck| async move {
                self.process_checkpoint(ck).await;
                let done = completed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if done.is_multiple_of(PROGRESS_REPORTING_FREQUENCY) || done == total {
                    info!("Progress: {}/{} checkpoints processed", done, total);
                }
            })
            .await;

        // Batch-completion: run the cross-cp chain check and drain accumulated
        // manager errors into `stats.failures`.
        if let Some(manager) = &self.verification_manager {
            manager.verify_checkpoint_chain();
            manager.record_all_errors(&mut *self.stats.failures.lock().await);
        }

        Ok(())
    }

    /// Consume the pipeline and run `operation.finalize` with `&stats` for
    /// operation-specific wrap-up (report, has_failures gate, op-specific
    /// side effects like well-known update.
    pub async fn finish(self, highest_checkpoint: u32) -> Result<(), Error> {
        let Self {
            operation, stats, ..
        } = self;
        operation.finalize(highest_checkpoint, &stats).await
    }

    /// Consume the pipeline and return its `ArchiveStats`
    pub fn into_stats(self) -> ArchiveStats {
        let Self { stats, .. } = self;
        stats
    }

    /// Process a single checkpoint: history+buckets concurrently with the
    /// per-checkpoint files (ledger, transactions, results, optional scp).
    ///
    /// Public so external callers (e.g. repair's retry phase, which builds a
    /// `Pipeline<MirrorOperation>` to re-mirror failed checkpoints) can drive
    /// the pipeline machinery directly without `Pipeline::run`'s full bounds +
    /// iteration loop.
    pub async fn process_checkpoint(&self, checkpoint: u32) {
        // Always: the three required per-cp xdr files.
        let cats = join_all(
            ["ledger", "transactions", "results"]
                .map(|cat| self.process_file(checkpoint, checkpoint_path(cat, checkpoint))),
        );

        // Optional: SCP file. Skipped when `skip_optional` is set.
        let scp: OptionFuture<_> = (!self.config.skip_optional)
            .then(|| self.process_file(checkpoint, checkpoint_path("scp", checkpoint)))
            .into();

        // Optional: HAS fetch + bucket discovery + bucket fetches. Skipped
        // by repair's `retry_failed_checkpoints` inner pipeline (chain
        // repair only needs the per-cp xdr files; bucket / HAS work is
        // handled by `retry_failed_files`).
        let history: OptionFuture<_> = (!self.config.skip_history_and_buckets)
            .then(|| self.process_history_and_buckets(checkpoint))
            .into();

        let _ = join3(cats, scp, history).await;

        // Per-cp manager release: drive `verify_and_release` for this cp's
        // accumulated per-file data. Triggers intra-cp completeness, tx/result
        // hash cross-checks, and the internal hash chain check.
        if let Some(manager) = &self.verification_manager {
            manager.verify_and_release(checkpoint);
        }
    }

    /// Fetch + parse + write a checkpoint's history file, then process all the
    /// bucket references it discovers.
    ///
    /// On history fetch/parse failure, `fetch_history_file_state` this method
    /// returns early and buckets are not processed. On success, the history
    /// buffer write and the bucket processing run concurrently.
    pub(crate) async fn process_history_and_buckets(&self, checkpoint: u32) {
        let Some((state, buffer)) = self.fetch_history_file_state(checkpoint).await else {
            return;
        };
        let history_path = checkpoint_path("history", checkpoint);
        let write_history_buffer = self.process_history_file(checkpoint, &history_path, buffer);
        let buckets = self.process_buckets(checkpoint, state);
        let _ = join(write_history_buffer, buckets).await;
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
            || self.operation.process_buffer(path, buffer.clone()),
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
    pub(crate) async fn process_file(&self, checkpoint: u32, path: String) {
        let result = utils::with_retries(
            self.config.storage_config.max_retries as u32,
            self.config.storage_config.retry_min_delay.as_millis() as u64,
            "process",
            &path,
            || {
                self.operation.process_object(
                    &path,
                    &self.src_store,
                    self.verification_manager.as_ref(),
                )
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
