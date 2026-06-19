//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! Retry, concurrency limiting, timeout handling, and bandwidth throttling are delegated
//! to the storage layer (`OpenDAL`).

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::{StorageConfig, StorageRef},
    utils::{self, ArchiveStats},
    xdr_verify::XdrVerificationManager,
};
use futures_util::{
    future::{join3, join_all, OptionFuture},
    stream, StreamExt,
};
use lru::LruCache;
use std::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info};

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

/// Non-error outcome of `Operation::process_object` / `process_history`.
///
/// Returned inside `Ok(_)`; the pipeline maps it to the matching
/// `ArchiveStats` recorder: `Processed` → `record_success`,
/// `Skipped` → `record_skipped`, `NeedsRepair` → `record_failure`. Errors are
/// signaled via `Err(_)` in the surrounding `Result` and recorded by the
/// pipeline as `record_failure` (after retries exhaust).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The operation did its work successfully.
    Processed,
    /// The operation deliberately did no work (e.g. mirror found the file
    /// already present at dst).
    Skipped,
    /// The operation determined this object needs repair but performed no work
    /// (e.g. repair dry-run). Recorded as a failure so it appears in
    /// stats/report, without being an error. Scan/mirror never return this.
    NeedsRepair,
}

/// Result of [`Operation::process_history`].
///
/// `outcome` is recorded into `ArchiveStats` exactly like `process_object`'s
/// (`Processed` → success, `Skipped` → skipped, `NeedsRepair` → failure).
/// `state` carries the parsed HAS for bucket discovery; it is `Some` for
/// `Processed`/`Skipped` and `None` only for `NeedsRepair`.
pub struct HistoryOutcome {
    pub outcome: ProcessOutcome,
    pub state: Option<HistoryFileState>,
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

    /// Process a checkpoint's history (HAS) file end-to-end, returning its
    /// parsed state so the pipeline can walk the bucket references it lists.
    ///
    /// The history-file analogue of [`Operation::process_object`]: the operation
    /// owns the full decision (probe destination, fetch from source, write,
    /// skip) and reports a [`ProcessOutcome`]. `HistoryOutcome::state` is `Some`
    /// whenever a good HAS was obtained (so its buckets should be walked) and
    /// `None` only for `ProcessOutcome::NeedsRepair` (repair dry-run on a
    /// missing/corrupt destination), where bucket discovery is skipped.
    ///
    /// - scan: download from `src_store`, parse, never write → `Processed`.
    /// - mirror: keep a valid dst copy when not overwriting (`Skipped`), else
    ///   copy from source (`Processed`).
    /// - repair: keep a valid dst copy (`Processed`, no write); fetch + write
    ///   from source when missing/corrupt or known-broken; `NeedsRepair` under
    ///   dry-run.
    ///
    /// The HAS is parsed unconditionally (needed for bucket discovery), so there
    /// is no verification-manager parameter.
    async fn process_history(
        &self,
        path: &str,
        src_store: &StorageRef,
    ) -> Result<HistoryOutcome, crate::storage::Error>;

    /// Called by `Pipeline::finish` after the manager (if any) has been
    /// drained into `stats.failures`. The operation is responsible for
    /// reporting and deciding `has_failures`-gated success.
    async fn finalize(
        &self,
        highest_checkpoint: u32,
        stats: &ArchiveStats,
        report_path: Option<&std::path::Path>,
    ) -> Result<(), Error>;
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
    /// Destination store. Currently unread: the history file now reaches its
    /// destination via the operation's own `dst_store`. Retained pending a
    /// follow-up that removes it from `Pipeline::new`.
    #[allow(dead_code)]
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
    /// Optional local path for the JSON status report, passed to
    /// `operation.finalize`. Owned solely by the pipeline (not per-stats) so
    /// sub-stats can't disagree about where the report goes.
    report_path: Option<std::path::PathBuf>,
}

impl<Op: Operation> Pipeline<Op> {
    /// Create a new pipeline.
    /// Callers create storage backends and pass them in directly.
    pub fn new(
        operation: Op,
        config: PipelineConfig,
        src_store: StorageRef,
        dst_store: Option<StorageRef>,
        report_path: Option<std::path::PathBuf>,
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
            report_path,
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
        let cps = cps.into_iter();
        // size_hint's upper bound is the exact count for every caller's
        // iterator (StepBy<RangeInclusive<u32>> on the main path, Vec/set
        // elsewhere), and we only use it for status reporting.
        let total = cps.size_hint().1;
        if total == Some(0) {
            return Ok(());
        }

        let num_completed = std::sync::atomic::AtomicUsize::new(0);
        let completed_ref = &num_completed;

        stream::iter(cps)
            .for_each_concurrent(self.config.concurrency, |ck| async move {
                self.process_checkpoint(ck).await;
                let done = completed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if done.is_multiple_of(PROGRESS_REPORTING_FREQUENCY) || total == Some(done) {
                    if let Some(total) = total {
                        info!("Progress: {done}/{total} checkpoints processed");
                    } else {
                        info!("Progress: {done} checkpoints processed");
                    }
                }
            })
            .await;

        // Batch-completion: run the cross-cp chain check and drain accumulated
        // manager errors into `stats.failures`.
        if let Some(manager) = &self.verification_manager {
            manager.verify_checkpoint_chain();
            manager.drain_all_errors(&mut *self.stats.failures.lock().await);
        }

        Ok(())
    }

    /// Consume the pipeline and run `operation.finalize` with `&stats` for
    /// operation-specific wrap-up (report, has_failures gate, op-specific
    /// side effects like well-known update.
    pub async fn finish(self, highest_checkpoint: u32) -> Result<(), Error> {
        let Self {
            operation,
            stats,
            report_path,
            ..
        } = self;
        operation
            .finalize(highest_checkpoint, &stats, report_path.as_deref())
            .await
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

    /// Process a checkpoint's history (HAS) file via the operation, then process
    /// every bucket it references.
    ///
    /// The operation owns the probe/fetch/write/skip decision and returns the
    /// parsed state. Buckets are walked only when a good HAS was obtained
    /// (`state.is_some()`); a `NeedsRepair` outcome (repair dry-run) or an error
    /// records the HISTORY failure and skips bucket discovery.
    pub(crate) async fn process_history_and_buckets(&self, checkpoint: u32) {
        let history_path = checkpoint_path("history", checkpoint);
        let result = utils::with_retries(
            self.config.storage_config.max_retries as u32,
            self.config.storage_config.retry_min_delay.as_millis() as u64,
            "process history",
            &history_path,
            || self.operation.process_history(&history_path, &self.src_store),
        )
        .await;

        match result {
            Ok(HistoryOutcome { outcome, state }) => {
                self.record_outcome(checkpoint, &history_path, Ok(outcome))
                    .await;
                if let Some(state) = state {
                    self.process_buckets(checkpoint, state).await;
                }
            }
            Err(e) => {
                self.record_outcome(checkpoint, &history_path, Err(e)).await;
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

    /// Record the result of processing a single object/buffer into `stats`,
    /// using the shared `ProcessOutcome` → recorder mapping. Used by both
    /// `process_file` and `process_history_and_buckets`.
    async fn record_outcome(
        &self,
        checkpoint: u32,
        path: &str,
        result: Result<ProcessOutcome, crate::storage::Error>,
    ) {
        match result {
            Ok(ProcessOutcome::Processed) => {
                debug!("Processed: {}", path);
                self.stats.record_success(path);
            }
            Ok(ProcessOutcome::Skipped) => {
                debug!("Skipped: {}", path);
                self.stats.record_skipped(path);
            }
            Ok(ProcessOutcome::NeedsRepair) => {
                debug!("Needs repair: {}", path);
                self.stats.record_failure(checkpoint, path).await;
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
        self.record_outcome(checkpoint, &path, result).await;
    }
}

pub use async_trait::async_trait;
