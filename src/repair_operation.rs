//! Repair operation — detects and fixes corrupted or missing files in a Stellar History Archive.
//!
//! Implements the `Operation` trait and runs inside the existing `Pipeline`.
//! Pipeline source = SOURCE archive (known-good). RepairOperation holds both
//! `src_store` and `dst_store`; the destination is what's being repaired.
//!
//! **Per-file flow** (in `process_object`, matching scan/mirror's "all verification
//! in process_object" pattern): try the destination first — open dst reader, parse
//! XDR (or verify bucket hash), and on success record the parsed data into the
//! verification manager and stop. On parse failure or missing dst file, fetch
//! from src via `verify_and_write_xdr` (or `verify_and_write_bucket`) and record
//! the result. In **failed-list mode** (`known_broken` is `Some` — the retry
//! stage and plan mode) a listed file skips the dst probe entirely and is
//! fetched from src directly; only unlisted (discovered) files get the probe.
//!
//! **Retry phase** (in `finalize`): after the main pass completes, the manager
//! has data for every file in the range. Run `verify_checkpoint_chain` (cross-
//! checkpoint chain) and accumulate any failing checkpoints (both ends of a
//! break) into `retry_checkpoints`. `finalize_checkpoint` already added per-
//! checkpoint cross-file failures during the main pass. For each retry
//! checkpoint, drive `pipeline::process_checkpoint` with a fresh
//! `MirrorOperation` (overwrite=true, verify=false) — pure copy from src,
//! trusting src is canonical.
//!
//! **Manual mode** (`run_manual`): applies a plan (an `ArchiveReport`, typically
//! from a prior `--dry-run`) by seeding the failure set and running the same
//! two-stage retry, bypassing the main-pass pipeline. `.well-known` is restored
//! from the checkpoint carried in the plan.

use crate::history_format;
use crate::mirror_operation::MirrorOperation;
use crate::pipeline::{
    self, async_trait, HistoryOutcome, Operation, Pipeline, PipelineConfig, ProcessOutcome,
};
use crate::storage::{self, Error as StorageError, ErrorClass, StorageRef};
use crate::utils::{self, ArchiveStats, FailureTracker};
use crate::xdr_verify::{self, XdrParseResult, XdrVerificationManager};
use futures_util::{stream, StreamExt};
use opendal::Reader;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tracing::{error, info, warn};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Archive repair failed")]
    RepairFailed,

    #[error("Invalid plan: {0}")]
    InvalidPlan(String),

    #[error(transparent)]
    Utils(#[from] utils::Error),

    #[error(transparent)]
    Storage(#[from] storage::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("History format error: {0}")]
    HistoryFormat(#[from] history_format::Error),

    #[error("Scan operation error: {0}")]
    ScanOperation(#[from] crate::scan_operation::Error),

    #[error("Report error: {0}")]
    Report(#[from] crate::report::ReportError),
}

// ============================================================================
// RepairOperation
// ============================================================================

pub struct RepairOperation {
    src_store: StorageRef,
    dst_store: StorageRef,
    low: Option<u32>,
    high: Option<u32>,
    dry_run: bool,
    /// Outer pipeline's config — held so the retry-phase inner pipelines
    /// (`retry_failed_files`, `retry_failed_checkpoints`) can be constructed
    /// with the same concurrency / skip_optional / verify / storage_config.
    /// Note: repair's own `process_object` does not read `pipeline_config.verify`;
    /// it branches on whether the `manager` parameter (from the outer pipeline)
    /// is `Some`.
    pipeline_config: PipelineConfig,
    /// Set during get_checkpoint_bounds if dest .well-known needs repair
    well_known_needs_repair: AtomicBool,
    /// Failures already known broken — `Some` only in the failed-list pipelines
    /// (the stage-2 file retry, which plan mode also drives), seeded from the
    /// main pass's stats or the plan. A file in this set is fetched from src
    /// directly, skipping the dst probe entirely. `None` (regular repair: main
    /// pass, dry-run) probes dst-first for every file.
    known_broken: Option<FailureTracker>,
}

impl RepairOperation {
    pub fn new(
        src_store: StorageRef,
        dst_store: StorageRef,
        low: Option<u32>,
        high: Option<u32>,
        dry_run: bool,
        pipeline_config: PipelineConfig,
    ) -> Self {
        debug_assert!(
            dst_store.supports_writes(),
            "RepairOperation requires a writable destination store"
        );
        Self {
            src_store,
            dst_store,
            low,
            high,
            dry_run,
            pipeline_config,
            well_known_needs_repair: AtomicBool::new(false),
            known_broken: None,
        }
    }

    /// Switch this operation into failed-list mode: every file in
    /// `known_broken` is fetched from src directly (no dst probe); files not
    /// in the set keep the regular dst-first probe.
    #[must_use]
    pub(crate) fn with_known_broken(mut self, known_broken: FailureTracker) -> Self {
        debug_assert!(self.known_broken.is_none());
        self.known_broken = Some(known_broken);
        self
    }

    /// Manual repair driven by a plan: apply an `ArchiveReport` (typically
    /// produced by an earlier `--dry-run`) by seeding the failure set and
    /// running the same two-stage retry the main flow uses — phase 1 over
    /// files/buckets, phase 2 over checkpoints. The plan is trusted: every
    /// listed item is re-fetched from source unconditionally (the destination
    /// is not re-audited). Only buckets *discovered* by re-walking a listed
    /// HISTORY are validated on dst and fetched just when missing or invalid.
    /// `--verify` works exactly as in regular repair: it governs validation
    /// of the downloaded source content and the probing of discovered files.
    ///
    /// `.well-known` is restored from the checkpoint carried in the plan, so no
    /// archive-bound recomputation is needed.
    pub async fn run_manual(
        &self,
        plan: crate::report::ArchiveReport,
        report_path: Option<&std::path::Path>,
    ) -> Result<(), Error> {
        let tracker = plan
            .into_failures()
            .map_err(|e| Error::InvalidPlan(e.to_string()))?;
        if tracker.is_empty() {
            info!("Plan is empty; nothing to repair");
            return Ok(());
        }
        let well_known_cp = tracker.well_known;

        let stats = ArchiveStats::new();
        *stats.failures.lock().await = tracker;

        let file_retry_stats = self.retry_failed_files(&stats).await;
        file_retry_stats.report("repair file retry").await;

        let checkpoint_retry_stats = self.retry_failed_checkpoints(&stats).await;
        checkpoint_retry_stats
            .report("repair checkpoint retry")
            .await;

        // `.well_known` restoration runs after the retry stages because the
        // history file it copies may itself be one the file retry just
        // restored.
        if let Some(cp) = well_known_cp {
            if !self.repair_well_known(cp).await {
                file_retry_stats.failures.lock().await.record_well_known(cp);
            }
        }

        // Two sections — plan-driven repair has no main pass.
        if let Some(path) = report_path {
            let out = crate::report::MultiSectionReport {
                version: crate::report::REPORT_VERSION,
                sections: [
                    (
                        "file_retry".to_string(),
                        file_retry_stats.report_section().await,
                    ),
                    (
                        "checkpoint_retry".to_string(),
                        checkpoint_retry_stats.report_section().await,
                    ),
                ]
                .into_iter()
                .collect(),
            };
            crate::report::write_to_path(path, &out)?;
        }

        if file_retry_stats.has_failures().await || checkpoint_retry_stats.has_failures().await {
            return Err(Error::RepairFailed);
        }
        Ok(())
    }

    /// Check whether dst already has a good copy of `path` and (when verify
    /// is on) record the parsed data into the manager.
    ///
    /// Returns:
    /// - `Ok(true)` — dst is acceptable; caller skips src fetch.
    /// - `Ok(false)` — dst is missing or corrupt, OR the file is in
    ///   `known_broken` (failed-list mode: no dst probe at all); caller
    ///   should fetch from src.
    ///   NotFound on `open_reader`/`exists()` is treated as "missing" so the
    ///   repair can fetch from src immediately. Same for parse failures on a
    ///   present-but-corrupt file.
    /// - `Err(StorageError)` — non-NotFound storage error on the dst probe
    ///   (permission denied, transient I/O, malformed backend response, etc.),
    ///   OR `path` doesn't match any recognized archive file type (contract
    ///   violation). The pipeline wraps `process_object` in `with_retries`, so
    ///   transient errors (`ErrorClass::Retry`) are retried automatically. If
    ///   the error persists after retries there's a real filesystem issue at
    ///   `path`; fetching from src is likely to fail too, so we record the
    ///   failure and move on instead of attempting a src fetch under broken
    ///   conditions.
    ///
    /// When `manager` is `None` (verify off), only existence is checked —
    /// content is not validated and nothing is recorded.
    async fn verify_and_record_dst(
        &self,
        path: &str,
        manager: Option<&XdrVerificationManager>,
    ) -> Result<bool, StorageError> {
        // Failed-list mode: a file already recorded broken is fetched from src
        // directly to avoid the expensive verification (bucket decompress+hash,
        // transactions parse). Files NOT in the list — e.g. buckets discovered
        // by re-walking a listed HISTORY — are probed normally so a still-valid
        // copy isn't re-downloaded.
        if let Some(known_broken) = &self.known_broken {
            if known_broken.contains_path(path) {
                return Ok(false);
            }
        }

        let Some(manager) = manager else {
            return match self.dst_store.exists(path).await {
                Ok(present) => Ok(present),
                Err(e) if e.class == ErrorClass::NotFound => Ok(false),
                Err(e) => Err(e),
            };
        };

        // Buckets and SCP files have no per-checkpoint manager state — verify
        // structurally on dst, no recording.
        if history_format::is_bucket_file(path) {
            return self
                .check_dst_or_missing(path, |reader| async move {
                    crate::verify::verify_bucket_stream(path, reader)
                        .await
                        .is_ok()
                })
                .await;
        }
        if history_format::is_scp_file(path) {
            return self
                .check_dst_or_missing(path, |reader| async move {
                    xdr_verify::parse_scp_stream(path, reader).await.is_ok()
                })
                .await;
        }

        // XDR file types: open, parse, and on success record into the manager.
        if history_format::is_ledger_header_file(path) {
            return self
                .check_dst_or_missing(path, |reader| async move {
                    match xdr_verify::parse_ledger_header_stream(path, reader).await {
                        Ok(data) => {
                            manager.record(path, XdrParseResult::Ledger(data));
                            true
                        }
                        Err(_) => false,
                    }
                })
                .await;
        }
        if history_format::is_transactions_file(path) {
            return self
                .check_dst_or_missing(path, |reader| async move {
                    match xdr_verify::parse_transactions_stream(path, reader).await {
                        Ok(hashes) => {
                            manager.record(path, XdrParseResult::Transactions(hashes));
                            true
                        }
                        Err(_) => false,
                    }
                })
                .await;
        }
        if history_format::is_results_file(path) {
            return self
                .check_dst_or_missing(path, |reader| async move {
                    match xdr_verify::parse_results_stream(path, reader).await {
                        Ok(hashes) => {
                            manager.record(path, XdrParseResult::Results(hashes));
                            true
                        }
                        Err(_) => false,
                    }
                })
                .await;
        }

        Err(StorageError::fatal(format!(
            "verify_and_record_dst: unrecognized archive path '{path}' \
             (expected bucket / ledger / transactions / results / scp)"
        )))
    }

    /// Run a content check on the dst copy of `path`, treating a missing
    /// dst file as the check failing.
    ///
    /// Returns:
    /// - `Ok(true)` — dst file opened AND `f(reader)` returned `true`.
    /// - `Ok(false)` — dst file is missing (`open_reader` returned `NotFound`),
    ///   OR `f(reader)` returned `false`.
    /// - `Err(_)` — any non-`NotFound` storage error from `open_reader`.
    ///
    /// `f` is typically a parse or hash verification that returns `true`
    /// when the dst content is good.
    async fn check_dst_or_missing<F, Fut>(&self, path: &str, f: F) -> Result<bool, StorageError>
    where
        F: FnOnce(Reader) -> Fut,
        Fut: std::future::Future<Output = bool> + Send,
    {
        match self.dst_store.open_reader(path).await {
            Ok(reader) => Ok(f(reader).await),
            Err(e) if e.class == ErrorClass::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Build a fresh `Pipeline<MirrorOperation>` for the per-checkpoint retry
    /// stage: overwrite=true (re-mirror whole cps whose files are individually
    /// valid but cross-file/chain inconsistent — a dst-first probe would wrongly
    /// skip them), allow_mirror_gaps=true (the retry set is disjoint),
    /// update_well_known=false (a partial-set retry can't claim contiguous
    /// coverage). `skip_history_and_buckets=true` because chain repair needs only
    /// the per-cp xdr files; bucket / HAS work is handled by the file-retry stage.
    fn build_checkpoint_retry_pipeline(&self) -> Pipeline<MirrorOperation> {
        let mut retry_config = self.pipeline_config.clone();
        retry_config.skip_history_and_buckets = true;
        let mirror_op = MirrorOperation::new(
            self.src_store.clone(),
            self.dst_store.clone(),
            /*overwrite=*/ true,
            None,
            None,
            /*allow_mirror_gaps=*/ true,
            retry_config.clone(),
            /*update_well_known=*/ false,
        );
        Pipeline::new(
            mirror_op,
            retry_config,
            /*report_path=*/ None,
        )
    }

    /// Build a fresh `Pipeline<RepairOperation>` for the per-file retry stage.
    /// Reuses repair's per-file logic (`process_object`) in failed-list mode:
    /// seeded with `known_broken`, so every listed file (any type) skips the
    /// dst probe and is re-fetched directly, while transitively-discovered
    /// buckets are validated first. `dry_run` is always false here (the retry
    /// stages early-return under dry_run).
    fn build_file_retry_pipeline(&self, known_broken: FailureTracker) -> Pipeline<RepairOperation> {
        let repair_op = RepairOperation::new(
            self.src_store.clone(),
            self.dst_store.clone(),
            self.low,
            self.high,
            /*dry_run=*/ false,
            self.pipeline_config.clone(),
        )
        .with_known_broken(known_broken);
        Pipeline::new(
            repair_op,
            self.pipeline_config.clone(),
            /*report_path=*/ None,
        )
    }

    /// Per-file retry — re-fetch individual files that the main pass failed
    /// to repair, on a **fresh `Pipeline<RepairOperation>`** built by
    /// [`Self::build_file_retry_pipeline`].
    ///
    /// For each broken file recorded in `parent_stats.failures` (per-cp file
    /// flags + bucket hashes), dispatch through the inner pipeline:
    ///
    /// - per-cp standard files (LEDGER/TRANSACTIONS/RESULTS/SCP) and buckets
    ///   → `Pipeline::process_file`.
    /// - HISTORY → `Pipeline::process_history_and_buckets` (re-walks bucket
    ///   refs, covering buckets the main pass never probed when history
    ///   failed before bucket discovery).
    ///
    /// We don't run the full pipeline (no `process_checkpoint`, no chain
    /// verification), only the per-item primitives. Stats are extracted via
    /// `into_stats` (bypassing mirror's `finalize` and its `has_failures`
    /// gate). Returns the inner's stats directly — no merging into the parent.
    ///
    /// Per-file validation still runs when `--verify` is on (the manager is
    /// carried via `pipeline_config.verify`), so each re-fetched file is checked
    /// before commit. Cross-file / chain checks don't run here and don't need
    /// to: single-file retry has no siblings loaded to cross-check, and any cp
    /// with a real cross-file/chain inconsistency is in `failures.checkpoints`,
    /// handled by [`Self::retry_failed_checkpoints`].
    pub(crate) async fn retry_failed_files(&self, parent_stats: &ArchiveStats) -> ArchiveStats {
        if self.dry_run {
            return ArchiveStats::new();
        }

        // One snapshot of the main pass's failures drives both the work list
        // (what to retry) and `known_broken` (which files to fetch directly vs.
        // probe). `parent_stats` is not mutated during file-retry, so the
        // snapshot stays consistent. The snapshot is stays read-only (built
        // once).
        let known_broken = parent_stats.failures.lock().await.clone();
        let work = build_failed_files_work_list(&known_broken);
        if work.is_empty() {
            return ArchiveStats::new();
        }

        info!("Retrying {} failed file(s)", work.len());

        // File-retry runs in failed-list mode: every listed file is fetched
        // from src directly (no dst probe); transitively discovered buckets are
        // validated on dst and re-fetched only if missing/invalid.
        //
        // HISTORY work re-walks its referenced buckets through the pipeline's
        // bucket scheduler; direct bucket work enters that same scheduler via
        // `process_bucket_file`. So a bucket listed directly AND referenced by a
        // failed HISTORY is processed once per file-retry run (shared LRU gate).
        let file_retry_pipeline = self.build_file_retry_pipeline(known_broken);
        stream::iter(work.into_iter())
            .for_each_concurrent(self.pipeline_config.concurrency, |(cp, path)| {
                let pipeline = &file_retry_pipeline;
                async move {
                    if history_format::is_history_file(&path) {
                        pipeline.process_history_and_buckets(cp).await;
                    } else if history_format::is_bucket_file(&path) {
                        pipeline.process_bucket_file(cp, path).await;
                    } else {
                        pipeline.process_file(cp, path).await;
                    }
                }
            })
            .await;
        file_retry_pipeline.into_stats()
    }

    /// Per-checkpoint retry — re-mirror whole checkpoints flagged by cross-
    /// file or cross-cp chain checks, on a **fresh `Pipeline<MirrorOperation>`**
    /// built by [`Self::build_checkpoint_retry_pipeline`].
    ///
    /// For each cp in `parent_stats.failures.checkpoints`, drive the inner
    /// pipeline's `run_checkpoints` to re-fetch every file in that cp from src
    /// (overwrite=true). `run_checkpoints` exercises the full per-cp loop
    /// (`verify_and_release` per cp) AND runs the cross-cp chain check + drain
    /// at its end, so `checkpoint_retry_pipeline.stats().failures.checkpoints`
    /// is fully populated by the time it returns. We just call `into_stats`
    /// to extract — no manual drain step.
    pub(crate) async fn retry_failed_checkpoints(
        &self,
        parent_stats: &ArchiveStats,
    ) -> ArchiveStats {
        if self.dry_run {
            return ArchiveStats::new();
        }

        let retry_cps: Vec<u32> = {
            let f = parent_stats.failures.lock().await;
            f.checkpoints.iter().copied().collect()
        };
        if retry_cps.is_empty() {
            return ArchiveStats::new();
        }

        info!(
            "Retrying {} failed checkpoint(s) (cross-file / chain failures)",
            retry_cps.len()
        );

        // `retry_failed_checkpoints` only needs per-cp xdr files. Chain
        // checks don't read bucket content, and bucket failures are
        // covered by `retry_failed_files` (either via `failures.buckets`
        // direct fetches or via its HISTORY entries, which trigger
        // `process_history_and_buckets`). So skip HAS + bucket work in
        // the inner `process_checkpoint` (the builder sets that).
        let checkpoint_retry_pipeline = self.build_checkpoint_retry_pipeline();
        let _ = checkpoint_retry_pipeline.run_checkpoints(retry_cps).await;
        checkpoint_retry_pipeline.into_stats()
    }

    /// Restore `.well-known/stellar-history.json` by copying the highest
    /// checkpoint's history file on the destination — a local dst-to-dst copy,
    /// not a fetch from the source.
    ///
    /// Returns `true` if `.well-known` is in its intended state (restored, or a
    /// no-op on a non-filesystem backend) and `false` if a needed restoration
    /// failed, so the caller can fail the run rather than report success with
    /// `.well-known` still broken.
    async fn repair_well_known(&self, highest_checkpoint: u32) -> bool {
        let history_path = history_format::checkpoint_path("history", highest_checkpoint);
        let well_known_path = history_format::ROOT_WELL_KNOWN_PATH;

        let Some(base_path) = self.dst_store.get_base_path() else {
            // Non-filesystem backend: nothing to copy locally. R-3.11 permits
            // a silent no-op (no writable non-filesystem backend exists today).
            return true;
        };

        let src_file = base_path.join(&history_path);
        let dst_file = base_path.join(well_known_path);

        if !tokio::fs::try_exists(&src_file).await.unwrap_or(false) {
            error!(
                "Cannot repair .well-known: history file at checkpoint {} not found",
                highest_checkpoint
            );
            return false;
        }

        if let Some(parent) = dst_file.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!("Failed to create .well-known directory: {}", e);
                return false;
            }
        }

        match tokio::fs::copy(&src_file, &dst_file).await {
            Ok(_) => {
                info!(
                    "Restored .well-known from checkpoint {} (0x{:08x})",
                    highest_checkpoint, highest_checkpoint
                );
                true
            }
            Err(e) => {
                error!("Failed to restore .well-known: {}", e);
                false
            }
        }
    }
}

// ============================================================================
// Repair helpers (free functions)
// ============================================================================

/// Build a `(cp, path)` work list from a snapshot of the failure tracker.
/// Read-only — captures every uniquely-identifiable failed file in `failures`.
/// The caller snapshots `parent_stats.failures` once and reuses it for both
/// this work list and the inner pipeline's `known_broken`.
///
/// Non-HISTORY per-cp file entries whose cp is also in `failures.checkpoints`
/// are skipped — `retry_failed_checkpoints` will re-mirror the whole cp
/// anyway, so re-fetching individual files in `retry_failed_files` is
/// redundant. HISTORY entries are kept unconditionally because the HISTORY
/// repair path dispatches through `Pipeline::process_history_and_buckets(cp)`,
/// which is the only bucket-discovery path for buckets that the main pass
/// never probed (the case where the history download itself failed before
/// bucket refs were walked). Bucket entries are also kept unconditionally —
/// they're content-addressed, not cp-keyed, and `retry_failed_files` is the
/// only place that repairs `failures.buckets`. Keeping a bucket here that a
/// failed HISTORY also references no longer means double processing: both routes
/// go through `Pipeline::process_bucket_file`, which dedupes on the bucket hash.
fn build_failed_files_work_list(failures: &FailureTracker) -> Vec<(u32, String)> {
    let mut work = Vec::new();

    for (&cp, flags) in &failures.files {
        let cp_in_chain_retry = failures.checkpoints.contains(&cp);
        for (bit, prefix) in [
            (utils::FileFlags::HISTORY, "history"),
            (utils::FileFlags::LEDGER, "ledger"),
            (utils::FileFlags::TRANSACTIONS, "transactions"),
            (utils::FileFlags::RESULTS, "results"),
            (utils::FileFlags::SCP, "scp"),
        ] {
            if !flags.has(bit) {
                continue;
            }
            // Skip non-HISTORY per-cp files when `retry_failed_checkpoints`
            // will re-mirror this whole cp anyway. HISTORY is kept because
            // its repair path in `retry_failed_files` also re-walks bucket
            // refs.
            if bit != utils::FileFlags::HISTORY && cp_in_chain_retry {
                continue;
            }
            work.push((cp, history_format::checkpoint_path(prefix, cp)));
        }
    }

    for hash in &failures.buckets {
        if let Ok(path) = history_format::bucket_path(&hex::encode(hash.0)) {
            // cp context is 0 — buckets are content-addressed and
            // `record_failure`'s bucket-path dispatch ignores cp.
            work.push((0, path));
        }
    }

    work
}

#[async_trait]
impl Operation for RepairOperation {
    async fn get_checkpoint_bounds(&self) -> Result<(u32, u32), pipeline::Error> {
        // Try destination .well-known first (determines what range to repair)
        let dst_result = utils::fetch_well_known_history_file(
            &self.dst_store,
            0, // no retries for local filesystem
            self.pipeline_config
                .storage_config
                .retry_min_delay
                .as_millis() as u64,
        )
        .await;

        let checkpoint = match dst_result {
            Ok(state) => history_format::round_to_lower_checkpoint(state.current_ledger),
            Err(e) => {
                warn!(
                    "Destination .well-known is unreadable ({}), falling back to source",
                    e
                );
                self.well_known_needs_repair.store(true, Ordering::Relaxed);

                // Fall back to source .well-known
                let src_state = utils::fetch_well_known_history_file(
                    &self.src_store,
                    self.pipeline_config.storage_config.max_retries as u32,
                    self.pipeline_config
                        .storage_config
                        .retry_min_delay
                        .as_millis() as u64,
                )
                .await
                .map_err(|e| pipeline::Error::RepairOperation(Error::Utils(e)))?;
                history_format::round_to_lower_checkpoint(src_state.current_ledger)
            }
        };

        utils::compute_checkpoint_bounds(checkpoint, self.low, self.high)
            .map_err(|e| pipeline::Error::RepairOperation(Error::Utils(e)))
    }

    /// Process a file: try dst first (verify + record into manager), and on
    /// missing/corrupt fetch from src and record the parsed XDR result into the
    /// manager.
    async fn process_object(
        &self,
        path: &str,
        manager: Option<&XdrVerificationManager>,
    ) -> Result<ProcessOutcome, StorageError> {
        if self.verify_and_record_dst(path, manager).await? {
            return Ok(ProcessOutcome::Processed);
        }
        if self.dry_run {
            // Record the broken file/bucket into the pipeline's stats (via the
            // NeedsRepair recorder) without fetching or writing.
            return Ok(ProcessOutcome::NeedsRepair);
        }
        xdr_verify::fetch_verify_and_write(&self.src_store, &self.dst_store, path, manager).await?;
        Ok(ProcessOutcome::Processed)
    }

    /// Process the history file: probe the destination first (a valid copy is
    /// kept as-is, with NO write — symmetric with `verify_and_record_dst`), and
    /// fetch from source only when the dst copy is missing/corrupt or the file
    /// is on the known-broken list. Returns the parsed state so the pipeline can
    /// walk bucket references.
    async fn process_history(&self, path: &str) -> Result<HistoryOutcome, StorageError> {
        // Failed-list mode: a listed file is fetched from source directly (no dst
        // probe), honoring `known_broken` exactly as verify_and_record_dst does.
        let listed = self
            .known_broken
            .as_ref()
            .is_some_and(|kb| kb.contains_path(path));

        if !listed {
            if let Ok(buffer) = storage::download_buffer(&self.dst_store, path).await {
                if let Ok(state) = history_format::parse_history(&buffer, path) {
                    // Healthy dst copy: keep it, do not write.
                    return Ok(HistoryOutcome {
                        outcome: ProcessOutcome::Processed,
                        state: Some(state),
                    });
                }
                // present but corrupt -> fall through and repair
            }
        }

        // Destination missing/corrupt, or this file is on the known-broken list.
        if self.dry_run {
            // Report needs-repair; never fetch or write under dry-run.
            return Ok(HistoryOutcome {
                outcome: ProcessOutcome::NeedsRepair,
                state: None,
            });
        }

        // Fetch from source, parse (before writing), then write to destination.
        let buffer = storage::download_buffer(&self.src_store, path).await?;
        let state = history_format::parse_history(&buffer, path)
            .map_err(|e| StorageError::fatal(format!("failed to parse history {path}: {e}")))?;
        storage::write_buffer_with_cleanup(&self.dst_store, path, buffer).await?;
        Ok(HistoryOutcome {
            outcome: ProcessOutcome::Processed,
            state: Some(state),
        })
    }

    async fn finalize(
        &self,
        highest_checkpoint: u32,
        stats: &ArchiveStats,
        report_path: Option<&std::path::Path>,
    ) -> Result<(), pipeline::Error> {
        if self.well_known_needs_repair.load(Ordering::Relaxed) {
            stats
                .failures
                .lock()
                .await
                .record_well_known(highest_checkpoint);
        }

        if self.dry_run {
            stats.report("repair").await;
            if let Some(path) = report_path {
                let report = crate::report::ArchiveReport {
                    version: crate::report::REPORT_VERSION,
                    section: stats.report_section().await,
                };
                crate::report::write_to_path(path, &report)
                    .map_err(|e| pipeline::Error::RepairOperation(Error::Report(e)))?;
            }
            return Ok(());
        }

        stats.report("repair main").await;

        // Per-file retry: re-fetch every entry in failures.files /
        // failures.buckets. Returns its own stats — no merging.
        let file_retry_stats = self.retry_failed_files(stats).await;
        file_retry_stats.report("repair file retry").await;

        // Per-checkpoint retry: re-mirror every cp in failures.checkpoints.
        // Returns its own stats (including any chain errors surfaced
        // post-retry).
        let checkpoint_retry_stats = self.retry_failed_checkpoints(stats).await;
        checkpoint_retry_stats
            .report("repair checkpoint retry")
            .await;

        // .well-known restoration runs *after* the retry stages. It copies the
        // highest checkpoint's history file on dst, and that history file may
        // itself be what the file-retry stage just restored (when its main-pass
        // fetch failed).
        if self.well_known_needs_repair.load(Ordering::Relaxed) {
            let repair_success = self.repair_well_known(highest_checkpoint).await;
            if !repair_success {
                file_retry_stats
                    .failures
                    .lock()
                    .await
                    .record_well_known(highest_checkpoint);
            }
        }

        // Full-context report: one section per stage (main pass + both retries).
        // Built and written once — all three stats are in hand here.
        if let Some(path) = report_path {
            let report = crate::report::MultiSectionReport {
                version: crate::report::REPORT_VERSION,
                sections: [
                    ("main_pass".to_string(), stats.report_section().await),
                    (
                        "file_retry".to_string(),
                        file_retry_stats.report_section().await,
                    ),
                    (
                        "checkpoint_retry".to_string(),
                        checkpoint_retry_stats.report_section().await,
                    ),
                ]
                .into_iter()
                .collect(),
            };
            crate::report::write_to_path(path, &report)
                .map_err(|e| pipeline::Error::RepairOperation(Error::Report(e)))?;
        }

        // Overall success = both retries recovered everything they attempted and
        // .well-known restoration (if needed) succeeded.
        let file_retry_failed = file_retry_stats.has_failures().await;
        let checkpoint_retry_failed = checkpoint_retry_stats.has_failures().await;
        if file_retry_failed || checkpoint_retry_failed {
            return Err(Error::RepairFailed.into());
        }

        Ok(())
    }

}
