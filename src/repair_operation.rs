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
//! the result.
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
//! **Manual mode** (`run_manual`): downloads a fixed list of paths directly,
//! bypassing the pipeline and the retry machinery.

use crate::history_format;
use crate::mirror_operation::MirrorOperation;
use crate::pipeline::{self, async_trait, Operation, Pipeline, PipelineConfig, ProcessOutcome};
use crate::storage::{
    self, cleanup_partial_file, Error as StorageError, ErrorClass, StorageConfig, StorageRef,
};
use crate::utils::{self, ArchiveStats};
use crate::xdr_verify::{self, XdrParseResult, XdrVerificationManager};
use futures_util::{stream, StreamExt};
use opendal::Buffer;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Archive repair failed")]
    RepairFailed,

    #[error("Invalid file list: {0}")]
    InvalidFileList(String),

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
}

/// How often to report progress (every N files) in manual mode
const MANUAL_PROGRESS_FREQUENCY: usize = 100;

// ============================================================================
// RepairOperation
// ============================================================================

pub struct RepairOperation {
    src_store: StorageRef,
    dst_store: StorageRef,
    low: Option<u32>,
    high: Option<u32>,
    /// Kept (despite the verification manager moving to Pipeline) because
    /// `retry_failed_files` / `retry_failed_checkpoints` build inner
    /// `Pipeline<MirrorOperation>` instances and need to set
    /// `PipelineConfig::verify` on them. Repair's own `process_object`
    /// doesn't read this field — it branches on whether the `manager`
    /// parameter (from the outer pipeline) is `Some`.
    verify: bool,
    dry_run: bool,
    concurrency: usize,
    skip_optional: bool,
    storage_config: StorageConfig,
    /// Set during get_checkpoint_bounds if dest .well-known needs repair
    well_known_needs_repair: AtomicBool,
    /// Dry-run counter: files that would be repaired
    needs_repair_count: AtomicU64,
}

impl RepairOperation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        src_store: StorageRef,
        dst_store: StorageRef,
        low: Option<u32>,
        high: Option<u32>,
        verify: bool,
        dry_run: bool,
        concurrency: usize,
        skip_optional: bool,
        storage_config: &StorageConfig,
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
            verify,
            dry_run,
            concurrency,
            skip_optional,
            storage_config: storage_config.clone(),
            well_known_needs_repair: AtomicBool::new(false),
            needs_repair_count: AtomicU64::new(0),
        }
    }

    /// Manual mode: download a fixed list of paths directly (no checkpoint
    /// iteration, no bucket discovery, no verification manager, no retry).
    pub async fn run_manual(&self, files: &[String]) -> Result<(), Error> {
        info!("Manual repair mode: {} files specified", files.len());
        let files = validate_file_list(files)?;
        if files.is_empty() {
            info!("No files need repair");
            return Ok(());
        }
        info!("{} file(s) to repair", files.len());

        if self.dry_run {
            report_dry_run(&files);
            return Ok(());
        }

        let stats = ArchiveStats::new();
        let completed = AtomicUsize::new(0);
        let total = files.len();

        stream::iter(files.iter())
            .for_each_concurrent(self.concurrency, |path| {
                let stats = &stats;
                let completed = &completed;
                async move {
                    let result = utils::with_retries(
                        self.storage_config.max_retries as u32,
                        self.storage_config.retry_min_delay.as_millis() as u64,
                        "repair",
                        path,
                        // Manual mode: no verification manager. Use plain
                        // src-fetch / dst-copy semantics.
                        || async { self.fetch_from_src(path, None).await.map(|_| ()) },
                    )
                    .await;
                    match result {
                        Ok(()) => {
                            debug!("Repaired: {}", path);
                            stats.record_success(path);
                        }
                        Err(e) => {
                            error!("Failed to repair {}: {}", path, e);
                            let cp = history_format::checkpoint_from_path(path).unwrap_or(0);
                            stats.record_failure(cp, path).await;
                        }
                    }
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    if done.is_multiple_of(MANUAL_PROGRESS_FREQUENCY) || done == total {
                        info!("Repair progress: {}/{} files", done, total);
                    }
                }
            })
            .await;

        stats.report("repair").await;
        if stats.has_failures().await {
            return Err(Error::RepairFailed);
        }
        Ok(())
    }

    /// Increment the dry-run counter (signalling "we'd repair this file but
    /// did nothing"). Used when dst checks fail and we'd otherwise fetch from src.
    fn note_dry_run(&self, path: &str) {
        self.needs_repair_count.fetch_add(1, Ordering::Relaxed);
        debug!("Dry run: would repair {}", path);
    }

    /// Fetch a file from src and write to dst with optional verification.
    /// Returns the parsed XDR data (for XDR files when `verify` is on) so the
    /// caller can record it into the manager. Returns `None` for buckets,
    /// non-verified copies, and unrecognized file types.
    async fn fetch_from_src(
        &self,
        path: &str,
        manager: Option<&XdrVerificationManager>,
    ) -> Result<Option<XdrParseResult>, StorageError> {
        let reader = self.src_store.open_reader(path).await?;
        if manager.is_some() {
            if history_format::is_bucket_file(path) {
                return match crate::verify::verify_and_write_bucket(path, reader, &self.dst_store)
                    .await
                {
                    Ok(()) => Ok(None),
                    Err(e) => {
                        if let Err(cleanup_err) = cleanup_partial_file(&self.dst_store, path).await
                        {
                            error!("Failed to cleanup partial file {}: {}", path, cleanup_err);
                        }
                        Err(e)
                    }
                };
            }
            if history_format::is_ledger_header_file(path)
                || history_format::is_transactions_file(path)
                || history_format::is_results_file(path)
                || history_format::is_scp_file(path)
            {
                return match xdr_verify::verify_and_write_xdr(path, reader, &self.dst_store).await {
                    Ok(result) => Ok(Some(result)),
                    Err(e) => {
                        if let Err(cleanup_err) = cleanup_partial_file(&self.dst_store, path).await
                        {
                            error!("Failed to cleanup partial file {}: {}", path, cleanup_err);
                        }
                        Err(e)
                    }
                };
            }
        }
        match self.dst_store.copy_from_reader(path, reader).await {
            Ok(()) => Ok(None),
            Err(e) => {
                if let Err(cleanup_err) = cleanup_partial_file(&self.dst_store, path).await {
                    error!("Failed to cleanup partial file {}: {}", path, cleanup_err);
                }
                Err(e)
            }
        }
    }

    /// Check whether dst already has a good copy of `path` and (when verify
    /// is on) record the parsed data into the manager.
    ///
    /// Returns:
    /// - `Ok(true)` — dst is acceptable; caller skips src fetch.
    /// - `Ok(false)` — dst is missing or corrupt; caller should fetch from src.
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
            return match self.dst_store.open_reader(path).await {
                Ok(reader) => Ok(crate::verify::verify_bucket_stream(path, reader)
                    .await
                    .is_ok()),
                Err(e) if e.class == ErrorClass::NotFound => Ok(false),
                Err(e) => Err(e),
            };
        }
        if history_format::is_scp_file(path) {
            return match self.dst_store.open_reader(path).await {
                Ok(reader) => Ok(xdr_verify::parse_scp_stream(path, reader).await.is_ok()),
                Err(e) if e.class == ErrorClass::NotFound => Ok(false),
                Err(e) => Err(e),
            };
        }

        // XDR file types: open, parse, and route recording through the shared
        // `record_result` helper inside the success arm.
        if history_format::is_ledger_header_file(path) {
            return match self.dst_store.open_reader(path).await {
                Ok(reader) => match xdr_verify::parse_ledger_header_stream(path, reader).await {
                    Ok(data) => {
                        record_result(manager, path, XdrParseResult::Ledger(data));
                        Ok(true)
                    }
                    Err(_) => Ok(false),
                },
                Err(e) if e.class == ErrorClass::NotFound => Ok(false),
                Err(e) => Err(e),
            };
        }
        if history_format::is_transactions_file(path) {
            return match self.dst_store.open_reader(path).await {
                Ok(reader) => match xdr_verify::parse_transactions_stream(path, reader).await {
                    Ok(hashes) => {
                        record_result(manager, path, XdrParseResult::Transactions(hashes));
                        Ok(true)
                    }
                    Err(_) => Ok(false),
                },
                Err(e) if e.class == ErrorClass::NotFound => Ok(false),
                Err(e) => Err(e),
            };
        }
        if history_format::is_results_file(path) {
            return match self.dst_store.open_reader(path).await {
                Ok(reader) => match xdr_verify::parse_results_stream(path, reader).await {
                    Ok(hashes) => {
                        record_result(manager, path, XdrParseResult::Results(hashes));
                        Ok(true)
                    }
                    Err(_) => Ok(false),
                },
                Err(e) if e.class == ErrorClass::NotFound => Ok(false),
                Err(e) => Err(e),
            };
        }

        Err(StorageError::fatal(format!(
            "verify_and_record_dst: unrecognized archive path '{path}' \
             (expected bucket / ledger / transactions / results / scp)"
        )))
    }

    /// Per-file retry — re-fetch individual files that the main pass failed
    /// to repair, on a **fresh `Pipeline<MirrorOperation>`**.
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
    /// The inner mirror is constructed with `overwrite=true` and
    /// `update_well_known=false`. We don't run the full pipeline (no
    /// `process_checkpoint`, no chain verification), only the per-item
    /// primitives. Stats are extracted via `into_stats` (bypassing mirror's
    /// `finalize` and its `has_failures` gate). Returns the inner's stats
    /// directly — no merging into the parent.
    pub(crate) async fn retry_failed_files(&self, parent_stats: &ArchiveStats) -> ArchiveStats {
        if self.dry_run {
            return ArchiveStats::new();
        }

        let work = build_failed_files_work_list(parent_stats).await;
        if work.is_empty() {
            return ArchiveStats::new();
        }

        info!("Retrying {} failed file(s)", work.len());

        let mirror_op = MirrorOperation::new(
            self.dst_store.clone(),
            /*overwrite=*/ true,
            None,
            None,
            /*allow_mirror_gaps=*/ true,
            &self.storage_config,
            /*update_well_known=*/ false,
        );
        let file_retry_pipeline = Pipeline::new(
            mirror_op,
            PipelineConfig {
                concurrency: self.concurrency,
                skip_optional: self.skip_optional,
                verify: self.verify,
                storage_config: self.storage_config.clone(),
            },
            self.src_store.clone(),
            Some(self.dst_store.clone()),
        );

        {
            let pref = &file_retry_pipeline;
            let cs = self.concurrency;
            stream::iter(work.into_iter())
                .for_each_concurrent(cs, |(cp, path)| async move {
                    if history_format::is_history_file(&path) {
                        pref.process_history_and_buckets(cp).await;
                    } else {
                        pref.process_file(cp, path).await;
                    }
                })
                .await;
        }

        file_retry_pipeline.into_stats()
    }

    /// Per-checkpoint retry — re-mirror whole checkpoints flagged by cross-
    /// file or cross-cp chain checks.
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

        let mirror_op = MirrorOperation::new(
            self.dst_store.clone(),
            /*overwrite=*/ true,
            None,
            None,
            /*allow_mirror_gaps=*/ true,
            &self.storage_config,
            /*update_well_known=*/ false,
        );
        let checkpoint_retry_pipeline = Pipeline::new(
            mirror_op,
            PipelineConfig {
                concurrency: self.concurrency,
                skip_optional: self.skip_optional,
                verify: self.verify,
                storage_config: self.storage_config.clone(),
            },
            self.src_store.clone(),
            Some(self.dst_store.clone()),
        );

        let _ = checkpoint_retry_pipeline.run_checkpoints(retry_cps).await;

        checkpoint_retry_pipeline.into_stats()
    }

    /// Repair .well-known by copying from highest checkpoint's history file.
    async fn repair_well_known(&self, highest_checkpoint: u32) {
        let history_path = history_format::checkpoint_path("history", highest_checkpoint);
        let well_known_path = history_format::ROOT_WELL_KNOWN_PATH;

        if let Some(base_path) = self.dst_store.get_base_path() {
            let src_file = base_path.join(&history_path);
            let dst_file = base_path.join(well_known_path);

            if !tokio::fs::try_exists(&src_file).await.unwrap_or(false) {
                error!(
                    "Cannot repair .well-known: history file at checkpoint {} not found",
                    highest_checkpoint
                );
                return;
            }

            if let Some(parent) = dst_file.parent() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    error!("Failed to create .well-known directory: {}", e);
                    return;
                }
            }

            match tokio::fs::copy(&src_file, &dst_file).await {
                Ok(_) => info!(
                    "Restored .well-known from checkpoint {} (0x{:08x})",
                    highest_checkpoint, highest_checkpoint
                ),
                Err(e) => error!("Failed to restore .well-known: {}", e),
            }
        }
    }
}

// ============================================================================
// Repair helpers (free functions)
// ============================================================================

/// Record a parsed XDR result into the verification manager. Used for both
/// dst-side parses (via `verify_and_record_dst`) and src-side fetches (via
/// `process_object` after `fetch_from_src`). No-op when the path doesn't
/// carry a checkpoint number.
fn record_result(manager: &XdrVerificationManager, path: &str, result: XdrParseResult) {
    let Some(cp) = history_format::checkpoint_from_path(path) else {
        return;
    };
    match result {
        XdrParseResult::Ledger(data) => manager.record_header_data(cp, data),
        XdrParseResult::Transactions(hashes) => manager.record_tx_set_hashes(cp, hashes),
        XdrParseResult::Results(hashes) => manager.record_result_hashes(cp, hashes),
        XdrParseResult::None => {}
    }
}

/// Build a `(cp, path)` work list from the current state of the failure
/// tracker. Read-only on `parent_stats` — no `mem::take`, no mutation. The
/// work list captures every uniquely-identifiable failed file at the moment
/// of the snapshot; concurrent writers (there shouldn't be any at this point,
/// but the lock is taken regardless) are blocked only for the duration of
/// the read.
async fn build_failed_files_work_list(parent_stats: &ArchiveStats) -> Vec<(u32, String)> {
    let f = parent_stats.failures.lock().await;
    let mut work = Vec::new();

    for (&cp, flags) in &f.files {
        for (bit, prefix) in [
            (utils::FileFlags::HISTORY, "history"),
            (utils::FileFlags::LEDGER, "ledger"),
            (utils::FileFlags::TRANSACTIONS, "transactions"),
            (utils::FileFlags::RESULTS, "results"),
            (utils::FileFlags::SCP, "scp"),
        ] {
            if flags.has(bit) {
                work.push((cp, history_format::checkpoint_path(prefix, cp)));
            }
        }
    }

    for hash in &f.buckets {
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
    async fn get_checkpoint_bounds(
        &self,
        source: &StorageRef,
    ) -> Result<(u32, u32), pipeline::Error> {
        // Try destination .well-known first (determines what range to repair)
        let dst_result = utils::fetch_well_known_history_file(
            &self.dst_store,
            0, // no retries for local filesystem
            self.storage_config.retry_min_delay.as_millis() as u64,
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
                    source,
                    self.storage_config.max_retries as u32,
                    self.storage_config.retry_min_delay.as_millis() as u64,
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
    /// missing/corrupt fetch from src (verify-and-write + record). All
    /// verification logic lives here, matching scan/mirror's pattern.
    async fn process_object(
        &self,
        path: &str,
        _src_store: &StorageRef,
        manager: Option<&XdrVerificationManager>,
    ) -> Result<ProcessOutcome, StorageError> {
        if self.verify_and_record_dst(path, manager).await? {
            return Ok(ProcessOutcome::Processed);
        }
        if self.dry_run {
            self.note_dry_run(path);
            return Ok(ProcessOutcome::Processed);
        }
        let result = self.fetch_from_src(path, manager).await?;
        if let (Some(parsed), Some(manager)) = (result, manager) {
            record_result(manager, path, parsed);
        }
        Ok(ProcessOutcome::Processed)
    }

    /// Override history buffer sourcing: read from destination first, fall back to source.
    async fn fetch_history_buffer(
        &self,
        history_path: &str,
        src_store: &StorageRef,
        dst_store: Option<&StorageRef>,
    ) -> Result<Buffer, StorageError> {
        // Try reading from destination (local, cheap)
        if let Some(dst) = dst_store {
            if let Ok(buffer) = storage::download_buffer(dst, history_path).await {
                if history_format::parse_history(&buffer, history_path).is_ok() {
                    return Ok(buffer);
                }
                // Exists but corrupt — fall through to source
            }
        }

        // Destination missing or corrupt — need from source
        if self.dry_run {
            self.needs_repair_count.fetch_add(1, Ordering::Relaxed);
            debug!("Dry run: would repair {}", history_path);
        }

        // Download from source (pipeline will write to dst via process_buffer)
        storage::download_buffer(src_store, history_path).await
    }

    async fn finalize(
        &self,
        highest_checkpoint: u32,
        stats: &ArchiveStats,
    ) -> Result<(), pipeline::Error> {
        // Manager drain (verify_checkpoint_chain + record_all_errors) already
        // happened in `Pipeline::run_checkpoints`, so `stats.failures.checkpoints`
        // is already populated for phase 2 to consume.

        if self.dry_run {
            let count = self.needs_repair_count.load(Ordering::Relaxed);
            info!("Dry run: {} file(s) would be repaired", count);
            return Ok(());
        }

        // .well-known restoration. Separate path from phases (a follow-up
        // may fold it into phase 1's per-path repair flow).
        if self.well_known_needs_repair.load(Ordering::Relaxed) {
            self.repair_well_known(highest_checkpoint).await;
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

        // Overall success = both retries recovered everything they attempted.
        // The parent's failures are expected — those are what we just tried
        // to fix; the retries' own failure trackers are the ground truth for
        // "what's still broken."
        let file_retry_failed = file_retry_stats.has_failures().await;
        let checkpoint_retry_failed = checkpoint_retry_stats.has_failures().await;
        if file_retry_failed || checkpoint_retry_failed {
            return Err(Error::RepairFailed.into());
        }

        Ok(())
    }

    /// Repair writes the history buffer to its own dst. In dry-run mode the
    /// write is skipped but the file is still reported as `Processed` (the
    /// pipeline records it as a success — matching how repair's `process_object`
    /// reports its dry-run no-op branches).
    async fn process_buffer(
        &self,
        path: &str,
        buffer: Buffer,
    ) -> Result<ProcessOutcome, StorageError> {
        if self.dry_run {
            return Ok(ProcessOutcome::Processed);
        }
        storage::write_buffer_with_cleanup(&self.dst_store, path, buffer).await?;
        Ok(ProcessOutcome::Processed)
    }
}

// ============================================================================
// Manual-mode helpers
// ============================================================================

/// Log what would be repaired (for manual mode dry-run).
fn report_dry_run(files: &[String]) {
    info!("Dry run: {} file(s) would be repaired:", files.len());

    let mut counts = [0u64; 7];
    for path in files {
        let idx = if path.starts_with("history/") || path.contains("stellar-history.json") {
            0
        } else if path.starts_with("ledger/") {
            1
        } else if path.starts_with("transactions/") {
            2
        } else if path.starts_with("results/") {
            3
        } else if path.starts_with("bucket/") {
            4
        } else if path.starts_with("scp/") {
            5
        } else {
            6
        };
        counts[idx] += 1;
    }

    let labels = [
        "history",
        "ledger",
        "transactions",
        "results",
        "bucket",
        "scp",
        "other",
    ];
    for (count, label) in counts.iter().zip(labels.iter()) {
        if *count > 0 {
            info!("  {} {} file(s)", count, label);
        }
    }
}

/// Validate a JSON file list for manual repair mode.
///
/// Each entry must be a recognized archive path — `.well-known/stellar-history.json`
/// or one of the per-category checkpoint/bucket paths under `history/`,
/// `ledger/`, `transactions/`, `results/`, `scp/`, or `bucket/`. This prevents
/// path traversal: manual-mode paths flow into `dst_store.copy_from_reader`,
/// which on filesystem-backed destinations joins onto the archive root, so
/// arbitrary input like `../../etc/foo` or `/absolute/path` would otherwise
/// allow writes outside the archive.
///
/// Defense-in-depth: also reject any path containing `..` segments, a leading
/// `/`, or a backslash, even if the per-category predicate were to drift in
/// the future. Empty strings are skipped (legacy behavior). Duplicates are
/// deduped.
pub fn validate_file_list(file_list: &[String]) -> Result<Vec<String>, Error> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for path in file_list {
        if path.is_empty() {
            continue;
        }

        // Path-traversal guards. Reject anything that looks like an absolute
        // path, a Windows path separator, or contains a `..` segment.
        if path.starts_with('/') || path.contains('\\') {
            return Err(Error::InvalidFileList(format!(
                "path '{path}' must be a relative archive path \
                 (no leading '/', no '\\\\' separators)"
            )));
        }
        if path.split('/').any(|seg| seg == "..") {
            return Err(Error::InvalidFileList(format!(
                "path '{path}' contains '..' segment; archive paths must not traverse"
            )));
        }

        // Must be a recognized archive path shape.
        let recognized = path == history_format::ROOT_WELL_KNOWN_PATH
            || history_format::is_history_file(path)
            || history_format::is_ledger_header_file(path)
            || history_format::is_transactions_file(path)
            || history_format::is_results_file(path)
            || history_format::is_scp_file(path)
            || history_format::is_bucket_file(path);
        if !recognized {
            return Err(Error::InvalidFileList(format!(
                "path '{path}' is not a recognized archive path \
                 (expected .well-known/stellar-history.json or a file under \
                 history/ ledger/ transactions/ results/ scp/ bucket/)"
            )));
        }

        if seen.insert(path.clone()) {
            result.push(path.clone());
        }
    }
    Ok(result)
}

/// Parse a JSON file containing an array of file paths for manual repair.
pub fn parse_file_list_json(content: &str) -> Result<Vec<String>, Error> {
    let parsed: serde_json::Value = serde_json::from_str(content)
        .map_err(|e| Error::InvalidFileList(format!("Invalid JSON: {e}")))?;

    let array = parsed
        .as_array()
        .ok_or_else(|| Error::InvalidFileList("Expected a JSON array of file paths".to_string()))?;

    let mut paths = Vec::with_capacity(array.len());
    for (i, item) in array.iter().enumerate() {
        let path = item.as_str().ok_or_else(|| {
            Error::InvalidFileList(format!("Element at index {i} is not a string"))
        })?;
        paths.push(path.to_string());
    }

    validate_file_list(&paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_file_list_valid() {
        let list = vec![
            "ledger/00/00/3f/ledger-0000003f.xdr.gz".to_string(),
            "bucket/ab/cd/ef/bucket-abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.xdr.gz".to_string(),
        ];
        let result = validate_file_list(&list).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_validate_file_list_empty() {
        let list: Vec<String> = vec![];
        let result = validate_file_list(&list).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_validate_file_list_deduplicates() {
        let list = vec![
            "ledger/00/00/3f/ledger-0000003f.xdr.gz".to_string(),
            "ledger/00/00/3f/ledger-0000003f.xdr.gz".to_string(),
        ];
        let result = validate_file_list(&list).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_validate_file_list_skips_empty_strings() {
        let list = vec![
            String::new(),
            "ledger/00/00/3f/ledger-0000003f.xdr.gz".to_string(),
        ];
        let result = validate_file_list(&list).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_validate_file_list_accepts_all_recognized_kinds() {
        let list = vec![
            ".well-known/stellar-history.json".to_string(),
            "history/00/00/3f/history-0000003f.json".to_string(),
            "ledger/00/00/3f/ledger-0000003f.xdr.gz".to_string(),
            "transactions/00/00/3f/transactions-0000003f.xdr.gz".to_string(),
            "results/00/00/3f/results-0000003f.xdr.gz".to_string(),
            "scp/00/00/3f/scp-0000003f.xdr.gz".to_string(),
            "bucket/ab/cd/ef/bucket-abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.xdr.gz".to_string(),
        ];
        let result = validate_file_list(&list).unwrap();
        assert_eq!(result.len(), 7);
    }

    #[test]
    fn test_validate_file_list_rejects_path_traversal() {
        let list = vec!["ledger/../etc/passwd".to_string()];
        let err = validate_file_list(&list).unwrap_err().to_string();
        assert!(err.contains(".."), "got: {err}");
    }

    #[test]
    fn test_validate_file_list_rejects_path_traversal_at_start() {
        let list = vec!["../escape.txt".to_string()];
        let err = validate_file_list(&list).unwrap_err().to_string();
        assert!(err.contains(".."), "got: {err}");
    }

    #[test]
    fn test_validate_file_list_rejects_absolute_path() {
        let list = vec!["/etc/passwd".to_string()];
        let err = validate_file_list(&list).unwrap_err().to_string();
        assert!(err.contains("relative"), "got: {err}");
    }

    #[test]
    fn test_validate_file_list_rejects_backslash() {
        let list = vec!["ledger\\foo.xdr.gz".to_string()];
        let err = validate_file_list(&list).unwrap_err().to_string();
        assert!(err.contains("relative"), "got: {err}");
    }

    #[test]
    fn test_validate_file_list_rejects_unknown_prefix() {
        let list = vec!["garbage/foo.xdr.gz".to_string()];
        let err = validate_file_list(&list).unwrap_err().to_string();
        assert!(err.contains("not a recognized"), "got: {err}");
    }

    #[test]
    fn test_validate_file_list_rejects_bucket_with_bad_hash() {
        // Right prefix, but the hash isn't 64 hex chars → is_bucket_file returns false.
        let list = vec!["bucket/ab/cd/ef/bucket-shorthash.xdr.gz".to_string()];
        let err = validate_file_list(&list).unwrap_err().to_string();
        assert!(err.contains("not a recognized"), "got: {err}");
    }

    #[test]
    fn test_parse_file_list_json_valid() {
        let json = r#"["ledger/00/00/3f/ledger-0000003f.xdr.gz", "bucket/ab/cd/ef/bucket-abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.xdr.gz"]"#;
        let result = parse_file_list_json(json).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_parse_file_list_json_empty_array() {
        let json = "[]";
        let result = parse_file_list_json(json).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_file_list_json_invalid_json() {
        let json = "not json {{";
        let result = parse_file_list_json(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid JSON"), "got: {err}");
    }

    #[test]
    fn test_parse_file_list_json_non_array() {
        let json = r#"{"key": "value"}"#;
        let result = parse_file_list_json(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("JSON array"), "got: {err}");
    }

    #[test]
    fn test_parse_file_list_json_non_string_elements() {
        let json = "[1, 2, 3]";
        let result = parse_file_list_json(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not a string"), "got: {err}");
    }

    #[test]
    fn test_parse_file_list_json_deduplicates() {
        let json = r#"["ledger/00/00/3f/ledger-0000003f.xdr.gz", "ledger/00/00/3f/ledger-0000003f.xdr.gz"]"#;
        let result = parse_file_list_json(json).unwrap();
        assert_eq!(result.len(), 1);
    }
}
