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
use crate::pipeline::{self, async_trait, Operation, Pipeline, PipelineConfig};
use crate::storage::{
    self, cleanup_partial_file, Error as StorageError, StorageConfig, StorageRef,
};
use crate::utils::{self, ArchiveStats};
use crate::xdr_verify::{self, XdrParseResult, XdrVerificationManager};
use futures_util::{stream, StreamExt};
use opendal::Buffer;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    verify: bool,
    dry_run: bool,
    concurrency: usize,
    skip_optional: bool,
    storage_config: StorageConfig,
    /// Verification manager — populated only when `verify` is on. Records
    /// per-file XDR data (from dst-first parse or src-fetched verify) so that
    /// `finalize_checkpoint` can run cross-file checks and `finalize` can run
    /// the cross-checkpoint chain check.
    verification_manager: Option<Arc<XdrVerificationManager>>,
    /// Set during get_checkpoint_bounds if dest .well-known needs repair
    well_known_needs_repair: AtomicBool,
    /// Dry-run counter: files that would be repaired
    needs_repair_count: AtomicU64,
    /// Checkpoints whose cross-file or chain checks failed; re-mirrored from
    /// src in `finalize`.
    retry_checkpoints: Mutex<BTreeSet<u32>>,
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
            verification_manager: if verify {
                Some(Arc::new(XdrVerificationManager::new()))
            } else {
                None
            },
            well_known_needs_repair: AtomicBool::new(false),
            needs_repair_count: AtomicU64::new(0),
            retry_checkpoints: Mutex::new(BTreeSet::new()),
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
                        || async { self.fetch_from_src(path).await.map(|_| ()) },
                    )
                    .await;
                    match result {
                        Ok(()) => {
                            debug!("Repaired: {}", path);
                            stats.record_success(path);
                        }
                        Err(e) => {
                            error!("Failed to repair {}: {}", path, e);
                            stats.record_failure(path).await;
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
        if stats.has_failures() {
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
    async fn fetch_from_src(&self, path: &str) -> Result<Option<XdrParseResult>, StorageError> {
        let reader = self.src_store.open_reader(path).await?;
        if self.verify {
            if history_format::is_bucket_file(path) {
                return match crate::verify::verify_and_write_bucket(path, reader, &self.dst_store)
                    .await
                {
                    Ok(()) => Ok(None),
                    Err(e) => {
                        cleanup_partial_file(&self.dst_store, path).await?;
                        Err(e)
                    }
                };
            }
            if history_format::is_ledger_file(path)
                || history_format::is_transactions_file(path)
                || history_format::is_results_file(path)
                || history_format::is_scp_file(path)
            {
                return match xdr_verify::verify_and_write_xdr(path, reader, &self.dst_store).await {
                    Ok(result) => Ok(Some(result)),
                    Err(e) => {
                        cleanup_partial_file(&self.dst_store, path).await?;
                        Err(e)
                    }
                };
            }
        }
        match self.dst_store.copy_from_reader(path, reader).await {
            Ok(()) => Ok(None),
            Err(e) => {
                cleanup_partial_file(&self.dst_store, path).await?;
                Err(e)
            }
        }
    }

    /// Check whether dst already has a good copy of `path` and (when `--verify`
    /// is on) record the parsed data into the manager.
    ///
    /// Returns `Ok(true)` if dst is acceptable (caller should skip src fetch),
    /// `Ok(false)` on missing or corrupt dst (caller should fetch from src),
    /// or `Err` if `path` doesn't match any recognized archive file type
    /// (a contract violation from the pipeline's dispatch).
    ///
    /// When `--verify` is off, only existence is checked — content is not
    /// validated and nothing is recorded into the manager.
    async fn verify_and_record_dst(&self, path: &str) -> Result<bool, StorageError> {
        if !self.verify {
            return Ok(self.dst_store.exists(path).await.unwrap_or(false));
        }

        // Buckets and SCP files have no per-checkpoint manager state — verify
        // structurally on dst, no recording.
        if history_format::is_bucket_file(path) {
            let Ok(reader) = self.dst_store.open_reader(path).await else {
                return Ok(false);
            };
            return Ok(crate::verify::verify_bucket_stream(path, reader)
                .await
                .is_ok());
        }
        if history_format::is_scp_file(path) {
            let Ok(reader) = self.dst_store.open_reader(path).await else {
                return Ok(false);
            };
            return Ok(xdr_verify::parse_scp_stream(path, reader).await.is_ok());
        }

        // XDR file types: parse into a uniform `XdrParseResult` and route
        // recording through the shared `record_result` helper.
        let parsed = if history_format::is_ledger_file(path) {
            let Ok(reader) = self.dst_store.open_reader(path).await else {
                return Ok(false);
            };
            match xdr_verify::parse_ledger_stream(path, reader).await {
                Ok(data) => XdrParseResult::Ledger(data),
                Err(_) => return Ok(false),
            }
        } else if history_format::is_transactions_file(path) {
            let Ok(reader) = self.dst_store.open_reader(path).await else {
                return Ok(false);
            };
            match xdr_verify::parse_transactions_stream(path, reader).await {
                Ok(hashes) => XdrParseResult::Transactions(hashes),
                Err(_) => return Ok(false),
            }
        } else if history_format::is_results_file(path) {
            let Ok(reader) = self.dst_store.open_reader(path).await else {
                return Ok(false);
            };
            match xdr_verify::parse_results_stream(path, reader).await {
                Ok(hashes) => XdrParseResult::Results(hashes),
                Err(_) => return Ok(false),
            }
        } else {
            return Err(StorageError::fatal(format!(
                "verify_and_record_dst: unrecognized archive path '{path}' \
                 (expected bucket / ledger / transactions / results / scp)"
            )));
        };

        self.record_result(path, parsed);
        Ok(true)
    }

    /// Record a parsed XDR result into the verification manager. Used for both
    /// dst-side parses (via `verify_and_record_dst`) and src-side fetches (via
    /// `process_object` after `fetch_from_src`). No-op when `--verify` is off
    /// (manager is `None`) or the path doesn't carry a checkpoint number.
    fn record_result(&self, path: &str, result: XdrParseResult) {
        let Some(ref manager) = self.verification_manager else {
            return;
        };
        let Some(cp) = history_format::checkpoint_from_path(path) else {
            return;
        };
        match result {
            XdrParseResult::Ledger(data) => manager.record_ledger_data(cp, data),
            XdrParseResult::Transactions(hashes) => manager.record_tx_set_hashes(cp, hashes),
            XdrParseResult::Results(hashes) => manager.record_result_hashes(cp, hashes),
            XdrParseResult::None => {}
        }
    }

    /// Re-mirror a list of checkpoints from src using `Pipeline<MirrorOperation>`
    /// with overwrite semantics (`overwrite=true`, `verify=false`). Trust src
    /// is canonical — no residual verification. Retry checkpoints are
    /// processed concurrently up to `self.concurrency`.
    async fn run_retry(&self, retry: &[u32]) {
        info!(
            "Retrying {} checkpoint(s) from src (cross-file or chain inconsistency)",
            retry.len()
        );
        let mirror_op = MirrorOperation::new(
            self.dst_store.clone(),
            /*overwrite=*/ true,
            None,
            None,
            /*allow_mirror_gaps=*/ true,
            &self.storage_config,
            /*verify=*/ false,
        );
        let retry_pipeline = Pipeline::new(
            mirror_op,
            PipelineConfig {
                concurrency: self.concurrency,
                skip_optional: self.skip_optional,
                storage_config: self.storage_config.clone(),
            },
            self.src_store.clone(),
            Some(self.dst_store.clone()),
        );
        let pipeline_ref = &retry_pipeline;
        stream::iter(retry.iter().copied())
            .for_each_concurrent(self.concurrency, |cp| async move {
                pipeline_ref.process_checkpoint(cp).await;
            })
            .await;
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
    ) -> Result<(), StorageError> {
        if self.verify_and_record_dst(path).await? {
            return Ok(());
        }
        if self.dry_run {
            self.note_dry_run(path);
            return Ok(());
        }
        let result = self.fetch_from_src(path).await?;
        if let Some(parsed) = result {
            self.record_result(path, parsed);
        }
        Ok(())
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

    fn finalize_checkpoint(&self, checkpoint: u32) {
        if let Some(ref manager) = self.verification_manager {
            let errors = manager.verify_and_release(checkpoint);
            if !errors.is_empty() {
                self.retry_checkpoints.lock().unwrap().insert(checkpoint);
            }
        }
    }

    async fn finalize(
        &self,
        highest_checkpoint: u32,
        stats: &ArchiveStats,
    ) -> Result<(), pipeline::Error> {
        // Step 1: cross-checkpoint chain check; both ends of any break go to retry.
        if let Some(ref manager) = self.verification_manager {
            let chain_errors = manager.verify_checkpoint_chain();
            let mut retry = self.retry_checkpoints.lock().unwrap();
            for err in chain_errors {
                retry.insert(err.checkpoint);
                if err.checkpoint >= history_format::CHECKPOINT_FREQUENCY {
                    retry.insert(err.checkpoint - history_format::CHECKPOINT_FREQUENCY);
                }
            }
        }

        let retry: Vec<u32> = std::mem::take(&mut *self.retry_checkpoints.lock().unwrap())
            .into_iter()
            .collect();

        // Step 2: re-mirror retry checkpoints from src using Pipeline<MirrorOperation>.
        if !retry.is_empty() && !self.dry_run {
            self.run_retry(&retry).await;
        }

        // Step 3: .well-known repair (after retry, so it reflects fixed data).
        if self.well_known_needs_repair.load(Ordering::Relaxed) && !self.dry_run {
            self.repair_well_known(highest_checkpoint).await;
        }

        // Step 4: report results.
        if self.dry_run {
            let count = self.needs_repair_count.load(Ordering::Relaxed);
            info!(
                "Dry run: {} file(s) would be repaired ({} checkpoint(s) would retry)",
                count,
                retry.len()
            );
        } else {
            stats.report("repair").await;
            if stats.has_failures() {
                return Err(Error::RepairFailed.into());
            }
        }

        Ok(())
    }

    /// Repair writes the history buffer to its own dst, except in dry-run mode
    /// where the write is skipped entirely (and stats are left untouched —
    /// dry-run reports via `needs_repair_count`, populated in
    /// `fetch_history_buffer`).
    async fn process_buffer(&self, path: &str, buffer: Buffer, stats: &ArchiveStats) {
        if self.dry_run {
            return;
        }
        match storage::write_buffer_with_cleanup(&self.dst_store, path, buffer).await {
            Ok(()) => stats.record_success(path),
            Err(e) => {
                error!("Failed to write history file {}: {}", path, e);
                stats.record_failure(path).await;
            }
        }
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
pub fn validate_file_list(file_list: &[String]) -> Result<Vec<String>, Error> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for path in file_list {
        if path.is_empty() {
            continue;
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
