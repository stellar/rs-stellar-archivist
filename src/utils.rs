use bytes::Buf;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use stellar_xdr::curr::Hash;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use crate::history_format::{self, HistoryFileState};
use crate::pipeline;
use crate::storage::StorageRef;
use crate::xdr_verify::VerificationErrorType;

//=============================================================================
// Retryable HTTP Error Codes
//=============================================================================

/// Standard HTTP server errors that are typically retried (500, 502, 503, 504).
pub const STANDARD_RETRYABLE_HTTP_ERRORS: &[(u16, &str)] = &[
    (500, "Internal Server Error"),
    (502, "Bad Gateway"),
    (503, "Service Unavailable"),
    (504, "Gateway Timeout"),
];

/// Non-standard HTTP status codes that should be treated as retryable.
///
/// References:
/// - Cloudflare: <https://developers.cloudflare.com/support/troubleshooting/cloudflare-errors/troubleshooting-cloudflare-5xx-errors>/
/// - Unofficial codes: <https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#Unofficial_codes>
pub const NON_STANDARD_RETRYABLE_HTTP_ERRORS: &[(u16, &str)] = &[
    // Standard retryable errors that some clients don't retry by default
    (408, "Request Timeout"),
    (429, "Too Many Requests"),
    // Cloudflare-specific errors
    (520, "Cloudflare Unknown Error"),
    (521, "Cloudflare Web Server Is Down"),
    (522, "Cloudflare Connection Timed Out"),
    (523, "Cloudflare Origin Is Unreachable"),
    (524, "Cloudflare A Timeout Occurred"),
    (530, "Cloudflare Origin DNS Error"),
    // Informal/proxy-specific errors
    (509, "Bandwidth Limit Exceeded"), // Apache/cPanel
    (529, "Site is Overloaded"),       // Qualys SSLLabs
    (598, "Network Read Timeout"),     // Informal, nginx
    (599, "Network Connect Timeout"),  // Informal, nginx
];

//=============================================================================
// Error Types
//=============================================================================

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
#[must_use]
pub fn map_pipeline_error(err: pipeline::Error) -> crate::Error {
    match err {
        pipeline::Error::ScanOperation(scan_err) => crate::Error::ScanOperation(scan_err),
        pipeline::Error::MirrorOperation(mirror_err) => crate::Error::MirrorOperation(mirror_err),
        pipeline::Error::RepairOperation(repair_err) => crate::Error::RepairOperation(repair_err),
        pipeline::Error::Io(io_err) => crate::Error::Io(io_err),
        other => crate::Error::Other(other.to_string()),
    }
}

//=============================================================================
// FailureTracker — the deterministic source of truth for archive failures.
//
// Four orthogonal failure kinds, each indexable by its natural key:
//   - well_known (singleton .well-known/stellar-history.json)
//   - files      (per-checkpoint standard files: history/ledger/tx/results/scp)
//   - buckets    (content-addressed; the same bucket may be referenced by
//                 many checkpoints, but a failure is identified by its hash)
//   - checkpoints (any checkpoint-level failure: cross-file verification,
//                  within-cp chain break, or cross-cp chain break — for the
//                  last, both cps flanking the break are inserted)
//
// Per-checkpoint "file" failures use a bitmask so a single cp can have
// multiple file-type failures without duplicating the cp key.
//=============================================================================

/// Bitmask of per-checkpoint standard file types. Bit set = that file is
/// missing or failed validation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileFlags(u8);

impl FileFlags {
    pub const HISTORY: u8 = 1 << 0;
    pub const LEDGER: u8 = 1 << 1;
    pub const TRANSACTIONS: u8 = 1 << 2;
    pub const RESULTS: u8 = 1 << 3;
    pub const SCP: u8 = 1 << 4;

    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }
    pub fn set(&mut self, flag: u8) {
        self.0 |= flag;
    }
    pub fn unset(&mut self, flag: u8) {
        self.0 &= !flag;
    }
    #[must_use]
    pub fn has(&self, flag: u8) -> bool {
        (self.0 & flag) != 0
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
    #[must_use]
    pub fn count(&self) -> u32 {
        self.0.count_ones()
    }
}

// detailed tracking of which files are broken. and which checkpoints are broken
#[derive(Debug, Default, Clone)]
pub struct FailureTracker {
    /// `Some(cp)` when the root `.well-known/stellar-history.json` is broken,
    /// carrying the checkpoint it should be restored from (the archive's
    /// highest checkpoint). `None` when healthy.
    pub well_known: Option<u32>,
    pub files: BTreeMap<u32, FileFlags>,
    pub buckets: BTreeSet<Hash>,
    pub checkpoints: BTreeSet<u32>,
}

impl FailureTracker {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.well_known.is_none()
            && self.files.is_empty()
            && self.buckets.is_empty()
            && self.checkpoints.is_empty()
    }

    /// True if `path` identifies a file already recorded as broken: a bucket
    /// whose content hash is in `buckets`, or a per-checkpoint file whose
    /// (cp, type) flag is set in `files`. Repair's failed-list mode uses this
    /// to fetch known-broken files directly instead of probing the destination.
    #[must_use]
    pub fn contains_path(&self, path: &str) -> bool {
        if let Some(hash_str) = history_format::bucket_hash_from_path(path) {
            return hex_to_hash(&hash_str).is_some_and(|h| self.buckets.contains(&h));
        }
        match (
            history_format::checkpoint_from_path(path),
            path_to_file_flag(path),
        ) {
            (Some(cp), Some(flag)) => self.files.get(&cp).is_some_and(|f| f.has(flag)),
            _ => false,
        }
    }

    /// Mark the root `.well-known` as needing restoration from checkpoint `cp`
    /// (the archive's highest checkpoint, which is what `.well-known` advertises).
    pub fn record_well_known(&mut self, cp: u32) {
        self.well_known = Some(cp);
    }
    pub fn unrecord_well_known(&mut self) {
        self.well_known = None;
    }

    pub fn record_file(&mut self, cp: u32, flag: u8) {
        self.files.entry(cp).or_default().set(flag);
    }
    pub fn unrecord_file(&mut self, cp: u32, flag: u8) {
        if let Some(flags) = self.files.get_mut(&cp) {
            flags.unset(flag);
            if flags.is_empty() {
                self.files.remove(&cp);
            }
        }
    }

    pub fn record_bucket(&mut self, hash: Hash) {
        self.buckets.insert(hash);
    }
    pub fn unrecord_bucket(&mut self, hash: &Hash) {
        self.buckets.remove(hash);
    }

    pub fn record_checkpoint(&mut self, cp: u32) {
        self.checkpoints.insert(cp);
    }
    pub fn unrecord_checkpoint(&mut self, cp: u32) {
        self.checkpoints.remove(&cp);
    }

    /// Number of checkpoints with the given file flag set.
    #[must_use]
    pub fn count_files(&self, flag: u8) -> u32 {
        self.files.values().filter(|f| f.has(flag)).count() as u32
    }

    /// Total file-failure count across all checkpoints and all flags.
    #[must_use]
    pub fn total_file_failures(&self) -> u32 {
        self.files.values().map(FileFlags::count).sum::<u32>()
    }

    /// Record a verification failure, dispatching on its kind:
    /// - `Ledger(seq)` and `Checkpoint(cp)` mark a single checkpoint as
    ///   problematic.
    /// - `Boundary(cp)` marks both `cp` and its previous checkpoint, since
    ///   a chain break implicates both ends.
    pub fn record_verification_failure(&mut self, kind: &VerificationErrorType) {
        match kind {
            VerificationErrorType::Ledger(seq) => {
                self.record_checkpoint(history_format::round_to_upper_checkpoint(*seq));
            }
            VerificationErrorType::Checkpoint(cp) => {
                self.record_checkpoint(*cp);
            }
            VerificationErrorType::Boundary(cp) => {
                self.record_checkpoint(*cp);
                if let Some(prev) = cp.checked_sub(history_format::CHECKPOINT_FREQUENCY) {
                    self.record_checkpoint(prev);
                }
            }
        }
    }
}

/// Classify a path into a `FileFlags` constant, or `None` if not a recognized
/// per-checkpoint standard-file path.
fn path_to_file_flag(path: &str) -> Option<u8> {
    if history_format::is_history_file(path) {
        Some(FileFlags::HISTORY)
    } else if history_format::is_ledger_header_file(path) {
        Some(FileFlags::LEDGER)
    } else if history_format::is_transactions_file(path) {
        Some(FileFlags::TRANSACTIONS)
    } else if history_format::is_results_file(path) {
        Some(FileFlags::RESULTS)
    } else if history_format::is_scp_file(path) {
        Some(FileFlags::SCP)
    } else {
        None
    }
}

/// Parse a hex bucket hash (as returned by `bucket_hash_from_path`) into a `Hash`.
pub(crate) fn hex_to_hash(s: &str) -> Option<Hash> {
    let bytes = hex::decode(s).ok()?;
    let arr: [u8; 32] = bytes.try_into().ok()?;
    Some(Hash(arr))
}

/// Shared archive operation statistics. The `failures` field is the
/// deterministic source of truth for the operation's outcome and may be
/// consulted to drive subsequent decisions (e.g. repair retry planning).
pub struct ArchiveStats {
    pub successful_files: AtomicU64,
    pub skipped_files: AtomicU64,
    pub retry_count: AtomicU64,
    pub failures: tokio::sync::Mutex<FailureTracker>,
}

impl Default for ArchiveStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveStats {
    #[must_use]
    pub fn new() -> Self {
        Self {
            successful_files: AtomicU64::new(0),
            skipped_files: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            failures: tokio::sync::Mutex::new(FailureTracker::default()),
        }
    }

    /// Snapshot this stats into a report section (locks failures once + reads
    /// its counters). The single "stats → section" primitive: operations
    /// assemble these into a single- or multi-section report and write it.
    /// Logging is separate ([`report`](Self::report)).
    pub async fn report_section(&self) -> crate::report::ReportSection {
        let failures = self.failures.lock().await;
        crate::report::section(
            &failures,
            self.successful_files.load(Ordering::Relaxed),
            self.skipped_files.load(Ordering::Relaxed),
            self.retry_count.load(Ordering::Relaxed),
        )
    }

    pub fn record_success(&self, _path: &str) {
        self.successful_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a skipped file (already exists in mirror mode).
    pub fn record_skipped(&self, _path: &str) {
        self.skipped_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a retry attempt.
    pub fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a per-file failure, routing by path to the appropriate
    /// `FailureTracker` slot. `cp` is the checkpoint context (used as the key
    /// for per-checkpoint standard files; ignored for buckets). The root
    /// `.well-known` is not a per-file failure — it is recorded directly via
    /// [`FailureTracker::record_well_known`] with the archive's high checkpoint.
    /// Unrecognized paths are logged and not recorded.
    pub async fn record_failure(&self, cp: u32, path: &str) {
        let mut failures = self.failures.lock().await;
        if let Some(hash_str) = history_format::bucket_hash_from_path(path) {
            if let Some(hash) = hex_to_hash(&hash_str) {
                failures.record_bucket(hash);
            } else {
                warn!("record_failure: malformed bucket hash in path {path}");
            }
        } else if let Some(flag) = path_to_file_flag(path) {
            failures.record_file(cp, flag);
        } else {
            warn!("record_failure: unrecognized path {path}");
        }
    }

    /// Remove a previously recorded per-file failure (e.g. when retry rescues
    /// the file). No-op if the path was not present in the tracker.
    pub async fn unrecord_failure(&self, cp: u32, path: &str) {
        let mut failures = self.failures.lock().await;
        if let Some(hash_str) = history_format::bucket_hash_from_path(path) {
            if let Some(hash) = hex_to_hash(&hash_str) {
                failures.unrecord_bucket(&hash);
            }
        } else if let Some(flag) = path_to_file_flag(path) {
            failures.unrecord_file(cp, flag);
        }
    }

    /// True iff the tracker holds any failure of any kind.
    pub async fn has_failures(&self) -> bool {
        !self.failures.lock().await.is_empty()
    }

    /// Number of checkpoints flagged by cross-file or cross-checkpoint
    /// verification. These are detected *after* the per-cp files are written, so
    /// a non-zero count means individually-valid files were committed but are
    /// mutually inconsistent.
    pub async fn checkpoint_failure_count(&self) -> usize {
        self.failures.lock().await.checkpoints.len()
    }

    /// Generate and log a complete report of the operation results.
    pub async fn report(&self, operation: &str) {
        let successful = self.successful_files.load(Ordering::Relaxed);
        let skipped = self.skipped_files.load(Ordering::Relaxed);
        let retries = self.retry_count.load(Ordering::Relaxed);

        let failures = self.failures.lock().await;
        let total_file_failures = failures.total_file_failures();
        let total_bucket_failures = failures.buckets.len() as u32;
        let checkpoint_failures = failures.checkpoints.len() as u32;
        let total_failures = total_file_failures + total_bucket_failures;

        match operation {
            "mirror" => info!(
                "Mirror completed: {successful} files copied, {total_failures} failed, {skipped} skipped"
            ),
            "repair" => info!(
                "Repair completed: {successful} files processed, {total_failures} failed"
            ),
            _ => info!("Scan complete: {successful} files found, {total_failures} missing"),
        }

        debug!(
            "Stats: {successful} successful, {total_failures} failed, {skipped} skipped, {retries} retries"
        );

        if failures.is_empty() {
            return;
        }

        if let Some(cp) = failures.well_known {
            error!(
                "Missing or unreadable .well-known/stellar-history.json (restore from checkpoint {cp} / 0x{cp:08x})"
            );
        }
        let missing_history = failures.count_files(FileFlags::HISTORY);
        let missing_ledger = failures.count_files(FileFlags::LEDGER);
        let missing_transactions = failures.count_files(FileFlags::TRANSACTIONS);
        let missing_results = failures.count_files(FileFlags::RESULTS);
        let missing_scp = failures.count_files(FileFlags::SCP);
        if missing_history > 0 {
            error!("Missing {missing_history} history files");
        }
        if missing_ledger > 0 {
            error!("Missing {missing_ledger} ledger header files");
        }
        if missing_transactions > 0 {
            error!("Missing {missing_transactions} transactions files");
        }
        if missing_results > 0 {
            error!("Missing {missing_results} results files");
        }
        if total_bucket_failures > 0 {
            error!("Missing {total_bucket_failures} bucket files");
        }
        if missing_scp > 0 {
            warn!("Missing {missing_scp} optional scp files");
        }
        if checkpoint_failures > 0 {
            error!("{checkpoint_failures} checkpoint(s) failed verification (cross-file or chain)");
        }
    }
}

/// Tracks retry state with exponential backoff
pub struct RetryState {
    pub attempt: u32,
    pub backoff_ms: u64,
    pub max_retries: u32,
}

impl RetryState {
    #[must_use]
    pub fn new(max_retries: u32, initial_backoff_ms: u64) -> Self {
        Self {
            attempt: 0,
            backoff_ms: initial_backoff_ms,
            max_retries,
        }
    }

    /// Evaluate an error and determine if we should retry.
    ///
    /// If the error is retryable and we haven't exhausted retries:
    /// - Logs retry count
    /// - Returns true (caller should call `backoff()` and retry)
    ///
    /// If the error is not retryable or retries are exhausted:
    /// - Logs an error
    /// - Returns false (caller should record failure and stop)
    pub fn should_retry(
        &mut self,
        error: &crate::storage::Error,
        action: &str,
        path: &str,
    ) -> bool {
        use crate::storage::ErrorClass;

        if error.class == ErrorClass::Retry {
            self.attempt += 1;
            if self.attempt <= self.max_retries {
                debug!(
                    "Retry {}/{} {} {}: {}",
                    self.attempt, self.max_retries, action, path, error
                );
                return true;
            }
            error!(
                "Exceeded {} retry attempts to {} {}: {}",
                self.max_retries, action, path, error
            );
        } else {
            error!("Failed to {} {}: {}", action, path, error);
        }
        false
    }

    /// Wait for the backoff period and increase it for next time
    pub async fn backoff(&mut self) {
        let duration = tokio::time::Duration::from_millis(self.backoff_ms);

        #[cfg(test)]
        if let Ok(vc) = crate::tests::utils::CLOCK_OVERRIDE.try_with(|c| c.clone()) {
            vc.sleep(duration).await;
            self.backoff_ms = (self.backoff_ms * 2).min(5000);
            return;
        }

        tokio::time::sleep(duration).await;
        self.backoff_ms = (self.backoff_ms * 2).min(5000); // Cap at 5 seconds
    }
}

/// Run a fallible async operation with retry-on-transient-error and
/// exponential backoff.
///
/// Wraps `f` in a `RetryState` loop: on `Err`, calls `should_retry` to
/// classify the error and (if retryable) waits via `backoff()` before retrying.
/// Used by the pipeline (per-file processing) and by repair manual mode.
///
/// **Signature note.** `F: FnMut() -> Fut` with an explicit `Fut: Send` bound
/// (rather than `impl AsyncFnMut() -> ...`) so the future's `Send`-ness is a
/// concrete constraint on a named type — this propagates cleanly through
/// outer compositions, including async_trait-wrapped methods that demand Send
/// on the boxed future. `AsyncFnMut`'s implicit `CallMutFuture` associated
/// type has no Send bound and is opaque, which trips Send-HRTB inference when
/// the resulting future is composed inside another async_trait method.
///
/// Caller usage: pass `|| obj.async_method(args)` (closure returns the future
/// directly) rather than `async || obj.async_method(args).await` — both work,
/// but the former avoids an extra opaque async-block layer.
pub async fn with_retries<T, F, Fut>(
    max_retries: u32,
    retry_min_delay_ms: u64,
    action: &str,
    path: &str,
    mut f: F,
) -> Result<T, crate::storage::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, crate::storage::Error>> + Send,
{
    let mut retry = RetryState::new(max_retries, retry_min_delay_ms);
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if retry.should_retry(&e, action, path) {
                    retry.backoff().await;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// Fetch and validate .well-known/stellar-history.json from store
///
/// Parameters:
/// - `store`: The storage backend to fetch from
/// - `max_retries`: Maximum number of retry attempts (0 for no retries, e.g., for local filesystem)
/// - `retry_min_delay_ms`: Initial backoff delay in milliseconds
pub async fn fetch_well_known_history_file(
    store: &StorageRef,
    max_retries: u32,
    retry_min_delay_ms: u64,
) -> Result<HistoryFileState, Error> {
    use crate::history_format::ROOT_WELL_KNOWN_PATH;

    debug!("Fetching .well-known from path: {}", ROOT_WELL_KNOWN_PATH);

    let buffer = with_retries(
        max_retries,
        retry_min_delay_ms,
        "download",
        ROOT_WELL_KNOWN_PATH,
        || crate::storage::download_buffer(store, ROOT_WELL_KNOWN_PATH),
    )
    .await
    .map_err(|e| std::io::Error::other(format!("Failed to fetch {ROOT_WELL_KNOWN_PATH}: {e}")))?;

    // Parse the JSON
    tracing::debug!("Read {} bytes from {}", buffer.len(), ROOT_WELL_KNOWN_PATH);
    if buffer.len() < 200 {
        tracing::debug!("Content: {:?}", String::from_utf8_lossy(&buffer.to_vec()));
    }
    let state: HistoryFileState = serde_json::from_reader(buffer.reader()).map_err(|e| {
        crate::history_format::Error::InvalidJson {
            path: ROOT_WELL_KNOWN_PATH.to_string(),
            error: e.to_string(),
        }
    })?;

    // Validate the .well-known format
    state.validate()?;

    Ok(state)
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
