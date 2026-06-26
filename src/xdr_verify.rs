//! XDR verification module.
//!
//! Parses and validates XDR-encoded archive files (ledger, transaction, result, SCP).
//!
//! Per-file verification:
//! - Ledger header hash: `entry.hash == SHA256(entry.header.to_xdr())`
//! - Transaction set hash: V0 = `SHA256(prev_hash || tx1 || ... || txN)`,
//!   V1 = `SHA256(GeneralizedTransactionSet.to_xdr())`
//! - Result set hash: `SHA256(tx_result_set.to_xdr())`
//! - SCP entry: validates XDR frame structure
//!
//! Cross-file verification (via [`XdrVerificationManager`]):
//! - Checkpoint completeness: all expected ledger sequences are present
//! - Transaction set hash matches ledger header's `scp_value.tx_set_hash`
//! - Result set hash matches ledger header's `tx_set_result_hash`
//! - Intra-checkpoint hash chain: `ledger[N].previous_ledger_hash == hash(ledger[N-1])`
//! - Cross-checkpoint hash chain: first ledger's `prev_hash` matches prior checkpoint's last hash

use crate::history_format::{self, CHECKPOINT_FREQUENCY, GENESIS_CHECKPOINT_LEDGER};
use crate::storage::{from_opendal_error, Error as StorageError, StorageRef};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use futures_util::StreamExt;
use opendal::{Reader, Writer};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::sync::Mutex;
use stellar_xdr::curr::{
    Frame, GeneralizedTransactionSet, Hash, LedgerHeaderHistoryEntry, Limited, Limits, ReadXdr,
    ScpHistoryEntry, TransactionHistoryEntry, TransactionHistoryEntryExt,
    TransactionHistoryResultEntry, TransactionSetV1, VecM, WriteXdr,
};
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;
use tracing::{debug, error, warn};

/// SHA256 hash of empty XDR array \[0,0,0,0\] used for ledgers with no transactions.
pub(crate) const EMPTY_XDR_ARRAY_HASH: Hash = Hash([
    0xdf, 0x3f, 0x61, 0x98, 0x04, 0xa9, 0x2f, 0xdb, 0x40, 0x57, 0x19, 0x2d, 0xc4, 0x3d, 0xd7, 0x48,
    0xea, 0x77, 0x8a, 0xdc, 0x52, 0xbc, 0x49, 0x8c, 0xe8, 0x05, 0x24, 0xc0, 0x14, 0xb8, 0x11, 0x19,
]);

/// All-zero hash. Used as a sentinel for "no result entry expected."
const ZERO_HASH: Hash = Hash([0; 32]);

/// SHA-256 of `data` returned as a `stellar_xdr::curr::Hash` (the same newtype
/// used by ledger headers, tx-set hashes, etc.).
fn sha256(data: &[u8]) -> Hash {
    Hash(Sha256::digest(data).into())
}

/// Extension methods for `stellar_xdr::curr::Hash`.
pub(crate) trait HashExt {
    /// Hex-encode the 32 bytes (lowercase, no prefix).
    fn to_hex(&self) -> String;
}

impl HashExt for Hash {
    fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

const DECOMPRESS_BUFFER_SIZE: usize = 64 * 1024;
const CHANNEL_CAPACITY: usize = 64;

/// Inclusive `(first_ledger, last_ledger)` range covered by a given checkpoint.
///
/// - **Genesis checkpoint (63)**: `(1, 63)` — ledger 0 has no header entry.
/// - **All others**: `(checkpoint - 63, checkpoint)` — 64 ledgers per checkpoint.
pub(crate) fn expected_ledger_range(checkpoint: u32) -> (u32, u32) {
    if checkpoint == GENESIS_CHECKPOINT_LEDGER {
        (1, GENESIS_CHECKPOINT_LEDGER)
    } else {
        (
            checkpoint.saturating_sub(CHECKPOINT_FREQUENCY - 1),
            checkpoint,
        )
    }
}

/// Hashes extracted from a single `LedgerHeaderHistoryEntry`, keyed by
/// ledger sequence. Produced by [`parse_ledger_header_entries_for_checkpoint`],
/// consumed by [`XdrVerificationManager`] for cross-file and hash-chain
/// verification.
///
/// Note: this is about *ledger headers*, not bucket-resident `LedgerEntry`s.
#[derive(Debug, Clone)]
pub struct LedgerHeaderVerificationData {
    /// `SHA256(entry.header.to_xdr())` — verified to equal `entry.hash` at parse time.
    pub computed_hash: Hash,
    /// `entry.header.previous_ledger_hash` — checked against prior ledger's `computed_hash`
    /// during intra-checkpoint and cross-checkpoint chain verification.
    pub prev_hash: Hash,
    /// `entry.header.scp_value.tx_set_hash` — cross-verified against the hash computed
    /// from the transaction file for the same ledger sequence.
    pub expected_tx_set_hash: Hash,
    /// `entry.header.tx_set_result_hash` — cross-verified against the hash computed
    /// from the result file for the same ledger sequence.
    pub expected_result_hash: Hash,
}

#[derive(Default)]
struct PendingCheckpoint {
    header_data: Option<BTreeMap<u32, LedgerHeaderVerificationData>>,
    tx_set_hashes: Option<BTreeMap<u32, Hash>>,
    result_hashes: Option<BTreeMap<u32, Hash>>,
}

/// First/last ledger-header hashes at a checkpoint boundary, used internally
/// by [`XdrVerificationManager`] to verify hash continuity across
/// consecutive checkpoints.
#[derive(Debug, Clone)]
struct CheckpointBoundary {
    first_prev_hash: Hash,
    last_computed_hash: Hash,
}

/// Kind of verification failure detected. The associated `u32` is the
/// identifier most natural for that kind:
/// - `Ledger`: the failing ledger sequence number (per-ledger failure).
/// - `Checkpoint`: the checkpoint ledger number (cross-ledger failure
///   contained within one checkpoint).
/// - `Boundary`: the curr checkpoint ledger number (cross-checkpoint
///   chain break between this cp and its previous cp).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationErrorType {
    Ledger(u32),
    Checkpoint(u32),
    Boundary(u32),
}

/// A verification failure detected during cross-file or chain validation.
/// Accumulated by [`XdrVerificationManager`] and retrieved via
/// [`get_errors`](XdrVerificationManager::get_errors).
#[derive(Debug, Clone)]
pub struct VerificationError {
    pub kind: VerificationErrorType,
    pub message: String,
}

impl VerificationError {
    /// The checkpoint this error pertains to, regardless of variant.
    /// `Ledger(seq)` is mapped to its containing checkpoint via
    /// `history_format::round_to_upper_checkpoint`.
    pub fn checkpoint(&self) -> u32 {
        match self.kind {
            VerificationErrorType::Ledger(seq) => history_format::round_to_upper_checkpoint(seq),
            VerificationErrorType::Checkpoint(cp) | VerificationErrorType::Boundary(cp) => cp,
        }
    }
}

/// Coordinates cross-file XDR verification across concurrent checkpoint processing.
///
/// **Usage flow:**
/// 1. For each checkpoint, record parsed data via `record_header_data`,
///    `record_tx_set_hashes`, and `record_result_hashes` (order doesn't matter,
///    can be called from different tasks concurrently).
/// 2. Call [`verify_and_release`](Self::verify_and_release) once all three files
///    for a checkpoint are recorded. This runs completeness, hash-match, and
///    intra-checkpoint chain checks, then frees the checkpoint's pending data.
/// 3. After all checkpoints are processed, call [`verify_checkpoint_chain`](Self::verify_checkpoint_chain)
///    to verify hash continuity across consecutive checkpoint boundaries.
///    Only checks adjacent checkpoints (skips non-consecutive ones from partial scans).
/// 4. Retrieve all accumulated errors via [`get_errors`](Self::get_errors).
///
/// All state is `Mutex`-protected for concurrent access from the pipeline.
pub struct XdrVerificationManager {
    pending: Mutex<HashMap<u32, PendingCheckpoint>>,
    boundaries: Mutex<BTreeMap<u32, CheckpointBoundary>>,
    errors: Mutex<Vec<VerificationError>>,
}

impl XdrVerificationManager {
    /// Construct an empty manager with no pending data, boundaries, or errors.
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            boundaries: Mutex::new(BTreeMap::new()),
            errors: Mutex::new(Vec::new()),
        }
    }

    /// Record per-ledger data parsed from the **ledger** file of a checkpoint.
    /// Overwrites any previously recorded ledger data for the same checkpoint.
    /// Pair with `record_tx_set_hashes` and `record_result_hashes`, then call
    /// [`verify_and_release`](Self::verify_and_release) once all three are in.
    pub fn record_header_data(
        &self,
        checkpoint: u32,
        data: BTreeMap<u32, LedgerHeaderVerificationData>,
    ) {
        let mut pending = self.pending.lock().unwrap();
        let entry = pending.entry(checkpoint).or_default();
        entry.header_data = Some(data);
    }

    /// Record per-ledger transaction-set hashes parsed from the **transactions**
    /// file of a checkpoint. See [`record_header_data`](Self::record_header_data).
    pub fn record_tx_set_hashes(&self, checkpoint: u32, hashes: BTreeMap<u32, Hash>) {
        let mut pending = self.pending.lock().unwrap();
        let entry = pending.entry(checkpoint).or_default();
        entry.tx_set_hashes = Some(hashes);
    }

    /// Record per-ledger result-set hashes parsed from the **results** file of
    /// a checkpoint. See [`record_header_data`](Self::record_header_data).
    pub fn record_result_hashes(&self, checkpoint: u32, hashes: BTreeMap<u32, Hash>) {
        let mut pending = self.pending.lock().unwrap();
        let entry = pending.entry(checkpoint).or_default();
        entry.result_hashes = Some(hashes);
    }

    /// Dispatch a parsed XDR result to the matching `record_*` method,
    /// deriving the checkpoint from `path`. No-op when `path` doesn't carry
    /// a checkpoint (e.g. unrecognized archive file) or the result is
    /// [`XdrParseResult::None`] (SCP files).
    pub fn record(&self, path: &str, result: XdrParseResult) {
        let Some(cp) = history_format::checkpoint_from_path(path) else {
            return;
        };
        match result {
            XdrParseResult::Ledger(data) => self.record_header_data(cp, data),
            XdrParseResult::Transactions(hashes) => self.record_tx_set_hashes(cp, hashes),
            XdrParseResult::Results(hashes) => self.record_result_hashes(cp, hashes),
            XdrParseResult::None => {}
        }
    }

    /// Run all intra-checkpoint verifications and free the pending data.
    ///
    /// Runs (in order): completeness check, tx set hash cross-check, result hash
    /// cross-check, internal hash chain, then stores the first/last boundary
    /// hashes for later cross-checkpoint verification. All errors are accumulated
    /// into `self.errors`; retrieve them via [`get_errors`](Self::get_errors).
    ///
    /// No-op if no data was recorded for this checkpoint. If ledger data is
    /// missing but tx/result data exists, records a warning error.
    pub(crate) fn verify_and_release(&self, checkpoint: u32) {
        let _g = crate::phase!(crate::metrics::Phase::CrossFileVerify);
        let data = {
            let mut pending = self.pending.lock().unwrap();
            pending.remove(&checkpoint)
        };

        let Some(data) = data else {
            return;
        };

        let Some(header_data) = data.header_data else {
            let err_msg = "missing ledger verification data for checkpoint";
            warn!("Checkpoint {checkpoint}: skipping cross-verification because {err_msg}");
            self.errors.lock().unwrap().push(VerificationError {
                kind: VerificationErrorType::Checkpoint(checkpoint),
                message: err_msg.to_string(),
            });
            return;
        };

        self.verify_checkpoint_completeness(checkpoint, &header_data);

        if let Some(tx_set_hashes) = data.tx_set_hashes {
            self.verify_tx_set_hashes_internal(&header_data, &tx_set_hashes);
        }

        if let Some(result_hashes) = data.result_hashes {
            self.verify_result_hashes_internal(&header_data, &result_hashes);
        }

        self.verify_internal_chain(checkpoint, &header_data);
        self.store_boundary(checkpoint, &header_data);
    }

    /// Verify that `header_data` covers exactly the ledger sequences expected
    /// for this checkpoint (per [`expected_ledger_range`]).
    ///
    /// Returns two error kinds:
    /// - **Unexpected**: ledger-header entries with sequences outside the expected range
    /// - **Missing**: ledger sequences in the expected range with no header entry
    ///   (truncated to the first 5 + a count when more than 10 are missing)
    fn verify_checkpoint_completeness(
        &self,
        checkpoint: u32,
        header_data: &BTreeMap<u32, LedgerHeaderVerificationData>,
    ) {
        let mut errors = Vec::new();
        let (first_ledger, last_ledger) = expected_ledger_range(checkpoint);
        let range = first_ledger..=last_ledger;
        let fmt_list = |seqs: &[u32]| {
            seqs.iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let unexpected: Vec<u32> = header_data
            .keys()
            .copied()
            .filter(|seq| !range.contains(seq))
            .collect();

        if !unexpected.is_empty() {
            let err_msg = format!(
                "unexpected ledger-header entries outside range {first_ledger}-{last_ledger}: {}",
                fmt_list(&unexpected),
            );
            error!("Checkpoint {checkpoint}: {err_msg}");
            errors.push(VerificationError {
                kind: VerificationErrorType::Checkpoint(checkpoint),
                message: err_msg,
            });
        }

        let missing: Vec<u32> = range.filter(|seq| !header_data.contains_key(seq)).collect();

        if !missing.is_empty() {
            let list_str = if missing.len() <= 10 {
                fmt_list(&missing)
            } else {
                format!(
                    "{}, ... ({} more)",
                    fmt_list(&missing[..5]),
                    missing.len() - 5
                )
            };
            let err_msg = format!(
                "missing {} of {} ledger-header entries (ledgers {first_ledger}-{last_ledger}): {list_str}",
                missing.len(),
                last_ledger - first_ledger + 1,
            );
            error!("Checkpoint {checkpoint}: {err_msg}");
            errors.push(VerificationError {
                kind: VerificationErrorType::Checkpoint(checkpoint),
                message: err_msg,
            });
        }
        if !errors.is_empty() {
            self.errors.lock().unwrap().extend(errors);
        }
    }

    /// Cross-verify per-ledger tx-set hashes from the **transactions** file
    /// against the `expected_tx_set_hash` field embedded in the ledger header.
    ///
    /// For each ledger sequence in `header_data`:
    /// - **Mismatch**: actual tx-set hash differs from `expected_tx_set_hash`
    /// - **Missing**: no entry in `tx_set_hashes`, *and* the ledger isn't
    ///   genuinely empty. A missing entry is acceptable only when both the
    ///   expected result hash equals [`EMPTY_XDR_ARRAY_HASH`] *and* the
    ///   expected tx-set hash is one of the recognized "empty tx set"
    ///   sentinels (see [`is_empty_tx_set_hash`]) — stellar-core omits empty
    ///   tx-set entries from the transactions file.
    fn verify_tx_set_hashes_internal(
        &self,
        header_data: &BTreeMap<u32, LedgerHeaderVerificationData>,
        tx_set_hashes: &BTreeMap<u32, Hash>,
    ) {
        let mut errors = Vec::new();
        for (&seq, data) in header_data {
            let expected = &data.expected_tx_set_hash;

            let err_msg = if let Some(actual) = tx_set_hashes.get(&seq) {
                if actual == expected {
                    None
                } else {
                    Some(format!(
                        "tx set hash mismatch: expected {}, got {}",
                        expected.to_hex(),
                        actual.to_hex(),
                    ))
                }
            } else if data.expected_result_hash != EMPTY_XDR_ARRAY_HASH
                && !is_empty_tx_set_hash(expected, &data.prev_hash)
            {
                Some(format!(
                    "missing tx set entry, expected hash {}",
                    expected.to_hex(),
                ))
            } else {
                None
            };

            if let Some(err_msg) = err_msg {
                error!("Ledger {seq}: {err_msg}");
                errors.push(VerificationError {
                    kind: VerificationErrorType::Ledger(seq),
                    message: err_msg,
                });
            }
        }
        if !errors.is_empty() {
            self.errors.lock().unwrap().extend(errors);
        }
    }

    /// Cross-verify per-ledger result-set hashes from the **results** file
    /// against the `expected_result_hash` field embedded in the ledger header.
    ///
    /// For each ledger sequence in `header_data`:
    /// - **Mismatch**: actual result-set hash differs from `expected_result_hash`
    /// - **Missing**: no entry in `result_hashes` *and* `expected_result_hash`
    ///   is non-zero and not [`EMPTY_XDR_ARRAY_HASH`] (those two values mark
    ///   "no result entry expected" and are tolerated as missing).
    fn verify_result_hashes_internal(
        &self,
        header_data: &BTreeMap<u32, LedgerHeaderVerificationData>,
        result_hashes: &BTreeMap<u32, Hash>,
    ) {
        let mut errors = Vec::new();
        for (&seq, data) in header_data {
            let expected = &data.expected_result_hash;

            let err_msg = if let Some(actual) = result_hashes.get(&seq) {
                if actual == expected {
                    None
                } else {
                    Some(format!(
                        "result set hash mismatch: expected {}, got {}",
                        expected.to_hex(),
                        actual.to_hex(),
                    ))
                }
            } else if *expected != EMPTY_XDR_ARRAY_HASH && *expected != ZERO_HASH {
                Some(format!(
                    "missing result entry, expected hash {}",
                    expected.to_hex(),
                ))
            } else {
                None
            };

            if let Some(err_msg) = err_msg {
                error!("Ledger {seq}: {err_msg}");
                errors.push(VerificationError {
                    kind: VerificationErrorType::Ledger(seq),
                    message: err_msg,
                });
            }
        }
        if !errors.is_empty() {
            self.errors.lock().unwrap().extend(errors);
        }
    }

    /// Verify the hash chain *within* a single checkpoint.
    ///
    /// For each adjacent ledger pair `(prev, curr)` in `header_data`:
    /// - **Consecutive**: `curr.seq == prev.seq + 1` (else "missing predecessor")
    /// - **Linked**: `curr.prev_hash == prev.computed_hash` (else "hash chain break")
    ///
    /// The link across checkpoint boundaries (this checkpoint's first ledger ↔
    /// prior checkpoint's last) is handled separately by
    /// [`verify_checkpoint_chain`](Self::verify_checkpoint_chain).
    fn verify_internal_chain(
        &self,
        checkpoint: u32,
        header_data: &BTreeMap<u32, LedgerHeaderVerificationData>,
    ) {
        let mut errors = Vec::new();
        let entries: Vec<_> = header_data.iter().collect();

        for pair in entries.windows(2) {
            let mut kind_and_msg: Option<(VerificationErrorType, String)> = None;

            if let [(&prev_seq, prev_data), (&seq, data)] = pair {
                if prev_seq.saturating_add(1) != seq {
                    let msg = format!("missing predecessor ledger {}", seq.saturating_sub(1),);
                    kind_and_msg = Some((VerificationErrorType::Ledger(seq), msg));
                } else if prev_data.computed_hash != data.prev_hash {
                    let msg = format!(
                        "hash chain break: previous_ledger_hash {} != computed hash of ledger {} ({})",
                        data.prev_hash.to_hex(),
                        prev_seq,
                        prev_data.computed_hash.to_hex(),
                    );
                    kind_and_msg = Some((VerificationErrorType::Ledger(seq), msg));
                }
            } else {
                kind_and_msg = Some((
                    VerificationErrorType::Checkpoint(checkpoint),
                    "internal error: unexpected window size in chain verification".to_string(),
                ));
            }

            if let Some((kind, err_msg)) = kind_and_msg {
                error!("Checkpoint {checkpoint}: {err_msg}");
                errors.push(VerificationError {
                    kind,
                    message: err_msg,
                });
            }
        }
        if !errors.is_empty() {
            self.errors.lock().unwrap().extend(errors);
        }
    }

    /// Stash the first/last ledger hashes of this checkpoint into
    /// `self.boundaries` so that [`verify_checkpoint_chain`](Self::verify_checkpoint_chain)
    /// can later check hash continuity across adjacent checkpoints.
    ///
    /// Stores:
    /// - `first_prev_hash` = first entry's `previous_ledger_hash`
    ///   (must equal the prior checkpoint's `last_computed_hash`)
    /// - `last_computed_hash` = last entry's `computed_hash` (the SHA-256 of
    ///   that ledger's header, which the next checkpoint will reference)
    ///
    /// No-op when `header_data` is empty (nothing to bracket).
    fn store_boundary(
        &self,
        checkpoint: u32,
        header_data: &BTreeMap<u32, LedgerHeaderVerificationData>,
    ) {
        let (Some((_, first_data)), Some((_, last_data))) =
            (header_data.first_key_value(), header_data.last_key_value())
        else {
            return;
        };

        self.boundaries.lock().unwrap().insert(
            checkpoint,
            CheckpointBoundary {
                first_prev_hash: first_data.prev_hash.clone(),
                last_computed_hash: last_data.computed_hash.clone(),
            },
        );
    }

    /// Verify hash chain continuity across consecutive checkpoint boundaries.
    /// Detected errors are pushed into `self.errors`; retrieve via
    /// [`get_errors`](Self::get_errors).
    ///
    /// Only checks adjacent checkpoints separated by exactly `CHECKPOINT_FREQUENCY` —
    /// non-consecutive checkpoints (from partial or bounded scans) are skipped.
    pub(crate) fn verify_checkpoint_chain(&self) {
        let _g = crate::phase!(crate::metrics::Phase::ChainVerify);
        let boundaries = self.boundaries.lock().unwrap();
        let mut chain_errors = Vec::new();

        let entries: Vec<_> = boundaries.iter().collect();

        // TODO: need to handle corner case of only 1 entry.
        for window in entries.windows(2) {
            let (&prev_checkpoint, prev_boundary) = window[0];
            let (&curr_checkpoint, curr_boundary) = window[1];

            if curr_checkpoint != prev_checkpoint + CHECKPOINT_FREQUENCY {
                continue;
            }

            if curr_boundary.first_prev_hash != prev_boundary.last_computed_hash {
                let first_ledger = curr_checkpoint.saturating_sub(63);
                let err_msg = format!(
                    "hash chain break between checkpoints {prev_checkpoint} and {curr_checkpoint}: \
                     ledger {first_ledger} prev_hash {} != checkpoint {prev_checkpoint} last hash {}",
                    curr_boundary.first_prev_hash.to_hex(),
                    prev_boundary.last_computed_hash.to_hex(),
                );
                error!("{err_msg}");
                chain_errors.push(VerificationError {
                    kind: VerificationErrorType::Boundary(curr_checkpoint),
                    message: err_msg,
                });
            }
        }

        if !chain_errors.is_empty() {
            self.errors.lock().unwrap().extend(chain_errors);
        }
    }

    /// Snapshot of all verification errors accumulated so far. Each call
    /// returns a fresh `Vec`; the manager retains the underlying state.
    #[cfg(test)]
    pub fn get_errors(&self) -> Vec<VerificationError> {
        self.errors.lock().unwrap().clone()
    }

    /// Drain accumulated verification errors into the supplied `FailureTracker`
    /// (which the caller already owns mutably — e.g. via `stats.failures`
    /// inside `Operation::finalize`). All variants funnel into the
    /// `checkpoints` slot — these are cross-file/cross-cp inconsistencies, not
    /// per-file failures.
    ///
    /// Clears the manager's error list at the end, so a subsequent call is a
    /// true no-op (idempotent by draining, not by relying on the tracker's set).
    ///
    /// Sync because the caller has exclusive `&mut` access and the manager's
    /// std `Mutex` is held only across the cheap iteration. No clone, no
    /// `.await`.
    pub fn drain_all_errors(&self, failures: &mut crate::utils::FailureTracker) {
        let mut errors = self.errors.lock().unwrap();
        for err in errors.iter() {
            failures.record_verification_failure(&err.kind);
        }
        if !errors.is_empty() {
            error!(
                "XDR verification found {} cross-file or chain inconsistency error(s)",
                errors.len()
            );
        }
        errors.clear();
    }
}

impl Default for XdrVerificationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse decompressed ledger XDR data into per-ledger verification data.
///
/// For each frame, verifies `SHA256(header.to_xdr()) == entry.hash` and extracts
/// the `prev_hash`, `tx_set_hash`, and `result_hash` fields for cross-file checks.
/// When `checkpoint` is `Some`, also rejects ledger sequences outside the
/// expected range.
///
/// Returns a fatal error on hash mismatch, duplicate sequence, or malformed XDR.
/// Empty input returns an empty map (valid for checkpoints with no ledger file).
pub(crate) fn parse_ledger_header_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<BTreeMap<u32, LedgerHeaderVerificationData>, StorageError> {
    let _g = crate::phase!(crate::metrics::Phase::XdrParseLedger);
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());
    let mut data = BTreeMap::new();

    let expected_range = checkpoint.map(expected_ledger_range);

    for result in Frame::<LedgerHeaderHistoryEntry>::read_xdr_iter(&mut limited) {
        let Frame(entry) = result.map_err(|e| {
            StorageError::fatal(format!("failed to parse LedgerHeaderHistoryEntry: {}", e))
        })?;

        let seq = entry.header.ledger_seq;

        if let Some((first_ledger, last_ledger)) = expected_range {
            if !(first_ledger..=last_ledger).contains(&seq) {
                return Err(StorageError::fatal(format!(
                    "ledger seq {} is outside expected checkpoint range {}-{}",
                    seq, first_ledger, last_ledger
                )));
            }
        }

        if data.contains_key(&seq) {
            return Err(StorageError::fatal(format!(
                "duplicate ledger-header entry for seq {}",
                seq
            )));
        }

        let header_xdr = entry.header.to_xdr(Limits::none()).map_err(|e| {
            StorageError::fatal(format!(
                "failed to serialize ledger header for seq {}: {}",
                seq, e
            ))
        })?;

        let computed_hash = sha256(&header_xdr);
        let expected_hash = entry.hash;

        if computed_hash != expected_hash {
            return Err(StorageError::fatal(format!(
                "ledger header hash mismatch at seq {}: expected {}, computed {}",
                seq,
                expected_hash.to_hex(),
                computed_hash.to_hex()
            )));
        }

        debug!("Verified ledger {} hash: {}", seq, computed_hash.to_hex());

        data.insert(
            seq,
            LedgerHeaderVerificationData {
                computed_hash,
                prev_hash: entry.header.previous_ledger_hash,
                expected_tx_set_hash: entry.header.scp_value.tx_set_hash,
                expected_result_hash: entry.header.tx_set_result_hash,
            },
        );
    }

    Ok(data)
}

/// Hash of an empty V0 `TransactionSet`: `SHA256(previous_ledger_hash)` —
/// equivalent to the V0 hash recipe with zero transactions concatenated.
pub(crate) fn compute_empty_v0_tx_set_hash(previous_ledger_hash: &Hash) -> Hash {
    sha256(&previous_ledger_hash.0)
}

/// Hash of an empty V1 `GeneralizedTransactionSet`: SHA-256 of the
/// XDR-serialized struct with `previous_ledger_hash` set and no phases.
pub(crate) fn compute_empty_v1_tx_set_hash(previous_ledger_hash: &Hash) -> Hash {
    let empty_v1 = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: previous_ledger_hash.clone(),
        phases: VecM::default(),
    });
    let xdr = empty_v1
        .to_xdr(Limits::none())
        .expect("serializing empty GeneralizedTransactionSet should not fail");
    sha256(&xdr)
}

/// Whether `expected` is one of the recognized "no transactions in this ledger"
/// markers, given the ledger's `prev_hash`.
///
/// Treated as empty if `expected` matches any of:
/// - all-zero hash ([`ZERO_HASH`])
/// - [`compute_empty_v0_tx_set_hash`]`(prev_hash)` — empty V0 set
/// - [`compute_empty_v1_tx_set_hash`]`(prev_hash)` — empty V1 set
///
/// Used by [`verify_tx_set_hashes_internal`](XdrVerificationManager::verify_tx_set_hashes_internal)
/// to decide whether a missing transactions-file entry is acceptable.
pub(crate) fn is_empty_tx_set_hash(expected: &Hash, prev_hash: &Hash) -> bool {
    *expected == ZERO_HASH
        || *expected == compute_empty_v0_tx_set_hash(prev_hash)
        || *expected == compute_empty_v1_tx_set_hash(prev_hash)
}

/// Compute the hash of a V0 TransactionSet.
///
/// The V0 hash is computed as:
///   SHA256(previous_ledger_hash || tx1_xdr || tx2_xdr || ... || txN_xdr)
///
/// This matches stellar-core's `computeNonGeneralizedTxSetContentsHash()`.
pub(crate) fn compute_v0_tx_set_hash(
    tx_set: &stellar_xdr::curr::TransactionSet,
) -> Result<Hash, StorageError> {
    let mut serialized_txs = Vec::with_capacity(tx_set.txs.len());
    for tx in tx_set.txs.iter() {
        let tx_xdr = tx.to_xdr(Limits::none()).map_err(|e| {
            StorageError::fatal(format!("failed to serialize TransactionEnvelope: {}", e))
        })?;
        let tx_hash = sha256(&tx_xdr);
        serialized_txs.push((tx_hash, tx_xdr));
    }

    if serialized_txs.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        return Err(StorageError::fatal(
            "TransactionSet contains transactions out of hash order",
        ));
    }

    let mut hasher = Sha256::new();
    hasher.update(tx_set.previous_ledger_hash.0);
    for (_, tx_xdr) in serialized_txs {
        hasher.update(&tx_xdr);
    }
    Ok(Hash(hasher.finalize().into()))
}

/// Compute the hash of a V1 GeneralizedTransactionSet.
///
/// The V1 hash is simply SHA256 of the entire XDR-serialized struct.
/// This matches stellar-core's `xdrSha256(xdrTxSet)`.
pub(crate) fn compute_v1_tx_set_hash(
    generalized_tx_set: &stellar_xdr::curr::GeneralizedTransactionSet,
) -> Result<Hash, StorageError> {
    let xdr = generalized_tx_set.to_xdr(Limits::none()).map_err(|e| {
        StorageError::fatal(format!(
            "failed to serialize GeneralizedTransactionSet: {}",
            e
        ))
    })?;

    Ok(sha256(&xdr))
}

/// Parse decompressed result XDR data, computing `SHA256(tx_result_set.to_xdr())`
/// per ledger. These hashes are cross-verified against `expected_result_hash` from
/// the ledger headers. When `checkpoint` is `Some`, rejects ledger sequences
/// outside the expected range.
///
/// Returns a fatal error on duplicate sequence, out-of-range sequence, or malformed XDR.
pub(crate) fn parse_result_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<BTreeMap<u32, Hash>, StorageError> {
    let _g = crate::phase!(crate::metrics::Phase::XdrParseResult);
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());
    let mut hashes = BTreeMap::new();

    let expected_range = checkpoint.map(expected_ledger_range);

    for result in Frame::<TransactionHistoryResultEntry>::read_xdr_iter(&mut limited) {
        let Frame(entry) = result.map_err(|e| {
            StorageError::fatal(format!(
                "failed to parse TransactionHistoryResultEntry: {}",
                e
            ))
        })?;

        let seq = entry.ledger_seq;

        if let Some((first_ledger, last_ledger)) = expected_range {
            if !(first_ledger..=last_ledger).contains(&seq) {
                return Err(StorageError::fatal(format!(
                    "result entry ledger seq {} is outside expected checkpoint range {}-{}",
                    seq, first_ledger, last_ledger
                )));
            }
        }

        if hashes.contains_key(&seq) {
            return Err(StorageError::fatal(format!(
                "duplicate result entry for ledger seq {}",
                seq
            )));
        }

        let result_xdr = entry.tx_result_set.to_xdr(Limits::none()).map_err(|e| {
            StorageError::fatal(format!(
                "failed to serialize tx_result_set for ledger {}: {}",
                seq, e
            ))
        })?;

        let computed_hash = sha256(&result_xdr);

        debug!(
            "Computed result hash for ledger {}: {}",
            seq,
            computed_hash.to_hex()
        );

        hashes.insert(seq, computed_hash);
    }

    Ok(hashes)
}

pub async fn parse_ledger_header_stream(
    path: &str,
    reader: Reader,
) -> Result<BTreeMap<u32, LedgerHeaderVerificationData>, StorageError> {
    let cp = history_format::checkpoint_from_path(path);
    Ok(decompress_then(path, reader, None, move |b| {
        parse_ledger_header_entries_for_checkpoint(b, cp)
    })
    .await?
    .0)
}

/// Decompress a gzipped result file from a reader and parse it.
/// Infers the checkpoint number from the file path for range validation.
pub async fn parse_results_stream(
    path: &str,
    reader: Reader,
) -> Result<BTreeMap<u32, Hash>, StorageError> {
    let cp = history_format::checkpoint_from_path(path);
    Ok(decompress_then(path, reader, None, move |b| {
        parse_result_entries_for_checkpoint(b, cp)
    })
    .await?
    .0)
}

/// Parse decompressed transaction XDR data, computing content hashes per ledger.
/// V0 entries: `SHA256(prev_hash || tx1_xdr || ... || txN_xdr)`.
/// V1 entries: `SHA256(GeneralizedTransactionSet.to_xdr())`.
/// These hashes are cross-verified against `expected_tx_set_hash` from the ledger headers.
/// When `checkpoint` is `Some`, rejects ledger sequences outside the expected range.
///
/// Returns a fatal error on duplicate sequence, out-of-range sequence, malformed XDR,
/// or V0 transactions not in hash-sorted order.
pub(crate) fn parse_transaction_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<BTreeMap<u32, Hash>, StorageError> {
    let _g = crate::phase!(crate::metrics::Phase::XdrParseTx);
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());
    let mut hashes = BTreeMap::new();

    let expected_range = checkpoint.map(expected_ledger_range);

    for result in Frame::<TransactionHistoryEntry>::read_xdr_iter(&mut limited) {
        let Frame(entry) = result.map_err(|e| {
            StorageError::fatal(format!("failed to parse TransactionHistoryEntry: {}", e))
        })?;

        let seq = entry.ledger_seq;

        if let Some((first_ledger, last_ledger)) = expected_range {
            if !(first_ledger..=last_ledger).contains(&seq) {
                return Err(StorageError::fatal(format!(
                    "transaction entry ledger seq {} is outside expected checkpoint range {}-{}",
                    seq, first_ledger, last_ledger
                )));
            }
        }

        if hashes.contains_key(&seq) {
            return Err(StorageError::fatal(format!(
                "duplicate transaction entry for ledger seq {}",
                seq
            )));
        }

        let computed_hash = match &entry.ext {
            TransactionHistoryEntryExt::V0 => compute_v0_tx_set_hash(&entry.tx_set)?,
            TransactionHistoryEntryExt::V1(generalized_tx_set) => {
                compute_v1_tx_set_hash(generalized_tx_set)?
            }
        };

        debug!(
            "Computed tx set hash for ledger {}: {}",
            seq,
            computed_hash.to_hex()
        );

        hashes.insert(seq, computed_hash);
    }

    Ok(hashes)
}

/// Validate SCP history XDR frame structure. Only checks that frames deserialize
/// correctly — no hashes are computed or returned. Fatal error on malformed XDR.
pub fn parse_scp_entries(decompressed_data: &[u8]) -> Result<(), StorageError> {
    let _g = crate::phase!(crate::metrics::Phase::XdrParseScp);
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());

    for result in Frame::<ScpHistoryEntry>::read_xdr_iter(&mut limited) {
        result
            .map_err(|e| StorageError::fatal(format!("failed to parse ScpHistoryEntry: {}", e)))?;
    }

    Ok(())
}

/// Decompress a gzipped transaction file from a reader and parse it.
/// Infers the checkpoint number from the file path for range validation.
pub async fn parse_transactions_stream(
    path: &str,
    reader: Reader,
) -> Result<BTreeMap<u32, Hash>, StorageError> {
    let cp = history_format::checkpoint_from_path(path);
    Ok(decompress_then(path, reader, None, move |b| {
        parse_transaction_entries_for_checkpoint(b, cp)
    })
    .await?
    .0)
}

/// Decompress a gzipped SCP file from a reader and validate its frame structure.
pub async fn parse_scp_stream(path: &str, reader: Reader) -> Result<(), StorageError> {
    decompress_then(path, reader, None, parse_scp_entries)
        .await
        .map(|(parsed, _sink)| parsed)
}

/// Result of parsing an XDR archive file during a verified mirror write.
/// Carries computed hashes for the caller to feed into [`XdrVerificationManager`].
/// `None` is returned for SCP files and unrecognized file types.
pub enum XdrParseResult {
    Ledger(BTreeMap<u32, LedgerHeaderVerificationData>),
    Transactions(BTreeMap<u32, Hash>),
    Results(BTreeMap<u32, Hash>),
    None,
}

/// Streaming core for every XDR file: read gzipped bytes from `reader`,
/// optionally tee the still-compressed bytes to `writer`, gzip-decode to a
/// buffer, and run `parse` on it ALL inside one spawned task (gzip decode AND
/// the XDR parse/hash leave the orchestration task; the caller only feeds
/// chunks). Returns the parse result alongside the **unclosed** sink (`None`
/// when `writer` is `None`); the caller commits a write only after `parse`
/// succeeds (parse-before-commit), so a parse failure never leaves committed
/// corrupt data.
///
/// Error classes (the pipeline retries only `ErrorClass::Retry`): gzip framing
/// → `retry`; XDR parse/hash → whatever `parse` returns (`fatal` in practice);
/// storage I/O → opendal-classified; channel-closed or task panic → `fatal`.
async fn decompress_then<T>(
    path: &str,
    reader: Reader,
    writer: Option<Writer>,
    parse: impl FnOnce(&[u8]) -> Result<T, StorageError> + Send + 'static,
) -> Result<(T, Option<opendal::BufferSink>), StorageError>
where
    T: Send + 'static,
{
    let decompress_phase = crate::phase!(crate::metrics::Phase::XdrDecompress);
    use futures_util::SinkExt;

    let stream = reader
        .into_stream(..)
        .await
        .map_err(|e| from_opendal_error(e, &format!("failed to create stream for {}", path)))?;

    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_CAPACITY);
    let path_owned = path.to_string();

    // Decode then parse, both inside the spawned task. Returns the parse result
    // plus the decompressed length so the parent records byte metrics exactly once.
    let task = tokio::spawn(async move {
        let _dg = crate::metrics::DecodeGuard::enter();
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
        let mut decoder = GzipDecoder::new(BufReader::new(StreamReader::new(stream)));

        let mut decompressed = Vec::new();
        let mut buf = vec![0u8; DECOMPRESS_BUFFER_SIZE];
        loop {
            let n = decoder.read(&mut buf).await.map_err(|e| {
                StorageError::retry(format!("failed to decompress {}: {}", path_owned, e))
            })?;
            if n == 0 {
                break;
            }
            decompressed.extend_from_slice(&buf[..n]);
        }

        let len = decompressed.len() as u64;
        let parsed = parse(&decompressed)?; // parse carries its own phase!(XdrParse*)
        Ok::<(T, u64), StorageError>((parsed, len))
    });

    futures_util::pin_mut!(stream);
    let mut sink = writer.map(|w| w.into_sink());
    let mut streaming_error: Option<StorageError> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok(buffer) => {
                if let Some(ref mut s) = sink {
                    if let Err(e) = s.send(buffer.clone()).await {
                        streaming_error = Some(from_opendal_error(
                            e,
                            &format!("failed to write to {}", path),
                        ));
                        break;
                    }
                }
                for chunk in buffer {
                    if tx.send(chunk).await.is_err() {
                        streaming_error = Some(StorageError::fatal(
                            "decompress channel closed unexpectedly",
                        ));
                        break;
                    }
                }
                if streaming_error.is_some() {
                    break;
                }
            }
            Err(e) => {
                streaming_error = Some(from_opendal_error(
                    e,
                    &format!("failed to read from {}", path),
                ));
                break;
            }
        }
    }

    drop(tx);

    if let Some(err) = streaming_error {
        task.abort();
        return Err(err); // sink dropped here unclosed — caller cleans up
    }

    let (parsed, len) = task.await.map_err(|e| {
        StorageError::fatal(format!(
            "decompress/parse task panicked for {}: {}",
            path, e
        ))
    })??;

    decompress_phase.record_file(len);

    Ok((parsed, sink))
}

/// Remove a partially-written file after a verified-write fails.
///
/// - **Atomic backends**: no-op — dropping the sink without `close()` already
///   prevents the temp-to-target rename, so nothing landed at `path`.
/// - **Non-atomic backends**: data sent via `sink.send()` is written directly
///   to `path`, so the abandoned partial file is `tokio::fs::remove_file`'d.
///   `NotFound` is silently tolerated; other errors are warned but not raised.
///
/// Safe to call on backends without a base path (e.g. HTTP) — does nothing.
pub(crate) async fn cleanup_non_atomic_partial_write(path: &str, dst_store: &StorageRef) {
    if dst_store.uses_atomic_writes() {
        return;
    }
    if let Some(base_path) = dst_store.get_base_path() {
        let file_path = base_path.join(path);
        if let Err(remove_err) = tokio::fs::remove_file(&file_path).await {
            if remove_err.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "failed to remove partially written file {} after error: {}",
                    file_path.display(),
                    remove_err
                );
            }
        }
    }
}

/// Commit a verified write performed at `write_path` to its final `path`.
///
/// - **Atomic backends**: `write_path == path` (the backend already committed
///   via its own temp-to-target rename on `close()`) — no-op.
/// - **Non-atomic backends**: rename the `.tmp` sibling to the final path.
///   The temp is removed on rename failure to avoid leaking `.tmp` files;
///   the pre-existing file at `path` (if any) is only replaced by the rename,
///   never deleted on failure.
pub(crate) async fn commit_non_atomic_write(
    dst_store: &StorageRef,
    write_path: &str,
    path: &str,
) -> Result<(), StorageError> {
    if write_path == path {
        return Ok(());
    }
    if let Some(base) = dst_store.get_base_path() {
        let tmp_full = base.join(write_path);
        let final_full = base.join(path);
        if let Err(e) = tokio::fs::rename(&tmp_full, &final_full).await {
            let _ = tokio::fs::remove_file(&tmp_full).await;
            return Err(StorageError::fatal(format!(
                "failed to rename {} to {}: {}",
                tmp_full.display(),
                final_full.display(),
                e
            )));
        }
    }
    Ok(())
}

/// Decompress, verify XDR structure, and write to destination in a single
/// streaming pass.
///
/// File type is inferred from `path`:
/// - **ledger** → `XdrParseResult::Ledger` with per-ledger hashes
/// - **transactions** → `XdrParseResult::Transactions`
/// - **results** → `XdrParseResult::Results`
/// - **scp** → `XdrParseResult::None` (frame structure validated, no hashes)
/// - other → `XdrParseResult::None`
///
/// Atomicity: the sink is closed (committing the write) only after the
/// in-memory decompressed payload parses successfully. On parse or
/// decompression failure the sink is dropped without close; for non-atomic
/// backends the partial file is additionally removed on disk.
///
/// Caller passes the returned hashes to
/// [`XdrVerificationManager::record_*`](XdrVerificationManager) for
/// cross-file verification.
pub async fn verify_and_write_xdr(
    path: &str,
    reader: Reader,
    dst_store: &StorageRef,
) -> Result<XdrParseResult, StorageError> {
    use futures_util::SinkExt;

    // For non-atomic backends, write to a `.tmp` sibling first and rename to
    // `path` only after parsing succeeds. This prevents a partial file from
    // being visible at the final path if the process is killed mid-stream
    // (SIGKILL, OOM), which would otherwise cause `pre_check()` to skip
    // re-downloading on resume. Atomic backends handle this internally.
    let write_path: String = if dst_store.uses_atomic_writes() {
        path.to_string()
    } else {
        format!("{path}.tmp")
    };

    let writer = dst_store.open_writer(&write_path).await?;

    // Decode + parse run together in the spawned task. The closure captures
    // only owned data (the logical `path` for file-type detection and `cp`) so
    // it is `Send + 'static`. Classify on `path`, NOT `write_path`, so a `.tmp`
    // sibling is never misread as an unrecognized type.
    let cp = history_format::checkpoint_from_path(path);
    let path_owned = path.to_string();
    let parse = move |b: &[u8]| -> Result<XdrParseResult, StorageError> {
        if crate::history_format::is_ledger_header_file(&path_owned) {
            parse_ledger_header_entries_for_checkpoint(b, cp).map(XdrParseResult::Ledger)
        } else if crate::history_format::is_results_file(&path_owned) {
            parse_result_entries_for_checkpoint(b, cp).map(XdrParseResult::Results)
        } else if crate::history_format::is_transactions_file(&path_owned) {
            parse_transaction_entries_for_checkpoint(b, cp).map(XdrParseResult::Transactions)
        } else if crate::history_format::is_scp_file(&path_owned) {
            parse_scp_entries(b).map(|()| XdrParseResult::None)
        } else {
            Ok(XdrParseResult::None)
        }
    };

    match decompress_then(&write_path, reader, Some(writer), parse).await {
        Ok((result, sink)) => {
            // Parse passed — close the sink to commit the write.
            if let Some(mut s) = sink {
                s.close().await.map_err(|e| {
                    from_opendal_error(e, &format!("failed to close {}", write_path))
                })?;
            }
            // Non-atomic FS backend: rename the temp to the final path.
            commit_non_atomic_write(dst_store, &write_path, path).await?;
            Ok(result)
        }
        Err(e) => {
            // Decode or parse failed — `decompress_then` dropped the sink unclosed
            // (no commit on atomic backends). On non-atomic backends partial data
            // may be on disk via send(), so remove it.
            cleanup_non_atomic_partial_write(&write_path, dst_store).await;
            Err(e)
        }
    }
}

/// Fetch `path` from `src_store`, write to `dst_store`, optionally verifying
/// the content and recording XDR parse results into `manager`.
///
/// Used by mirror's `process_object` (always called) and repair's
/// `process_object` (called when dst is missing or corrupt). The branching
/// matches the trait's existing pattern:
/// - bucket file + manager → [`crate::verify::verify_and_write_bucket`]
/// - xdr per-cp file + manager → [`verify_and_write_xdr`] + record into manager
/// - everything else (and the manager-off path) → plain
///   [`crate::storage::Storage::copy_from_reader`]
///
/// Every branch writes via a temp artifact (`.tmp` sibling on non-atomic
/// backends, the backend's own temp-to-target rename on atomic ones) and
/// cleans up after itself on failure — a failed fetch never disturbs a
/// pre-existing file at `path`. Repair's failed-list mode relies on this:
/// it force-fetches listed files that may still be valid on dst, and a
/// transient src failure must not destroy that copy.
pub async fn fetch_verify_and_write(
    src_store: &StorageRef,
    dst_store: &StorageRef,
    path: &str,
    manager: Option<&XdrVerificationManager>,
) -> Result<(), StorageError> {
    let reader = src_store.open_reader(path).await?;

    let write_result: Result<Option<XdrParseResult>, StorageError> = if manager.is_some() {
        if history_format::is_bucket_file(path) {
            crate::verify::verify_and_write_bucket(path, reader, dst_store)
                .await
                .map(|()| None)
        } else if history_format::is_ledger_header_file(path)
            || history_format::is_transactions_file(path)
            || history_format::is_results_file(path)
            || history_format::is_scp_file(path)
        {
            verify_and_write_xdr(path, reader, dst_store)
                .await
                .map(Some)
        } else {
            dst_store
                .copy_from_reader(path, reader)
                .await
                .map(|()| None)
        }
    } else {
        dst_store
            .copy_from_reader(path, reader)
            .await
            .map(|()| None)
    };

    if let (Some(parsed), Some(mgr)) = (write_result?, manager) {
        mgr.record(path, parsed);
    }
    Ok(())
}
