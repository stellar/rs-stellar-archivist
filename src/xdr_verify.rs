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
pub(crate) const EMPTY_XDR_ARRAY_HASH: [u8; 32] = [
    0xdf, 0x3f, 0x61, 0x98, 0x04, 0xa9, 0x2f, 0xdb, 0x40, 0x57, 0x19, 0x2d, 0xc4, 0x3d, 0xd7, 0x48,
    0xea, 0x77, 0x8a, 0xdc, 0x52, 0xbc, 0x49, 0x8c, 0xe8, 0x05, 0x24, 0xc0, 0x14, 0xb8, 0x11, 0x19,
];

const DECOMPRESS_BUFFER_SIZE: usize = 64 * 1024;
const CHANNEL_CAPACITY: usize = 64;

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

/// Hashes extracted from a single ledger header entry, keyed by ledger sequence.
/// Produced by [`parse_ledger_entries`], consumed by [`XdrVerificationManager`] for
/// cross-file and hash-chain verification.
#[derive(Debug, Clone)]
pub struct LedgerVerificationData {
    /// `SHA256(entry.header.to_xdr())` — verified to equal `entry.hash` at parse time.
    pub computed_hash: [u8; 32],
    /// `entry.header.previous_ledger_hash` — checked against prior ledger's `computed_hash`
    /// during intra-checkpoint and cross-checkpoint chain verification.
    pub prev_hash: [u8; 32],
    /// `entry.header.scp_value.tx_set_hash` — cross-verified against the hash computed
    /// from the transaction file for the same ledger sequence.
    pub expected_tx_set_hash: [u8; 32],
    /// `entry.header.tx_set_result_hash` — cross-verified against the hash computed
    /// from the result file for the same ledger sequence.
    pub expected_result_hash: [u8; 32],
}

#[derive(Default)]
struct PendingCheckpoint {
    ledger_data: Option<BTreeMap<u32, LedgerVerificationData>>,
    tx_set_hashes: Option<HashMap<u32, [u8; 32]>>,
    result_hashes: Option<HashMap<u32, [u8; 32]>>,
}

#[derive(Debug, Clone)]
struct CheckpointBoundary {
    first_prev_hash: [u8; 32],
    last_computed_hash: [u8; 32],
}

/// A verification failure detected during cross-file or chain validation.
/// Accumulated by [`XdrVerificationManager`] and retrieved via
/// [`get_errors`](XdrVerificationManager::get_errors).
#[derive(Debug, Clone)]
pub struct VerificationError {
    pub checkpoint: u32,
    /// The specific ledger sequence that failed, or `None` for checkpoint-level errors
    /// (e.g. missing ledger data, internal errors).
    pub ledger_seq: Option<u32>,
    pub message: String,
}

/// Coordinates cross-file XDR verification across concurrent checkpoint processing.
///
/// **Usage flow:**
/// 1. For each checkpoint, record parsed data via `record_ledger_data`,
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
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            boundaries: Mutex::new(BTreeMap::new()),
            errors: Mutex::new(Vec::new()),
        }
    }

    pub fn record_ledger_data(&self, checkpoint: u32, data: BTreeMap<u32, LedgerVerificationData>) {
        let mut pending = self.pending.lock().unwrap();
        let entry = pending.entry(checkpoint).or_default();
        entry.ledger_data = Some(data);
    }

    pub fn record_tx_set_hashes(&self, checkpoint: u32, hashes: HashMap<u32, [u8; 32]>) {
        let mut pending = self.pending.lock().unwrap();
        let entry = pending.entry(checkpoint).or_default();
        entry.tx_set_hashes = Some(hashes);
    }

    pub fn record_result_hashes(&self, checkpoint: u32, hashes: HashMap<u32, [u8; 32]>) {
        let mut pending = self.pending.lock().unwrap();
        let entry = pending.entry(checkpoint).or_default();
        entry.result_hashes = Some(hashes);
    }

    /// Run all intra-checkpoint verifications and free the pending data.
    ///
    /// Runs (in order): completeness check, tx set hash cross-check, result hash
    /// cross-check, internal hash chain, then stores the first/last boundary hashes
    /// for later cross-checkpoint verification. Errors are accumulated, not returned —
    /// retrieve via [`get_errors`](Self::get_errors).
    ///
    /// No-op if no data was recorded for this checkpoint. If ledger data is missing
    /// but tx/result data exists, records a warning and skips all checks.
    pub fn verify_and_release(&self, checkpoint: u32) {
        let data = {
            let mut pending = self.pending.lock().unwrap();
            pending.remove(&checkpoint)
        };

        let Some(data) = data else {
            return;
        };

        let Some(ledger_data) = data.ledger_data else {
            let err_msg = "missing ledger verification data for checkpoint";
            warn!("Checkpoint {checkpoint}: skipping cross-verification because {err_msg}");
            self.errors.lock().unwrap().push(VerificationError {
                checkpoint,
                ledger_seq: None,
                message: err_msg.to_string(),
            });
            return;
        };

        self.verify_checkpoint_completeness(checkpoint, &ledger_data);

        if let Some(tx_set_hashes) = data.tx_set_hashes {
            self.verify_tx_set_hashes_internal(checkpoint, &ledger_data, &tx_set_hashes);
        }

        if let Some(result_hashes) = data.result_hashes {
            self.verify_result_hashes_internal(checkpoint, &ledger_data, &result_hashes);
        }

        self.verify_internal_chain(checkpoint, &ledger_data);
        self.store_boundary(checkpoint, &ledger_data);
    }

    // verifies entries in the `ledger_data` matches exactly to the ledgers in
    // this `checkpoint`
    fn verify_checkpoint_completeness(
        &self,
        checkpoint: u32,
        ledger_data: &BTreeMap<u32, LedgerVerificationData>,
    ) {
        let (first_ledger, last_ledger) = expected_ledger_range(checkpoint);
        let range = first_ledger..=last_ledger;
        let fmt_list = |seqs: &[u32]| {
            seqs.iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let unexpected: Vec<u32> = ledger_data
            .keys()
            .copied()
            .filter(|seq| !range.contains(seq))
            .collect();

        if !unexpected.is_empty() {
            let err_msg = format!(
                "unexpected ledger entries outside range {first_ledger}-{last_ledger}: {}",
                fmt_list(&unexpected),
            );
            error!("Checkpoint {checkpoint}: {err_msg}");
            self.errors.lock().unwrap().push(VerificationError {
                checkpoint,
                ledger_seq: unexpected.first().copied(),
                message: err_msg,
            });
        }

        let missing: Vec<u32> = range.filter(|seq| !ledger_data.contains_key(seq)).collect();

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
                "missing {} of {} ledger entries (ledgers {first_ledger}-{last_ledger}): {list_str}",
                missing.len(),
                last_ledger - first_ledger + 1,
            );
            error!("Checkpoint {checkpoint}: {err_msg}");
            self.errors.lock().unwrap().push(VerificationError {
                checkpoint,
                ledger_seq: missing.first().copied(),
                message: err_msg,
            });
        }
    }

    fn verify_tx_set_hashes_internal(
        &self,
        checkpoint: u32,
        ledger_data: &BTreeMap<u32, LedgerVerificationData>,
        tx_set_hashes: &HashMap<u32, [u8; 32]>,
    ) {
        for (&seq, data) in ledger_data {
            let expected = data.expected_tx_set_hash;

            let err_msg = if let Some(&actual) = tx_set_hashes.get(&seq) {
                if actual == expected {
                    None
                } else {
                    Some(format!(
                        "tx set hash mismatch: expected {}, got {}",
                        hex::encode(expected),
                        hex::encode(actual),
                    ))
                }
            } else if data.expected_result_hash != EMPTY_XDR_ARRAY_HASH
                && !is_empty_tx_set_hash(&expected, &data.prev_hash)
            {
                Some(format!(
                    "missing tx set entry, expected hash {}",
                    hex::encode(expected),
                ))
            } else {
                None
            };

            if let Some(err_msg) = err_msg {
                error!("Ledger {seq}: {err_msg}");
                self.errors.lock().unwrap().push(VerificationError {
                    checkpoint,
                    ledger_seq: Some(seq),
                    message: err_msg,
                });
            }
        }
    }

    fn verify_result_hashes_internal(
        &self,
        checkpoint: u32,
        ledger_data: &BTreeMap<u32, LedgerVerificationData>,
        result_hashes: &HashMap<u32, [u8; 32]>,
    ) {
        for (&seq, data) in ledger_data {
            let expected = data.expected_result_hash;

            let err_msg = if let Some(&actual) = result_hashes.get(&seq) {
                if actual == expected {
                    None
                } else {
                    Some(format!(
                        "result set hash mismatch: expected {}, got {}",
                        hex::encode(expected),
                        hex::encode(actual),
                    ))
                }
            } else if expected != EMPTY_XDR_ARRAY_HASH && expected != [0; 32] {
                Some(format!(
                    "missing result entry, expected hash {}",
                    hex::encode(expected),
                ))
            } else {
                None
            };

            if let Some(err_msg) = err_msg {
                error!("Ledger {seq}: {err_msg}");
                self.errors.lock().unwrap().push(VerificationError {
                    checkpoint,
                    ledger_seq: Some(seq),
                    message: err_msg,
                });
            }
        }
    }

    fn verify_internal_chain(
        &self,
        checkpoint: u32,
        ledger_data: &BTreeMap<u32, LedgerVerificationData>,
    ) {
        let entries: Vec<_> = ledger_data.iter().collect();

        for pair in entries.windows(2) {
            let mut err_msg: Option<String> = None;
            let mut ledger_seq: Option<u32> = None;

            if let [(&prev_seq, prev_data), (&seq, data)] = pair {
                if prev_seq.saturating_add(1) != seq {
                    err_msg = Some(format!(
                        "missing predecessor ledger {}",
                        seq.saturating_sub(1),
                    ));
                    ledger_seq = Some(seq);
                } else if prev_data.computed_hash != data.prev_hash {
                    err_msg = Some(format!(
                        "hash chain break: previous_ledger_hash {} != computed hash of ledger {} ({})",
                        hex::encode(data.prev_hash),
                        prev_seq,
                        hex::encode(prev_data.computed_hash),
                    ));
                    ledger_seq = Some(seq);
                }
            } else {
                err_msg = Some(
                    "internal error: unexpected window size in chain verification".to_string(),
                );
            }

            if let Some(err_msg) = err_msg {
                error!("Checkpoint {checkpoint}: {err_msg}");
                self.errors.lock().unwrap().push(VerificationError {
                    checkpoint,
                    ledger_seq,
                    message: err_msg,
                });
            }
        }
    }

    fn store_boundary(&self, checkpoint: u32, ledger_data: &BTreeMap<u32, LedgerVerificationData>) {
        let (Some((_, first_data)), Some((_, last_data))) =
            (ledger_data.first_key_value(), ledger_data.last_key_value())
        else {
            return;
        };

        self.boundaries.lock().unwrap().insert(
            checkpoint,
            CheckpointBoundary {
                first_prev_hash: first_data.prev_hash,
                last_computed_hash: last_data.computed_hash,
            },
        );
    }

    /// Verify hash chain continuity across consecutive checkpoint boundaries.
    /// Returns errors directly (not accumulated in `get_errors`) since this runs
    /// after all per-checkpoint verification is complete.
    ///
    /// Only checks adjacent checkpoints separated by exactly `CHECKPOINT_FREQUENCY` —
    /// non-consecutive checkpoints (from partial or bounded scans) are skipped.
    pub fn verify_checkpoint_chain(&self) -> Vec<VerificationError> {
        let boundaries = self.boundaries.lock().unwrap();
        let mut chain_errors = Vec::new();

        let mut checkpoints: Vec<_> = boundaries.keys().copied().collect();
        checkpoints.sort_unstable();

        for window in checkpoints.windows(2) {
            let prev_checkpoint = window[0];
            let curr_checkpoint = window[1];

            if curr_checkpoint != prev_checkpoint + CHECKPOINT_FREQUENCY {
                continue;
            }

            let prev_boundary = &boundaries[&prev_checkpoint];
            let curr_boundary = &boundaries[&curr_checkpoint];

            if curr_boundary.first_prev_hash != prev_boundary.last_computed_hash {
                let first_ledger = curr_checkpoint.saturating_sub(63);
                let err_msg = format!(
                    "hash chain break between checkpoints {prev_checkpoint} and {curr_checkpoint}: \
                     ledger {first_ledger} prev_hash {} != checkpoint {prev_checkpoint} last hash {}",
                    hex::encode(curr_boundary.first_prev_hash),
                    hex::encode(prev_boundary.last_computed_hash),
                );
                error!("{err_msg}");
                chain_errors.push(VerificationError {
                    checkpoint: curr_checkpoint,
                    ledger_seq: Some(first_ledger),
                    message: err_msg,
                });
            }
        }

        chain_errors
    }

    pub fn get_errors(&self) -> Vec<VerificationError> {
        self.errors.lock().unwrap().clone()
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
///
/// Returns a fatal error on hash mismatch, duplicate sequence, or malformed XDR.
/// Empty input returns an empty map (valid for checkpoints with no ledger file).
pub fn parse_ledger_entries(
    decompressed_data: &[u8],
) -> Result<BTreeMap<u32, LedgerVerificationData>, StorageError> {
    parse_ledger_entries_for_checkpoint(decompressed_data, None)
}

pub(crate) fn parse_ledger_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<BTreeMap<u32, LedgerVerificationData>, StorageError> {
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
                "duplicate ledger entry for seq {}",
                seq
            )));
        }

        let header_xdr = entry.header.to_xdr(Limits::none()).map_err(|e| {
            StorageError::fatal(format!(
                "failed to serialize ledger header for seq {}: {}",
                seq, e
            ))
        })?;

        let computed_hash: [u8; 32] = Sha256::digest(&header_xdr).into();
        let expected_hash: [u8; 32] = entry.hash.0;

        if computed_hash != expected_hash {
            return Err(StorageError::fatal(format!(
                "ledger header hash mismatch at seq {}: expected {}, computed {}",
                seq,
                hex::encode(expected_hash),
                hex::encode(computed_hash)
            )));
        }

        debug!(
            "Verified ledger {} hash: {}",
            seq,
            hex::encode(computed_hash)
        );

        let prev_hash: [u8; 32] = entry.header.previous_ledger_hash.0;
        let tx_set_hash: [u8; 32] = entry.header.scp_value.tx_set_hash.0;
        let result_hash: [u8; 32] = entry.header.tx_set_result_hash.0;

        data.insert(
            seq,
            LedgerVerificationData {
                computed_hash,
                prev_hash,
                expected_tx_set_hash: tx_set_hash,
                expected_result_hash: result_hash,
            },
        );
    }

    Ok(data)
}

pub(crate) fn compute_empty_v0_tx_set_hash(previous_ledger_hash: &[u8; 32]) -> [u8; 32] {
    Sha256::digest(previous_ledger_hash).into()
}

pub(crate) fn compute_empty_v1_tx_set_hash(previous_ledger_hash: &[u8; 32]) -> [u8; 32] {
    let empty_v1 = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: Hash(*previous_ledger_hash),
        phases: VecM::default(),
    });
    let xdr = empty_v1
        .to_xdr(Limits::none())
        .expect("serializing empty GeneralizedTransactionSet should not fail");
    Sha256::digest(&xdr).into()
}

pub(crate) fn is_empty_tx_set_hash(expected: &[u8; 32], prev_hash: &[u8; 32]) -> bool {
    *expected == [0; 32]
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
) -> Result<[u8; 32], StorageError> {
    let mut serialized_txs = Vec::with_capacity(tx_set.txs.len());
    for tx in tx_set.txs.iter() {
        let tx_xdr = tx.to_xdr(Limits::none()).map_err(|e| {
            StorageError::fatal(format!("failed to serialize TransactionEnvelope: {}", e))
        })?;
        let tx_hash: [u8; 32] = Sha256::digest(&tx_xdr).into();
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
    Ok(hasher.finalize().into())
}

/// Compute the hash of a V1 GeneralizedTransactionSet.
///
/// The V1 hash is simply SHA256 of the entire XDR-serialized struct.
/// This matches stellar-core's `xdrSha256(xdrTxSet)`.
pub(crate) fn compute_v1_tx_set_hash(
    generalized_tx_set: &stellar_xdr::curr::GeneralizedTransactionSet,
) -> Result<[u8; 32], StorageError> {
    let xdr = generalized_tx_set.to_xdr(Limits::none()).map_err(|e| {
        StorageError::fatal(format!(
            "failed to serialize GeneralizedTransactionSet: {}",
            e
        ))
    })?;

    Ok(Sha256::digest(&xdr).into())
}

/// Parse decompressed result XDR data, computing `SHA256(tx_result_set.to_xdr())`
/// per ledger. These hashes are cross-verified against `expected_result_hash` from
/// the ledger headers.
///
/// Returns a fatal error on duplicate sequence, out-of-range sequence, or malformed XDR.
pub fn parse_result_entries(
    decompressed_data: &[u8],
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    parse_result_entries_for_checkpoint(decompressed_data, None)
}

pub(crate) fn parse_result_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());
    let mut hashes = HashMap::new();

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

        let computed_hash: [u8; 32] = Sha256::digest(&result_xdr).into();

        debug!(
            "Computed result hash for ledger {}: {}",
            seq,
            hex::encode(computed_hash)
        );

        hashes.insert(seq, computed_hash);
    }

    Ok(hashes)
}

/// Decompress gzip data from a reader into an in-memory buffer.
pub(crate) async fn decompress_to_buffer(
    path: &str,
    reader: Reader,
) -> Result<Vec<u8>, StorageError> {
    // the writer is None, thus the return sink is also None, which is safe to
    // be discarded
    let (decompressed, _) = decompress_and_write_internal(path, reader, None).await?;
    Ok(decompressed)
}

/// Decompress a gzipped ledger file from a reader and parse it.
/// Infers the checkpoint number from the file path for range validation.
pub async fn parse_ledger_stream(
    path: &str,
    reader: Reader,
) -> Result<BTreeMap<u32, LedgerVerificationData>, StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_ledger_entries_for_checkpoint(&decompressed, history_format::checkpoint_from_path(path))
}

/// Decompress a gzipped result file from a reader and parse it.
/// Infers the checkpoint number from the file path for range validation.
pub async fn parse_results_stream(
    path: &str,
    reader: Reader,
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_result_entries_for_checkpoint(&decompressed, history_format::checkpoint_from_path(path))
}

/// Parse decompressed transaction XDR data, computing content hashes per ledger.
/// V0 entries: `SHA256(prev_hash || tx1_xdr || ... || txN_xdr)`.
/// V1 entries: `SHA256(GeneralizedTransactionSet.to_xdr())`.
/// These hashes are cross-verified against `expected_tx_set_hash` from the ledger headers.
///
/// Returns a fatal error on duplicate sequence, out-of-range sequence, malformed XDR,
/// or V0 transactions not in hash-sorted order.
pub fn parse_transaction_entries(
    decompressed_data: &[u8],
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    parse_transaction_entries_for_checkpoint(decompressed_data, None)
}

pub(crate) fn parse_transaction_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());
    let mut hashes = HashMap::new();

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
            hex::encode(computed_hash)
        );

        hashes.insert(seq, computed_hash);
    }

    Ok(hashes)
}

/// Validate SCP history XDR frame structure. Only checks that frames deserialize
/// correctly — no hashes are computed or returned. Fatal error on malformed XDR.
pub fn parse_scp_entries(decompressed_data: &[u8]) -> Result<(), StorageError> {
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
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_transaction_entries_for_checkpoint(
        &decompressed,
        history_format::checkpoint_from_path(path),
    )
}

/// Decompress a gzipped SCP file from a reader and validate its frame structure.
pub async fn parse_scp_stream(path: &str, reader: Reader) -> Result<(), StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_scp_entries(&decompressed)
}

/// Result of parsing an XDR archive file during a verified mirror write.
/// Carries computed hashes for the caller to feed into [`XdrVerificationManager`].
/// `None` is returned for SCP files and unrecognized file types.
pub enum XdrParseResult {
    Ledger(BTreeMap<u32, LedgerVerificationData>),
    Transactions(HashMap<u32, [u8; 32]>),
    Results(HashMap<u32, [u8; 32]>),
    None,
}

async fn decompress_and_write_internal(
    path: &str,
    reader: Reader,
    writer: Option<Writer>,
) -> Result<(Vec<u8>, Option<opendal::BufferSink>), StorageError> {
    use futures_util::SinkExt;

    let stream = reader
        .into_stream(..)
        .await
        .map_err(|e| from_opendal_error(e, &format!("failed to create stream for {}", path)))?;

    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_CAPACITY);

    let decompress_task = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let stream = stream.map(Ok::<_, std::io::Error>);
        let stream_reader = StreamReader::new(stream);
        let mut decoder = GzipDecoder::new(BufReader::new(stream_reader));

        let mut decompressed = Vec::new();
        let mut buf = vec![0u8; DECOMPRESS_BUFFER_SIZE];

        loop {
            let n = decoder.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            decompressed.extend_from_slice(&buf[..n]);
        }

        Ok::<_, std::io::Error>(decompressed)
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
        decompress_task.abort();
        return Err(err);
    }

    let decompressed = decompress_task
        .await
        .map_err(|e| StorageError::fatal(format!("decompress task panicked for {}: {}", path, e)))?
        .map_err(|e| StorageError::retry(format!("failed to decompress {}: {}", path, e)))?;

    // Return the unclosed sink — caller is responsible for closing it only after
    // verification succeeds, preventing corrupt data from being committed on
    // atomic backends.
    Ok((decompressed, sink))
}

/// On non-atomic backends, data sent via `sink.send()` is written directly to
/// the target path. If the write is abandoned (sink dropped without close),
/// the partial file must be removed. On atomic backends the sink drop alone
/// prevents the temp-to-target rename, so no file cleanup is needed.
async fn cleanup_non_atomic_partial_write(path: &str, dst_store: &StorageRef) {
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

/// Decompress, verify XDR structure, and write to destination in a single streaming pass.
/// The file type is determined from `path` (ledger, transaction, result, or SCP).
/// Returns the parsed hashes for the caller to record with [`XdrVerificationManager`].
pub async fn verify_and_write_xdr(
    path: &str,
    reader: Reader,
    dst_store: &StorageRef,
) -> Result<XdrParseResult, StorageError> {
    use futures_util::SinkExt;

    let writer = dst_store.open_writer(path).await?;
    let (decompressed, sink) = match decompress_and_write_internal(path, reader, Some(writer)).await
    {
        Ok(result) => result,
        Err(e) => {
            // Decompression or I/O error — sink was dropped inside
            // decompress_and_write_internal without close. Clean up any
            // partial data on non-atomic backends.
            cleanup_non_atomic_partial_write(path, dst_store).await;
            return Err(e);
        }
    };

    let parse_result = if crate::history_format::is_ledger_file(path) {
        parse_ledger_entries_for_checkpoint(
            &decompressed,
            history_format::checkpoint_from_path(path),
        )
        .map(XdrParseResult::Ledger)
    } else if crate::history_format::is_results_file(path) {
        parse_result_entries_for_checkpoint(
            &decompressed,
            history_format::checkpoint_from_path(path),
        )
        .map(XdrParseResult::Results)
    } else if crate::history_format::is_transactions_file(path) {
        parse_transaction_entries_for_checkpoint(
            &decompressed,
            history_format::checkpoint_from_path(path),
        )
        .map(XdrParseResult::Transactions)
    } else if crate::history_format::is_scp_file(path) {
        parse_scp_entries(&decompressed).map(|()| XdrParseResult::None)
    } else {
        Ok(XdrParseResult::None)
    };

    match parse_result {
        Ok(result) => {
            // Verification passed — close sink to commit the write
            if let Some(mut s) = sink {
                s.close()
                    .await
                    .map_err(|e| from_opendal_error(e, &format!("failed to close {}", path)))?;
            }
            Ok(result)
        }
        Err(err) => {
            // Verification failed — don't close sink, prevents committing corrupt data
            // on atomic backends. For non-atomic backends, data is already on disk via
            // send() so clean up the partial file.
            cleanup_non_atomic_partial_write(path, dst_store).await;
            Err(err)
        }
    }
}
