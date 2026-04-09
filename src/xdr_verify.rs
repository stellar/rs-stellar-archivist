//! XDR verification module.
//!
//! Provides hash verification for ledger, transaction, and result files.
//! Cross-file verification validates that:
//! - Ledger entry.hash == SHA256(entry.header.to_xdr())
//! - Transaction set hash matches ledger header's scp_value.tx_set_hash
//! - Result set hash matches ledger header's tx_set_result_hash
//! - Ledger hash chain: ledger[N].previous_ledger_hash == hash(ledger[N-1])

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

#[derive(Debug, Clone)]
pub struct LedgerVerificationData {
    pub computed_hash: [u8; 32],
    pub prev_hash: [u8; 32],
    pub expected_tx_set_hash: [u8; 32],
    pub expected_result_hash: [u8; 32],
}

#[derive(Default)]
struct PendingCheckpoint {
    ledger_data: Option<HashMap<u32, LedgerVerificationData>>,
    tx_set_hashes: Option<HashMap<u32, [u8; 32]>>,
    result_hashes: Option<HashMap<u32, [u8; 32]>>,
}

#[derive(Debug, Clone)]
struct CheckpointBoundary {
    first_prev_hash: [u8; 32],
    last_computed_hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct VerificationError {
    pub checkpoint: u32,
    pub ledger_seq: Option<u32>,
    pub message: String,
}

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

    pub fn record_ledger_data(&self, checkpoint: u32, data: HashMap<u32, LedgerVerificationData>) {
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

    pub fn verify_and_release(&self, checkpoint: u32) {
        let data = {
            let mut pending = self.pending.lock().unwrap();
            pending.remove(&checkpoint)
        };

        let Some(data) = data else {
            return;
        };

        let Some(ledger_data) = data.ledger_data else {
            warn!(
                "Checkpoint {}: skipping cross-verification because ledger verification data is missing",
                checkpoint
            );
            self.errors.lock().unwrap().push(VerificationError {
                checkpoint,
                ledger_seq: None,
                message: "missing ledger verification data for checkpoint".to_string(),
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

    fn verify_checkpoint_completeness(
        &self,
        checkpoint: u32,
        ledger_data: &HashMap<u32, LedgerVerificationData>,
    ) {
        let (first_ledger, last_ledger) = expected_ledger_range(checkpoint);
        let expected_count = (last_ledger - first_ledger + 1) as usize;

        let unexpected: Vec<u32> = ledger_data
            .keys()
            .copied()
            .filter(|seq| !(first_ledger..=last_ledger).contains(seq))
            .collect();

        if !unexpected.is_empty() {
            error!(
                "Checkpoint {}: found ledger entries outside expected range {}-{}: {}",
                checkpoint,
                first_ledger,
                last_ledger,
                unexpected
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            self.errors.lock().unwrap().push(VerificationError {
                checkpoint,
                ledger_seq: unexpected.first().copied(),
                message: format!(
                    "unexpected ledger entries outside range {}-{}: {}",
                    first_ledger,
                    last_ledger,
                    unexpected
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }

        let mut missing: Vec<u32> = Vec::new();
        for seq in first_ledger..=last_ledger {
            if !ledger_data.contains_key(&seq) {
                missing.push(seq);
            }
        }

        if !missing.is_empty() {
            let missing_str = if missing.len() <= 10 {
                missing
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                format!(
                    "{}, ... ({} more)",
                    missing[..5]
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    missing.len() - 5
                )
            };

            error!(
                "Checkpoint {}: missing {} ledger entries (expected {} entries for ledgers {}-{}): {}",
                checkpoint,
                missing.len(),
                expected_count,
                first_ledger,
                last_ledger,
                missing_str
            );

            self.errors.lock().unwrap().push(VerificationError {
                checkpoint,
                ledger_seq: missing.first().copied(),
                message: format!(
                    "missing {} ledger entries (expected ledgers {}-{}): {}",
                    missing.len(),
                    first_ledger,
                    last_ledger,
                    missing_str
                ),
            });
        }
    }

    fn verify_tx_set_hashes_internal(
        &self,
        checkpoint: u32,
        ledger_data: &HashMap<u32, LedgerVerificationData>,
        tx_set_hashes: &HashMap<u32, [u8; 32]>,
    ) {
        for (&seq, data) in ledger_data {
            let expected = data.expected_tx_set_hash;

            if let Some(&actual) = tx_set_hashes.get(&seq) {
                if actual != expected {
                    error!(
                        "Ledger {}: tx set hash mismatch: expected {}, got {}",
                        seq,
                        hex::encode(expected),
                        hex::encode(actual)
                    );
                    self.errors.lock().unwrap().push(VerificationError {
                        checkpoint,
                        ledger_seq: Some(seq),
                        message: format!(
                            "tx set hash mismatch: expected {}, got {}",
                            hex::encode(expected),
                            hex::encode(actual)
                        ),
                    });
                }
            } else if data.expected_result_hash != EMPTY_XDR_ARRAY_HASH
                && !is_empty_tx_set_hash(&expected, &data.prev_hash)
            {
                error!(
                    "Ledger {}: missing tx set entry, expected hash {}",
                    seq,
                    hex::encode(expected)
                );
                self.errors.lock().unwrap().push(VerificationError {
                    checkpoint,
                    ledger_seq: Some(seq),
                    message: format!(
                        "missing tx set entry, expected hash {}",
                        hex::encode(expected)
                    ),
                });
            }
        }
    }

    fn verify_result_hashes_internal(
        &self,
        checkpoint: u32,
        ledger_data: &HashMap<u32, LedgerVerificationData>,
        result_hashes: &HashMap<u32, [u8; 32]>,
    ) {
        for (&seq, data) in ledger_data {
            let expected = data.expected_result_hash;

            if let Some(&actual) = result_hashes.get(&seq) {
                if actual != expected {
                    error!(
                        "Ledger {}: result set hash mismatch: expected {}, got {}",
                        seq,
                        hex::encode(expected),
                        hex::encode(actual)
                    );
                    self.errors.lock().unwrap().push(VerificationError {
                        checkpoint,
                        ledger_seq: Some(seq),
                        message: format!(
                            "result set hash mismatch: expected {}, got {}",
                            hex::encode(expected),
                            hex::encode(actual)
                        ),
                    });
                }
            } else if expected != EMPTY_XDR_ARRAY_HASH && expected != [0; 32] {
                error!(
                    "Ledger {}: missing result entry, expected hash {}",
                    seq,
                    hex::encode(expected)
                );
                self.errors.lock().unwrap().push(VerificationError {
                    checkpoint,
                    ledger_seq: Some(seq),
                    message: format!(
                        "missing result entry, expected hash {}",
                        hex::encode(expected)
                    ),
                });
            }
        }
    }

    fn verify_internal_chain(
        &self,
        checkpoint: u32,
        ledger_data: &HashMap<u32, LedgerVerificationData>,
    ) {
        if ledger_data.is_empty() {
            return;
        }

        let (first_expected, _) = expected_ledger_range(checkpoint);

        for (&seq, data) in ledger_data {
            let prev_seq = seq - 1;

            if prev_seq < first_expected {
                continue;
            }

            if let Some(prev_data) = ledger_data.get(&prev_seq) {
                if prev_data.computed_hash != data.prev_hash {
                    error!(
                        "Ledger {}: hash chain break: previous_ledger_hash {} != computed hash of ledger {} ({})",
                        seq,
                        hex::encode(data.prev_hash),
                        prev_seq,
                        hex::encode(prev_data.computed_hash)
                    );
                    self.errors.lock().unwrap().push(VerificationError {
                        checkpoint,
                        ledger_seq: Some(seq),
                        message: format!(
                            "hash chain break: previous_ledger_hash {} != computed hash of ledger {} ({})",
                            hex::encode(data.prev_hash),
                            prev_seq,
                            hex::encode(prev_data.computed_hash)
                        ),
                    });
                }
            } else {
                error!(
                    "Ledger {}: missing predecessor ledger {} in checkpoint file",
                    seq, prev_seq
                );
                self.errors.lock().unwrap().push(VerificationError {
                    checkpoint,
                    ledger_seq: Some(seq),
                    message: format!("missing predecessor ledger {}", prev_seq),
                });
            }
        }
    }

    fn store_boundary(&self, checkpoint: u32, ledger_data: &HashMap<u32, LedgerVerificationData>) {
        if ledger_data.is_empty() {
            return;
        }

        let min_seq = *ledger_data.keys().min().unwrap();
        let max_seq = *ledger_data.keys().max().unwrap();

        let first_data = &ledger_data[&min_seq];
        let last_data = &ledger_data[&max_seq];

        self.boundaries.lock().unwrap().insert(
            checkpoint,
            CheckpointBoundary {
                first_prev_hash: first_data.prev_hash,
                last_computed_hash: last_data.computed_hash,
            },
        );
    }

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
                error!(
                    "Hash chain break between checkpoints {} and {}: ledger {} prev_hash {} != checkpoint {} last hash {}",
                    prev_checkpoint,
                    curr_checkpoint,
                    first_ledger,
                    hex::encode(curr_boundary.first_prev_hash),
                    prev_checkpoint,
                    hex::encode(prev_boundary.last_computed_hash)
                );
                chain_errors.push(VerificationError {
                    checkpoint: curr_checkpoint,
                    ledger_seq: Some(first_ledger),
                    message: format!(
                        "hash chain break between checkpoints {} and {}",
                        prev_checkpoint, curr_checkpoint
                    ),
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

pub fn parse_ledger_entries(
    decompressed_data: &[u8],
) -> Result<HashMap<u32, LedgerVerificationData>, StorageError> {
    parse_ledger_entries_for_checkpoint(decompressed_data, None)
}

pub(crate) fn parse_ledger_entries_for_checkpoint(
    decompressed_data: &[u8],
    checkpoint: Option<u32>,
) -> Result<HashMap<u32, LedgerVerificationData>, StorageError> {
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());
    let mut data = HashMap::new();

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

/// Parse and validate transaction file XDR structure with hash verification.
///
/// For each TransactionHistoryEntry:
/// - V0 (ext == V0): Hash = SHA256(previous_ledger_hash || tx1_xdr || ... || txN_xdr)
/// - V1 (ext == V1): Hash = SHA256(GeneralizedTransactionSet.to_xdr())
///
/// Records computed hashes for cross-verification against ledger headers.
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
pub async fn decompress_to_buffer(path: &str, reader: Reader) -> Result<Vec<u8>, StorageError> {
    // the writer is None, thus the return sink is also None, which is safe to
    // be discarded
    let (decompressed, _) = decompress_and_write_internal(path, reader, None).await?;
    Ok(decompressed)
}

pub async fn parse_ledger_stream(
    path: &str,
    reader: Reader,
) -> Result<HashMap<u32, LedgerVerificationData>, StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_ledger_entries_for_checkpoint(&decompressed, history_format::checkpoint_from_path(path))
}

pub async fn parse_results_stream(
    path: &str,
    reader: Reader,
) -> Result<HashMap<u32, [u8; 32]>, StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_result_entries_for_checkpoint(&decompressed, history_format::checkpoint_from_path(path))
}

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

pub fn parse_scp_entries(decompressed_data: &[u8]) -> Result<(), StorageError> {
    let cursor = Cursor::new(decompressed_data);
    let mut limited = Limited::new(cursor, Limits::none());

    for result in Frame::<ScpHistoryEntry>::read_xdr_iter(&mut limited) {
        result
            .map_err(|e| StorageError::fatal(format!("failed to parse ScpHistoryEntry: {}", e)))?;
    }

    Ok(())
}

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

pub async fn parse_scp_stream(path: &str, reader: Reader) -> Result<(), StorageError> {
    let decompressed = decompress_to_buffer(path, reader).await?;
    parse_scp_entries(&decompressed)
}

pub enum XdrParseResult {
    Ledger(HashMap<u32, LedgerVerificationData>),
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

pub async fn verify_and_write_xdr(
    path: &str,
    reader: Reader,
    dst_store: &StorageRef,
) -> Result<XdrParseResult, StorageError> {
    use futures_util::SinkExt;

    let writer = dst_store.open_writer(path).await?;
    let (decompressed, sink) = decompress_and_write_internal(path, reader, Some(writer)).await?;

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
            if !dst_store.uses_atomic_writes() {
                if let Some(base_path) = dst_store.get_base_path() {
                    let file_path = base_path.join(path);
                    if let Err(remove_err) = tokio::fs::remove_file(&file_path).await {
                        if remove_err.kind() != std::io::ErrorKind::NotFound {
                            warn!(
                                "failed to remove partially written file {} after verification error: {}",
                                file_path.display(),
                                remove_err
                            );
                        }
                    }
                }
            }
            Err(err)
        }
    }
}
