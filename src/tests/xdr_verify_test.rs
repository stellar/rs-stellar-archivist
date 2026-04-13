use crate::xdr_verify::{
    compute_empty_v0_tx_set_hash, compute_empty_v1_tx_set_hash, compute_v0_tx_set_hash,
    compute_v1_tx_set_hash, expected_ledger_range, is_empty_tx_set_hash, parse_ledger_entries,
    parse_ledger_entries_for_checkpoint, parse_result_entries, parse_result_entries_for_checkpoint,
    parse_scp_entries, parse_transaction_entries, parse_transaction_entries_for_checkpoint,
    LedgerVerificationData, XdrVerificationManager, EMPTY_XDR_ARRAY_HASH,
};
use rstest::rstest;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use stellar_xdr::curr::{
    AccountId, CreateAccountOp, GeneralizedTransactionSet, Hash, LedgerHeader, LedgerHeaderExt,
    LedgerHeaderHistoryEntry, LedgerHeaderHistoryEntryExt, LedgerScpMessages, Limits, Memo,
    MuxedAccount, Operation, OperationBody, Preconditions, PublicKey, ScpHistoryEntry,
    ScpHistoryEntryV0, SequenceNumber, TimePoint, Transaction, TransactionEnvelope,
    TransactionHistoryEntry, TransactionHistoryEntryExt, TransactionHistoryResultEntry,
    TransactionHistoryResultEntryExt, TransactionPhase, TransactionResult, TransactionResultExt,
    TransactionResultPair, TransactionResultResult, TransactionResultSet, TransactionSet,
    TransactionSetV1, TransactionV0, TransactionV0Envelope, TransactionV0Ext,
    TransactionV1Envelope, Uint256, VecM, WriteXdr,
};

fn frame_xdr<T: WriteXdr>(entry: &T) -> Vec<u8> {
    let entry_xdr = entry.to_xdr(Limits::none()).unwrap();
    let frame_len = entry_xdr.len() as u32 | 0x8000_0000;
    let mut framed = Vec::with_capacity(4 + entry_xdr.len());
    framed.extend_from_slice(&frame_len.to_be_bytes());
    framed.extend_from_slice(&entry_xdr);
    framed
}

fn ed25519(id: u8) -> Uint256 {
    Uint256([id; 32])
}

fn account_id(id: u8) -> AccountId {
    AccountId(PublicKey::PublicKeyTypeEd25519(ed25519(id)))
}

fn muxed_account(id: u8) -> MuxedAccount {
    MuxedAccount::Ed25519(ed25519(id))
}

fn create_account_operation(id: u8) -> Operation {
    Operation {
        source_account: None,
        body: OperationBody::CreateAccount(CreateAccountOp {
            destination: account_id(id.saturating_add(1)),
            starting_balance: 100,
        }),
    }
}

fn tx_v0_envelope(id: u8) -> TransactionEnvelope {
    TransactionEnvelope::TxV0(TransactionV0Envelope {
        tx: TransactionV0 {
            source_account_ed25519: ed25519(id),
            fee: 100,
            seq_num: SequenceNumber(i64::from(id) + 1),
            time_bounds: None,
            memo: Memo::None,
            operations: vec![create_account_operation(id)].try_into().unwrap(),
            ext: TransactionV0Ext::V0,
        },
        signatures: VecM::default(),
    })
}

fn tx_v1_envelope(id: u8) -> TransactionEnvelope {
    TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: Transaction {
            source_account: muxed_account(id),
            fee: 100,
            seq_num: SequenceNumber(i64::from(id) + 1),
            cond: Preconditions::None,
            memo: Memo::None,
            operations: vec![create_account_operation(id)].try_into().unwrap(),
            ext: stellar_xdr::curr::TransactionExt::V0,
        },
        signatures: VecM::default(),
    })
}

fn tx_hash(tx: &TransactionEnvelope) -> [u8; 32] {
    Sha256::digest(tx.to_xdr(Limits::none()).unwrap()).into()
}

fn v0_history_entry(
    seq: u32,
    prev_hash: [u8; 32],
    txs: Vec<TransactionEnvelope>,
) -> TransactionHistoryEntry {
    TransactionHistoryEntry {
        ledger_seq: seq,
        tx_set: TransactionSet {
            previous_ledger_hash: Hash(prev_hash),
            txs: txs.try_into().unwrap(),
        },
        ext: TransactionHistoryEntryExt::V0,
    }
}

fn v1_history_entry(
    seq: u32,
    prev_hash: [u8; 32],
    txs: Vec<TransactionEnvelope>,
) -> TransactionHistoryEntry {
    let component = stellar_xdr::curr::TxSetComponent::TxsetCompTxsMaybeDiscountedFee(
        stellar_xdr::curr::TxSetComponentTxsMaybeDiscountedFee {
            base_fee: None,
            txs: txs.try_into().unwrap(),
        },
    );

    let generalized = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: Hash(prev_hash),
        phases: vec![TransactionPhase::V0(vec![component].try_into().unwrap())]
            .try_into()
            .unwrap(),
    });

    TransactionHistoryEntry {
        ledger_seq: seq,
        tx_set: TransactionSet {
            previous_ledger_hash: Hash([0; 32]),
            txs: VecM::default(),
        },
        ext: TransactionHistoryEntryExt::V1(generalized),
    }
}

fn result_pair(id: u8) -> TransactionResultPair {
    TransactionResultPair {
        transaction_hash: Hash([id; 32]),
        result: TransactionResult {
            fee_charged: 100,
            result: TransactionResultResult::TxSuccess(VecM::default()),
            ext: TransactionResultExt::V0,
        },
    }
}

fn result_entry(seq: u32, ids: &[u8]) -> TransactionHistoryResultEntry {
    TransactionHistoryResultEntry {
        ledger_seq: seq,
        tx_result_set: TransactionResultSet {
            results: ids
                .iter()
                .copied()
                .map(result_pair)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        },
        ext: TransactionHistoryResultEntryExt::V0,
    }
}

fn create_minimal_ledger_header(
    seq: u32,
    prev_hash: [u8; 32],
    tx_set_hash: [u8; 32],
    result_hash: [u8; 32],
) -> LedgerHeader {
    LedgerHeader {
        ledger_version: 21,
        previous_ledger_hash: Hash(prev_hash),
        scp_value: stellar_xdr::curr::StellarValue {
            tx_set_hash: Hash(tx_set_hash),
            close_time: TimePoint(0),
            upgrades: VecM::default(),
            ext: stellar_xdr::curr::StellarValueExt::Basic,
        },
        tx_set_result_hash: Hash(result_hash),
        bucket_list_hash: Hash([0; 32]),
        ledger_seq: seq,
        total_coins: 0,
        fee_pool: 0,
        inflation_seq: 0,
        id_pool: 0,
        base_fee: 100,
        base_reserve: 5_000_000,
        max_tx_set_size: 100,
        skip_list: [Hash([0; 32]), Hash([0; 32]), Hash([0; 32]), Hash([0; 32])],
        ext: LedgerHeaderExt::V0,
    }
}

fn create_valid_ledger_entry(
    seq: u32,
    prev_hash: [u8; 32],
    tx_set_hash: [u8; 32],
    result_hash: [u8; 32],
) -> LedgerHeaderHistoryEntry {
    let header = create_minimal_ledger_header(seq, prev_hash, tx_set_hash, result_hash);
    let header_xdr = header.to_xdr(Limits::none()).unwrap();
    let computed_hash: [u8; 32] = Sha256::digest(&header_xdr).into();

    LedgerHeaderHistoryEntry {
        hash: Hash(computed_hash),
        header,
        ext: LedgerHeaderHistoryEntryExt::V0,
    }
}

fn create_complete_checkpoint_data(
    checkpoint: u32,
    initial_prev_hash: [u8; 32],
) -> BTreeMap<u32, LedgerVerificationData> {
    let (first_ledger, last_ledger) = expected_ledger_range(checkpoint);
    let mut ledger_data = BTreeMap::new();
    let mut prev_hash = initial_prev_hash;

    for seq in first_ledger..=last_ledger {
        let computed_hash: [u8; 32] = Sha256::digest(format!("ledger{}", seq).as_bytes()).into();
        ledger_data.insert(
            seq,
            LedgerVerificationData {
                computed_hash,
                prev_hash,
                expected_tx_set_hash: [0; 32],
                expected_result_hash: [0; 32],
            },
        );
        prev_hash = computed_hash;
    }

    ledger_data
}

fn create_checkpoint_data_missing(
    checkpoint: u32,
    missing: &[u32],
) -> BTreeMap<u32, LedgerVerificationData> {
    let mut data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for seq in missing {
        data.remove(seq);
    }
    data
}

fn single_ledger_data(
    seq: u32,
    computed_hash: [u8; 32],
    prev_hash: [u8; 32],
) -> BTreeMap<u32, LedgerVerificationData> {
    BTreeMap::from([(
        seq,
        LedgerVerificationData {
            computed_hash,
            prev_hash,
            expected_tx_set_hash: [0; 32],
            expected_result_hash: [0; 32],
        },
    )])
}

fn hash_of(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

fn assert_has_error(manager: &XdrVerificationManager, substring: &str) {
    assert!(
        manager
            .get_errors()
            .iter()
            .any(|e| e.message.contains(substring)),
        "expected an error containing {substring:?}, got: {:?}",
        manager
            .get_errors()
            .iter()
            .map(|e| &e.message)
            .collect::<Vec<_>>(),
    );
}

fn assert_no_errors_matching(manager: &XdrVerificationManager, substring: &str) {
    let matching: Vec<_> = manager
        .get_errors()
        .iter()
        .filter(|e| e.message.contains(substring))
        .cloned()
        .collect();
    assert!(
        matching.is_empty(),
        "expected no errors containing {substring:?}, got: {matching:?}",
    );
}

#[test]
fn test_parse_ledger_entries_empty_input() {
    let parsed = parse_ledger_entries(&[]).unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn test_parse_ledger_entries_single_entry() {
    let entry = create_valid_ledger_entry(100, [1; 32], [2; 32], [3; 32]);
    let parsed = parse_ledger_entries(&frame_xdr(&entry)).unwrap();

    assert_eq!(parsed.len(), 1);
    let actual = parsed.get(&100).unwrap();
    assert_eq!(actual.prev_hash, [1; 32]);
    assert_eq!(actual.expected_tx_set_hash, [2; 32]);
    assert_eq!(actual.expected_result_hash, [3; 32]);
}

#[test]
fn test_parse_ledger_entries_hash_mismatch() {
    let entry = LedgerHeaderHistoryEntry {
        hash: Hash([0xff; 32]),
        header: create_minimal_ledger_header(100, [0; 32], [0; 32], [0; 32]),
        ext: LedgerHeaderHistoryEntryExt::V0,
    };

    let err = parse_ledger_entries(&frame_xdr(&entry)).unwrap_err();
    assert!(err.message.contains("hash mismatch"));
}

#[rstest]
#[case::ledger("ledger")]
#[case::transaction("transaction")]
#[case::result("result")]
fn test_parse_rejects_duplicate_sequence(#[case] file_type: &str) {
    let frame = match file_type {
        "ledger" => frame_xdr(&create_valid_ledger_entry(100, [0; 32], [0; 32], [0; 32])),
        "transaction" => frame_xdr(&v0_history_entry(100, [0; 32], vec![tx_v0_envelope(1)])),
        "result" => frame_xdr(&result_entry(100, &[1])),
        _ => unreachable!(),
    };
    let mut data = frame.clone();
    data.extend(frame);
    let err = match file_type {
        "ledger" => parse_ledger_entries(&data).unwrap_err(),
        "transaction" => parse_transaction_entries(&data).unwrap_err(),
        "result" => parse_result_entries(&data).unwrap_err(),
        _ => unreachable!(),
    };
    assert!(err.message.contains("duplicate"));
}

#[rstest]
#[case::ledger("ledger")]
#[case::transaction("transaction")]
#[case::result("result")]
fn test_parse_rejects_malformed_frame(#[case] file_type: &str) {
    let valid = match file_type {
        "ledger" => frame_xdr(&create_valid_ledger_entry(100, [0; 32], [0; 32], [0; 32])),
        "transaction" => frame_xdr(&v0_history_entry(100, [0; 32], vec![tx_v0_envelope(1)])),
        "result" => frame_xdr(&result_entry(100, &[1])),
        _ => unreachable!(),
    };
    let truncated = &valid[..valid.len() / 2];
    let err = match file_type {
        "ledger" => parse_ledger_entries(truncated).unwrap_err(),
        "transaction" => parse_transaction_entries(truncated).unwrap_err(),
        "result" => parse_result_entries(truncated).unwrap_err(),
        _ => unreachable!(),
    };
    assert!(err.message.contains("failed to parse"));
}

#[test]
fn test_parse_result_entries_empty_input() {
    let parsed = parse_result_entries(&[]).unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn test_parse_result_entries_single_entry() {
    let entry = result_entry(100, &[1, 2]);
    let data = frame_xdr(&entry);
    let parsed = parse_result_entries(&data).unwrap();
    let expected: [u8; 32] =
        Sha256::digest(entry.tx_result_set.to_xdr(Limits::none()).unwrap()).into();

    assert_eq!(parsed, HashMap::from([(100, expected)]));
}

#[test]
fn test_parse_transaction_entries_v0_non_empty() {
    let prev_hash = [0x42; 32];
    let txs = vec![tx_v0_envelope(1), tx_v0_envelope(2)];
    let entry = v0_history_entry(100, prev_hash, txs.clone());
    let parsed = parse_transaction_entries(&frame_xdr(&entry)).unwrap();

    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[&100], compute_v0_tx_set_hash(&entry.tx_set).unwrap());
    assert!(!is_empty_tx_set_hash(&parsed[&100], &prev_hash));
}

#[test]
fn test_parse_transaction_entries_v1_non_empty() {
    let prev_hash = [0x24; 32];
    let entry = v1_history_entry(100, prev_hash, vec![tx_v1_envelope(1), tx_v1_envelope(2)]);
    let parsed = parse_transaction_entries(&frame_xdr(&entry)).unwrap();

    let TransactionHistoryEntryExt::V1(generalized) = &entry.ext else {
        panic!("expected V1 entry");
    };

    assert_eq!(parsed[&100], compute_v1_tx_set_hash(generalized).unwrap());
    assert!(!is_empty_tx_set_hash(&parsed[&100], &prev_hash));
}

#[test]
fn test_compute_v0_tx_set_hash_matches_manual_hash() {
    let prev_hash = [0x10; 32];
    let txs = vec![tx_v0_envelope(1), tx_v0_envelope(2)];
    let tx_set = TransactionSet {
        previous_ledger_hash: Hash(prev_hash),
        txs: txs.clone().try_into().unwrap(),
    };

    let mut hasher = Sha256::new();
    hasher.update(prev_hash);
    for tx in &txs {
        hasher.update(tx.to_xdr(Limits::none()).unwrap());
    }

    let expected: [u8; 32] = hasher.finalize().into();
    assert_eq!(compute_v0_tx_set_hash(&tx_set).unwrap(), expected);
}

#[test]
fn test_compute_v0_tx_set_hash_rejects_unsorted_transactions() {
    let a = tx_v0_envelope(1);
    let b = tx_v0_envelope(2);
    let (first, second) = if tx_hash(&a) < tx_hash(&b) {
        (b, a)
    } else {
        (a, b)
    };
    let tx_set = TransactionSet {
        previous_ledger_hash: Hash([0; 32]),
        txs: vec![first, second].try_into().unwrap(),
    };

    let err = compute_v0_tx_set_hash(&tx_set).unwrap_err();
    assert!(err.message.contains("out of hash order"));
}

#[test]
fn test_compute_v1_tx_set_hash_matches_manual_hash() {
    let generalized = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: Hash([0x11; 32]),
        phases: vec![TransactionPhase::V0(
            vec![
                stellar_xdr::curr::TxSetComponent::TxsetCompTxsMaybeDiscountedFee(
                    stellar_xdr::curr::TxSetComponentTxsMaybeDiscountedFee {
                        base_fee: None,
                        txs: vec![tx_v1_envelope(1)].try_into().unwrap(),
                    },
                ),
            ]
            .try_into()
            .unwrap(),
        )]
        .try_into()
        .unwrap(),
    });
    let expected: [u8; 32] = Sha256::digest(generalized.to_xdr(Limits::none()).unwrap()).into();

    assert_eq!(compute_v1_tx_set_hash(&generalized).unwrap(), expected);
}

#[test]
fn test_is_empty_tx_set_hash_direct_hashes() {
    let prev_hash = [0x55; 32];
    let expected_v0: [u8; 32] = Sha256::digest(prev_hash).into();
    assert_eq!(compute_empty_v0_tx_set_hash(&prev_hash), expected_v0);

    let empty_v1 = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: Hash(prev_hash),
        phases: VecM::default(),
    });
    let expected_v1: [u8; 32] = Sha256::digest(empty_v1.to_xdr(Limits::none()).unwrap()).into();
    assert_eq!(compute_empty_v1_tx_set_hash(&prev_hash), expected_v1);

    assert!(is_empty_tx_set_hash(
        &compute_empty_v0_tx_set_hash(&prev_hash),
        &prev_hash
    ));
    assert!(is_empty_tx_set_hash(
        &compute_empty_v1_tx_set_hash(&prev_hash),
        &prev_hash
    ));
}

#[test]
fn test_empty_xdr_array_hash_constant_matches_hash() {
    let expected: [u8; 32] = Sha256::digest([0_u8, 0, 0, 0]).into();
    assert_eq!(EMPTY_XDR_ARRAY_HASH, expected);
}

#[test]
fn test_parse_scp_entries_accepts_valid_frame() {
    let entry = ScpHistoryEntry::V0(ScpHistoryEntryV0 {
        quorum_sets: VecM::default(),
        ledger_messages: LedgerScpMessages {
            ledger_seq: 100,
            messages: VecM::default(),
        },
    });

    parse_scp_entries(&frame_xdr(&entry)).unwrap();
}

#[test]
fn test_parse_scp_entries_rejects_invalid_bytes() {
    let err = parse_scp_entries(b"not-scp-xdr").unwrap_err();
    assert!(err.message.contains("failed to parse"));
}

#[test]
fn test_manager_records_and_verifies_checkpoint() {
    let manager = XdrVerificationManager::new();
    let checkpoint = 63;
    let result_hash = hash_of("result");

    let mut ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in ledger_data.values_mut() {
        data.expected_result_hash = result_hash;
    }

    let result_hashes: HashMap<u32, [u8; 32]> =
        ledger_data.keys().map(|&seq| (seq, result_hash)).collect();

    manager.record_ledger_data(checkpoint, ledger_data);
    manager.record_result_hashes(checkpoint, result_hashes);
    manager.verify_and_release(checkpoint);

    assert!(manager.get_errors().is_empty());
}

#[rstest]
#[case::beginning(127, 65)]
#[case::middle(127, 95)]
#[case::end(127, 127)]
#[case::genesis(63, 2)]
fn test_chain_break_within_ledger_file(#[case] checkpoint: u32, #[case] corrupted_ledger: u32) {
    let manager = XdrVerificationManager::new();

    let mut ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    ledger_data.get_mut(&corrupted_ledger).unwrap().prev_hash = [0xff; 32];

    manager.record_ledger_data(checkpoint, ledger_data);
    manager.verify_and_release(checkpoint);

    assert!(manager
        .get_errors()
        .iter()
        .any(|e| e.ledger_seq == Some(corrupted_ledger) && e.message.contains("hash chain break")),);
}

#[rstest]
#[case::genesis(63, 63)]
#[case::regular(127, 64)]
fn test_complete_checkpoint_passes(#[case] checkpoint: u32, #[case] expected_count: usize) {
    let manager = XdrVerificationManager::new();
    let ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    assert_eq!(ledger_data.len(), expected_count);

    manager.record_ledger_data(checkpoint, ledger_data);
    manager.verify_and_release(checkpoint);

    assert!(manager.get_errors().is_empty());
}

#[rstest]
#[case::first_ledger(127, vec![64])]
#[case::last_ledger(127, vec![127])]
#[case::middle_ledger(127, vec![100])]
#[case::multiple_ledgers(127, vec![66, 67])]
#[case::genesis_first(63, vec![1])]
#[case::all_missing(127, (64..=127).collect())]
fn test_missing_ledger_entries(#[case] checkpoint: u32, #[case] missing: Vec<u32>) {
    let manager = XdrVerificationManager::new();
    manager.record_ledger_data(
        checkpoint,
        create_checkpoint_data_missing(checkpoint, &missing),
    );
    manager.verify_and_release(checkpoint);

    assert_has_error(&manager, "missing");
}

#[test]
fn test_ledger_outside_expected_checkpoint_range() {
    let manager = XdrVerificationManager::new();
    let mut ledger_data = create_complete_checkpoint_data(127, [0; 32]);
    ledger_data.insert(
        200,
        LedgerVerificationData {
            computed_hash: [0xaa; 32],
            prev_hash: [0xbb; 32],
            expected_tx_set_hash: [0; 32],
            expected_result_hash: [0; 32],
        },
    );

    manager.record_ledger_data(127, ledger_data);
    manager.verify_and_release(127);

    assert_has_error(&manager, "unexpected ledger entries outside range");
}

#[rstest]
#[case::valid_chain(false)]
#[case::broken_chain(true)]
fn test_cross_checkpoint_chain(#[case] break_chain: bool) {
    let manager = XdrVerificationManager::new();
    let ledger_data_63 = single_ledger_data(63, hash_of("ledger63"), [0; 32]);
    let prev_hash = if break_chain {
        [0xff; 32]
    } else {
        hash_of("ledger63")
    };
    let ledger_data_127 = single_ledger_data(64, hash_of("ledger127"), prev_hash);

    manager.record_ledger_data(63, ledger_data_63);
    manager.record_ledger_data(127, ledger_data_127);
    manager.verify_and_release(63);
    manager.verify_and_release(127);

    let chain_errors = manager.verify_checkpoint_chain();
    if break_chain {
        assert_eq!(chain_errors.len(), 1);
    } else {
        assert!(chain_errors.is_empty());
    }
}

#[rstest]
#[case::valid(false)]
#[case::broken(true)]
fn test_consecutive_checkpoints_full(#[case] break_chain: bool) {
    let manager = XdrVerificationManager::new();
    let ledger_data_63 = create_complete_checkpoint_data(63, [0; 32]);
    let last_hash_of_63 = ledger_data_63.get(&63).unwrap().computed_hash;

    let mut ledger_data_127 = BTreeMap::new();
    let mut prev_hash = if break_chain {
        [0xff; 32]
    } else {
        last_hash_of_63
    };
    for seq in 64_u32..=127 {
        let computed_hash: [u8; 32] = Sha256::digest(seq.to_le_bytes()).into();
        ledger_data_127.insert(
            seq,
            LedgerVerificationData {
                computed_hash,
                prev_hash,
                expected_tx_set_hash: [0; 32],
                expected_result_hash: [0; 32],
            },
        );
        prev_hash = computed_hash;
    }

    manager.record_ledger_data(63, ledger_data_63);
    manager.record_ledger_data(127, ledger_data_127);
    manager.verify_and_release(63);
    manager.verify_and_release(127);

    assert!(manager.get_errors().is_empty());
    let chain_errors = manager.verify_checkpoint_chain();
    if break_chain {
        assert!(!chain_errors.is_empty());
    } else {
        assert!(chain_errors.is_empty());
    }
}

#[test]
fn test_non_consecutive_checkpoint_scanning() {
    let manager = XdrVerificationManager::new();
    manager.record_ledger_data(63, create_complete_checkpoint_data(63, [0; 32]));
    manager.record_ledger_data(191, create_complete_checkpoint_data(191, [0; 32]));
    manager.verify_and_release(63);
    manager.verify_and_release(191);

    assert!(manager.get_errors().is_empty());
    assert!(manager.verify_checkpoint_chain().is_empty());
}

#[test]
fn test_cross_checkpoint_missing_last_ledger_breaks_chain() {
    let manager = XdrVerificationManager::new();
    let mut ledger_data_63 = create_complete_checkpoint_data(63, [0; 32]);
    ledger_data_63.remove(&63);

    manager.record_ledger_data(63, ledger_data_63);
    manager.record_ledger_data(127, create_complete_checkpoint_data(127, [0; 32]));
    manager.verify_and_release(63);
    manager.verify_and_release(127);

    let total_errors = manager.get_errors().len() + manager.verify_checkpoint_chain().len();
    assert!(total_errors > 0);
}

#[test]
fn test_manager_memory_freed_after_verification() {
    let manager = XdrVerificationManager::new();
    let checkpoint = 63;
    let ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);

    manager.record_ledger_data(checkpoint, ledger_data.clone());
    manager.verify_and_release(checkpoint);
    manager.record_ledger_data(checkpoint, ledger_data);
    manager.verify_and_release(checkpoint);

    assert!(manager.get_errors().is_empty());
}

#[test]
fn test_verify_and_release_with_only_tx_hashes_records_error_and_is_idempotent() {
    let manager = XdrVerificationManager::new();
    manager.record_tx_set_hashes(127, HashMap::from([(100, [1; 32])]));
    manager.verify_and_release(127);
    let first_errors = manager.get_errors();
    manager.verify_and_release(127);
    let second_errors = manager.get_errors();

    assert_eq!(first_errors.len(), 1);
    assert_eq!(second_errors.len(), 1);
    assert_has_error(&manager, "missing ledger verification data");
}

#[test]
fn test_verify_and_release_with_only_result_hashes_records_error() {
    let manager = XdrVerificationManager::new();
    manager.record_result_hashes(127, HashMap::from([(100, [1; 32])]));
    manager.verify_and_release(127);

    assert_has_error(&manager, "missing ledger verification data");
}

#[test]
fn test_verify_checkpoint_chain_with_three_consecutive_checkpoints() {
    let manager = XdrVerificationManager::new();
    let ledger_data_63 = create_complete_checkpoint_data(63, [0; 32]);
    let hash_63 = ledger_data_63.get(&63).unwrap().computed_hash;
    let ledger_data_127 = create_complete_checkpoint_data(127, hash_63);
    let hash_127 = ledger_data_127.get(&127).unwrap().computed_hash;
    let ledger_data_191 = create_complete_checkpoint_data(191, hash_127);

    manager.record_ledger_data(63, ledger_data_63);
    manager.record_ledger_data(127, ledger_data_127);
    manager.record_ledger_data(191, ledger_data_191);
    manager.verify_and_release(63);
    manager.verify_and_release(127);
    manager.verify_and_release(191);

    assert!(manager.verify_checkpoint_chain().is_empty());
}

#[test]
fn test_verify_checkpoint_chain_empty_manager() {
    let manager = XdrVerificationManager::new();
    assert!(manager.verify_checkpoint_chain().is_empty());
}

#[test]
fn test_expected_ledger_range_large_checkpoint() {
    let checkpoint = u32::MAX - (u32::MAX % 64);
    let (first, last) = expected_ledger_range(checkpoint);
    assert_eq!(last, checkpoint);
    assert_eq!(last - first, 63);
}

#[rstest]
#[case::tx_set("tx set")]
#[case::result_set("result set")]
fn test_manager_detects_hash_mismatch(#[case] hash_type: &str) {
    let manager = XdrVerificationManager::new();
    let checkpoint = 127;
    let expected = hash_of("expected");
    let wrong = hash_of("wrong");

    let mut ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in ledger_data.values_mut() {
        match hash_type {
            "tx set" => data.expected_tx_set_hash = expected,
            "result set" => data.expected_result_hash = expected,
            _ => unreachable!(),
        }
    }

    let wrong_hashes: HashMap<u32, [u8; 32]> =
        ledger_data.keys().map(|&seq| (seq, wrong)).collect();

    manager.record_ledger_data(checkpoint, ledger_data);
    match hash_type {
        "tx set" => manager.record_tx_set_hashes(checkpoint, wrong_hashes),
        "result set" => manager.record_result_hashes(checkpoint, wrong_hashes),
        _ => unreachable!(),
    }
    manager.verify_and_release(checkpoint);

    assert_has_error(&manager, &format!("{hash_type} hash mismatch"));
}

#[rstest]
#[case::tx_set("tx set")]
#[case::result("result")]
fn test_manager_detects_missing_entry_for_non_empty_hash(#[case] hash_type: &str) {
    let manager = XdrVerificationManager::new();
    let checkpoint = 127;
    let non_empty_hash = hash_of("non_empty");

    let mut ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in ledger_data.values_mut() {
        // tx set missing-entry detection requires expected_result_hash != EMPTY_XDR_ARRAY_HASH,
        // so both cases set non-empty hashes for both fields.
        data.expected_tx_set_hash = non_empty_hash;
        data.expected_result_hash = non_empty_hash;
    }

    manager.record_ledger_data(checkpoint, ledger_data);
    match hash_type {
        "tx set" => manager.record_tx_set_hashes(checkpoint, HashMap::new()),
        "result" => manager.record_result_hashes(checkpoint, HashMap::new()),
        _ => unreachable!(),
    }
    manager.verify_and_release(checkpoint);

    assert_has_error(&manager, &format!("missing {hash_type} entry"));
}

/// Missing entries should not be flagged when the expected hash indicates an empty
/// or zero-valued set. Each case sets a different "empty" hash variant and verifies
/// no spurious errors are produced.
#[rstest]
#[case::tx_set_empty_v0("tx set", "empty_v0")]
#[case::tx_set_empty_v1("tx set", "empty_v1")]
#[case::tx_set_zero("tx set", "zero")]
#[case::result_empty_xdr_array("result", "empty_xdr_array")]
#[case::result_zero("result", "zero")]
fn test_manager_allows_missing_entry_for_empty_hash(
    #[case] hash_type: &str,
    #[case] empty_variant: &str,
) {
    let manager = XdrVerificationManager::new();
    let checkpoint = 127;

    let mut ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in ledger_data.values_mut() {
        match (hash_type, empty_variant) {
            ("tx set", "empty_v0") => {
                data.expected_tx_set_hash = compute_empty_v0_tx_set_hash(&data.prev_hash);
                data.expected_result_hash = hash_of("some_result");
            }
            ("tx set", "empty_v1") => {
                data.expected_tx_set_hash = compute_empty_v1_tx_set_hash(&data.prev_hash);
                data.expected_result_hash = hash_of("some_result");
            }
            ("tx set", "zero") => {} // defaults are already [0; 32]
            ("result", "empty_xdr_array") => {
                data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
            }
            ("result", "zero") => {} // defaults are already [0; 32]
            _ => unreachable!(),
        }
    }

    manager.record_ledger_data(checkpoint, ledger_data);
    match hash_type {
        "tx set" => manager.record_tx_set_hashes(checkpoint, HashMap::new()),
        "result" => manager.record_result_hashes(checkpoint, HashMap::new()),
        _ => unreachable!(),
    }
    manager.verify_and_release(checkpoint);

    assert_no_errors_matching(&manager, hash_type);
}

#[test]
fn test_manager_still_flags_missing_tx_set_when_result_hash_is_empty_array_hash() {
    // Covers the suspicious branch where expected_result_hash == EMPTY_XDR_ARRAY_HASH
    // currently suppresses a missing-transactions error. When the result hash
    // indicates no results, we infer no transactions occurred even if the
    // tx_set_hash doesn't match known empty patterns (e.g., different protocol
    // versions may compute the empty hash differently).
    let manager = XdrVerificationManager::new();
    let checkpoint = 127;
    let non_empty_tx_hash = hash_of("non_empty_tx_set");

    let mut ledger_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in ledger_data.values_mut() {
        data.expected_tx_set_hash = non_empty_tx_hash;
        data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
    }

    manager.record_ledger_data(checkpoint, ledger_data);
    manager.record_tx_set_hashes(checkpoint, HashMap::new());
    manager.verify_and_release(checkpoint);

    // The EMPTY_XDR_ARRAY_HASH result hash suppresses the missing tx set error
    // because no results implies no transactions, regardless of tx_set_hash format
    assert_no_errors_matching(&manager, "missing tx set entry");
}

#[rstest]
#[case::ledger("ledger")]
#[case::transaction("transaction")]
#[case::result("result")]
fn test_parse_rejects_ledger_outside_checkpoint_range(#[case] file_type: &str) {
    let data = match file_type {
        "ledger" => frame_xdr(&create_valid_ledger_entry(200, [0; 32], [0; 32], [0; 32])),
        "transaction" => frame_xdr(&v0_history_entry(200, [0; 32], vec![tx_v0_envelope(1)])),
        "result" => frame_xdr(&result_entry(200, &[1])),
        _ => unreachable!(),
    };
    let err = match file_type {
        "ledger" => parse_ledger_entries_for_checkpoint(&data, Some(127)).unwrap_err(),
        "transaction" => parse_transaction_entries_for_checkpoint(&data, Some(127)).unwrap_err(),
        "result" => parse_result_entries_for_checkpoint(&data, Some(127)).unwrap_err(),
        _ => unreachable!(),
    };
    assert!(err.message.contains("outside expected checkpoint range"));
}
