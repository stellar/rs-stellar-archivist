use super::utils::{parse_ledger_header_entries, parse_result_entries, parse_transaction_entries};
use crate::xdr_verify::{
    compute_empty_v0_tx_set_hash, compute_empty_v1_parallel_tx_set_hash,
    compute_empty_v1_sequential_tx_set_hash, compute_v0_tx_set_hash, compute_v1_tx_set_hash,
    expected_ledger_range, is_empty_tx_set_hash, parse_ledger_header_entries_for_checkpoint,
    parse_result_entries_for_checkpoint, parse_scp_entries,
    parse_transaction_entries_for_checkpoint, EmptyTxSetInfo, LedgerHeaderVerificationData,
    VerificationErrorType, XdrVerificationManager, EMPTY_XDR_ARRAY_HASH,
};
use rstest::rstest;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use stellar_xdr::{
    AccountId, ContractExecutable, ContractExecutableExternalRef, ContractId, ContractIdPreimage,
    ContractIdPreimageFromAddress, CreateAccountOp, CreateContractArgsV2,
    GeneralizedTransactionSet, Hash, HostFunction, InvokeContractArgs, InvokeHostFunctionOp,
    LedgerCloseValueSignature, LedgerHeader, LedgerHeaderExt, LedgerHeaderHistoryEntry,
    LedgerHeaderHistoryEntryExt, LedgerScpMessages, Limits, Memo, MuxedAccount, NodeId, Operation,
    OperationBody, ParallelTxsComponent, Preconditions, PublicKey, ScAddress, ScString, ScSymbol,
    ScVal, ScpHistoryEntry, ScpHistoryEntryV0, SequenceNumber, Signature,
    SorobanAddressCredentials, SorobanAuthorizationEntry, SorobanAuthorizedFunction,
    SorobanAuthorizedInvocation, SorobanCredentials, StellarValueExt, StellarValueProposedValue,
    TimePoint, Transaction, TransactionEnvelope, TransactionHistoryEntry,
    TransactionHistoryEntryExt, TransactionHistoryResultEntry, TransactionHistoryResultEntryExt,
    TransactionPhase, TransactionResult, TransactionResultExt, TransactionResultPair,
    TransactionResultResult, TransactionResultSet, TransactionSet, TransactionSetV1, TransactionV0,
    TransactionV0Envelope, TransactionV0Ext, TransactionV1Envelope, Uint256, VecM, WriteXdr,
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
            ext: stellar_xdr::TransactionExt::V0,
        },
        signatures: VecM::default(),
    })
}

/// Soroban transaction whose auth entry uses the CAP-0071 (protocol 27)
/// `SOROBAN_CREDENTIALS_ADDRESS_V2` arm, which pre-27 XDR cannot decode.
fn tx_soroban_envelope_with_address_v2_auth(id: u8) -> TransactionEnvelope {
    let invoke_args = InvokeContractArgs {
        contract_address: ScAddress::Contract(ContractId(Hash([id; 32]))),
        function_name: ScSymbol("transfer".try_into().unwrap()),
        args: VecM::default(),
    };
    let auth = SorobanAuthorizationEntry {
        credentials: SorobanCredentials::AddressV2(SorobanAddressCredentials {
            address: ScAddress::Account(account_id(id)),
            nonce: 1,
            signature_expiration_ledger: 100,
            signature: ScVal::Void,
        }),
        root_invocation: SorobanAuthorizedInvocation {
            function: SorobanAuthorizedFunction::ContractFn(invoke_args.clone()),
            sub_invocations: VecM::default(),
        },
    };
    TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: Transaction {
            source_account: muxed_account(id),
            fee: 100,
            seq_num: SequenceNumber(i64::from(id) + 1),
            cond: Preconditions::None,
            memo: Memo::None,
            operations: vec![Operation {
                source_account: None,
                body: OperationBody::InvokeHostFunction(InvokeHostFunctionOp {
                    host_function: HostFunction::InvokeContract(invoke_args),
                    auth: vec![auth].try_into().unwrap(),
                }),
            }]
            .try_into()
            .unwrap(),
            ext: stellar_xdr::TransactionExt::V0,
        },
        signatures: VecM::default(),
    })
}

/// Soroban transaction exercising the CAP-0085 (protocol 28) arms that
/// pre-28 XDR cannot decode: a `CONTRACT_EXECUTABLE_EXTERNAL_REF` executable
/// and an `SCV_EXECUTABLE_TAG` constructor argument.
fn tx_soroban_envelope_with_external_ref_executable(id: u8) -> TransactionEnvelope {
    let create_v2 = CreateContractArgsV2 {
        contract_id_preimage: ContractIdPreimage::Address(ContractIdPreimageFromAddress {
            address: ScAddress::Account(account_id(id)),
            salt: Uint256([id; 32]),
        }),
        executable: ContractExecutable::ExternalRef(ContractExecutableExternalRef {
            executable_owner: ScAddress::Contract(ContractId(Hash([id; 32]))),
            tag: ScString("fleet-v2".try_into().unwrap()),
        }),
        constructor_args: vec![ScVal::ExecutableTag(ScString(
            "fleet-v2".try_into().unwrap(),
        ))]
        .try_into()
        .unwrap(),
    };
    TransactionEnvelope::Tx(TransactionV1Envelope {
        tx: Transaction {
            source_account: muxed_account(id),
            fee: 100,
            seq_num: SequenceNumber(i64::from(id) + 1),
            cond: Preconditions::None,
            memo: Memo::None,
            operations: vec![Operation {
                source_account: None,
                body: OperationBody::InvokeHostFunction(InvokeHostFunctionOp {
                    host_function: HostFunction::CreateContractV2(create_v2),
                    auth: VecM::default(),
                }),
            }]
            .try_into()
            .unwrap(),
            ext: stellar_xdr::TransactionExt::V0,
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
    let component = stellar_xdr::TxSetComponent::TxsetCompTxsMaybeDiscountedFee(
        stellar_xdr::TxSetComponentTxsMaybeDiscountedFee {
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
        scp_value: stellar_xdr::StellarValue {
            tx_set_hash: Hash(tx_set_hash),
            close_time: TimePoint(0),
            upgrades: VecM::default(),
            ext: stellar_xdr::StellarValueExt::Basic,
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

fn create_valid_ledger_header_entry(
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

fn cap83_proposed_value(prev_hash: [u8; 32], dropped: [u8; 32]) -> StellarValueProposedValue {
    StellarValueProposedValue {
        tx_set_hash: Hash(dropped),
        previous_ledger_hash: Hash(prev_hash),
        previous_ledger_version: 28,
        lc_value_signature: LedgerCloseValueSignature {
            node_id: NodeId(PublicKey::PublicKeyTypeEd25519(Uint256([7; 32]))),
            signature: Signature::default(),
        },
    }
}

fn create_cap83_ledger_header_entry(
    seq: u32,
    prev_hash: [u8; 32],
    dropped: [u8; 32],
) -> LedgerHeaderHistoryEntry {
    let mut header = create_minimal_ledger_header(seq, prev_hash, [0; 32], EMPTY_XDR_ARRAY_HASH.0);
    header.ledger_version = 28;
    header.scp_value.ext = StellarValueExt::EmptyTxSet(cap83_proposed_value(prev_hash, dropped));
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
) -> BTreeMap<u32, LedgerHeaderVerificationData> {
    let (first_ledger, last_ledger) = expected_ledger_range(checkpoint);
    let mut header_data = BTreeMap::new();
    let mut prev_hash = initial_prev_hash;

    for seq in first_ledger..=last_ledger {
        let computed_hash: [u8; 32] = Sha256::digest(format!("ledger{}", seq).as_bytes()).into();
        header_data.insert(
            seq,
            LedgerHeaderVerificationData {
                computed_hash: Hash(computed_hash),
                prev_ledger_hash: Hash(prev_hash),
                expected_tx_set_hash: Hash([0; 32]),
                expected_result_hash: Hash([0; 32]),
                ledger_version: 21,
                empty_tx_set: None,
            },
        );
        prev_hash = computed_hash;
    }

    header_data
}

fn create_checkpoint_data_missing(
    checkpoint: u32,
    missing: &[u32],
) -> BTreeMap<u32, LedgerHeaderVerificationData> {
    let mut data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for seq in missing {
        data.remove(seq);
    }
    data
}

fn single_ledger_header_data(
    seq: u32,
    computed_hash: [u8; 32],
    prev_hash: [u8; 32],
) -> BTreeMap<u32, LedgerHeaderVerificationData> {
    BTreeMap::from([(
        seq,
        LedgerHeaderVerificationData {
            computed_hash: Hash(computed_hash),
            prev_ledger_hash: Hash(prev_hash),
            expected_tx_set_hash: Hash([0; 32]),
            expected_result_hash: Hash([0; 32]),
            ledger_version: 21,
            empty_tx_set: None,
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
fn test_parse_ledger_header_entries_empty_input() {
    let parsed = parse_ledger_header_entries(&[]).unwrap();
    assert!(parsed.is_empty());
}

#[test]
fn test_parse_ledger_header_entries_single_entry() {
    let entry = create_valid_ledger_header_entry(100, [1; 32], [2; 32], [3; 32]);
    let parsed = parse_ledger_header_entries(&frame_xdr(&entry)).unwrap();

    assert_eq!(parsed.len(), 1);
    let actual = parsed.get(&100).unwrap();
    assert_eq!(actual.prev_ledger_hash, Hash([1; 32]));
    assert_eq!(actual.expected_tx_set_hash, Hash([2; 32]));
    assert_eq!(actual.expected_result_hash, Hash([3; 32]));
}

#[test]
fn test_parse_cap83_header_extracts_empty_tx_set_info() {
    let prev = hash_of("prev-ledger");
    let dropped = hash_of("dropped-tx-set");
    let entry = create_cap83_ledger_header_entry(100, prev, dropped);
    let parsed = parse_ledger_header_entries(&frame_xdr(&entry)).unwrap();
    let data = &parsed[&100];
    assert_eq!(data.ledger_version, 28);
    let info: &EmptyTxSetInfo = data
        .empty_tx_set
        .as_ref()
        .expect("EmptyTxSet ext must be extracted");
    assert_eq!(info.proposed_prev_ledger_hash, Hash(prev));
    assert_eq!(info.proposed_prev_ledger_version, 28);
    assert_eq!(data.expected_tx_set_hash, Hash([0; 32]));
    assert_eq!(data.expected_result_hash, EMPTY_XDR_ARRAY_HASH);
}

#[test]
fn test_parse_plain_header_has_no_empty_tx_set_info() {
    let entry = create_valid_ledger_header_entry(100, hash_of("p"), hash_of("t"), hash_of("r"));
    let parsed = parse_ledger_header_entries(&frame_xdr(&entry)).unwrap();
    assert_eq!(parsed[&100].ledger_version, 21);
    assert!(parsed[&100].empty_tx_set.is_none());
}

#[test]
fn test_parse_ledger_header_entries_hash_mismatch() {
    let entry = LedgerHeaderHistoryEntry {
        hash: Hash([0xff; 32]),
        header: create_minimal_ledger_header(100, [0; 32], [0; 32], [0; 32]),
        ext: LedgerHeaderHistoryEntryExt::V0,
    };

    let err = parse_ledger_header_entries(&frame_xdr(&entry)).unwrap_err();
    assert!(err.message.contains("hash mismatch"));
}

#[rstest]
#[case::ledger_header("ledger")]
#[case::transaction("transaction")]
#[case::result("result")]
fn test_parse_rejects_duplicate_sequence(#[case] file_type: &str) {
    let frame = match file_type {
        "ledger" => frame_xdr(&create_valid_ledger_header_entry(
            100, [0; 32], [0; 32], [0; 32],
        )),
        "transaction" => frame_xdr(&v0_history_entry(100, [0; 32], vec![tx_v0_envelope(1)])),
        "result" => frame_xdr(&result_entry(100, &[1])),
        _ => unreachable!(),
    };
    let mut data = frame.clone();
    data.extend(frame);
    let err = match file_type {
        "ledger" => parse_ledger_header_entries(&data).unwrap_err(),
        "transaction" => parse_transaction_entries(&data).unwrap_err(),
        "result" => parse_result_entries(&data).unwrap_err(),
        _ => unreachable!(),
    };
    assert!(err.message.contains("duplicate"));
}

#[rstest]
#[case::ledger_header("ledger")]
#[case::transaction("transaction")]
#[case::result("result")]
fn test_parse_rejects_malformed_frame(#[case] file_type: &str) {
    let valid = match file_type {
        "ledger" => frame_xdr(&create_valid_ledger_header_entry(
            100, [0; 32], [0; 32], [0; 32],
        )),
        "transaction" => frame_xdr(&v0_history_entry(100, [0; 32], vec![tx_v0_envelope(1)])),
        "result" => frame_xdr(&result_entry(100, &[1])),
        _ => unreachable!(),
    };
    let truncated = &valid[..valid.len() / 2];
    let err = match file_type {
        "ledger" => parse_ledger_header_entries(truncated).unwrap_err(),
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

    assert_eq!(parsed, BTreeMap::from([(100u32, Hash(expected))]));
}

#[test]
fn test_parse_transaction_entries_v0_non_empty() {
    let prev_hash = [0x42; 32];
    let txs = vec![tx_v0_envelope(1), tx_v0_envelope(2)];
    let entry = v0_history_entry(100, prev_hash, txs.clone());
    let parsed = parse_transaction_entries(&frame_xdr(&entry)).unwrap();

    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[&100], compute_v0_tx_set_hash(&entry.tx_set).unwrap());
    assert!(!is_empty_tx_set_hash(&parsed[&100], &Hash(prev_hash)));
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
    assert!(!is_empty_tx_set_hash(&parsed[&100], &Hash(prev_hash)));
}

#[test]
fn test_parse_transaction_entries_v1_with_cap71_address_v2_credentials() {
    let prev_hash = [0x24; 32];
    let entry = v1_history_entry(
        100,
        prev_hash,
        vec![tx_soroban_envelope_with_address_v2_auth(1)],
    );
    let parsed = parse_transaction_entries(&frame_xdr(&entry)).unwrap();

    let TransactionHistoryEntryExt::V1(generalized) = &entry.ext else {
        panic!("expected V1 entry");
    };
    assert_eq!(parsed[&100], compute_v1_tx_set_hash(generalized).unwrap());
}

#[test]
fn test_parse_transaction_entries_v1_with_cap85_external_ref_executable() {
    let prev_hash = [0x24; 32];
    let entry = v1_history_entry(
        100,
        prev_hash,
        vec![tx_soroban_envelope_with_external_ref_executable(1)],
    );
    let parsed = parse_transaction_entries(&frame_xdr(&entry)).unwrap();

    let TransactionHistoryEntryExt::V1(generalized) = &entry.ext else {
        panic!("expected V1 entry");
    };
    assert_eq!(parsed[&100], compute_v1_tx_set_hash(generalized).unwrap());
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
    assert_eq!(compute_v0_tx_set_hash(&tx_set).unwrap(), Hash(expected));
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
            vec![stellar_xdr::TxSetComponent::TxsetCompTxsMaybeDiscountedFee(
                stellar_xdr::TxSetComponentTxsMaybeDiscountedFee {
                    base_fee: None,
                    txs: vec![tx_v1_envelope(1)].try_into().unwrap(),
                },
            )]
            .try_into()
            .unwrap(),
        )]
        .try_into()
        .unwrap(),
    });
    let expected: [u8; 32] = Sha256::digest(generalized.to_xdr(Limits::none()).unwrap()).into();

    assert_eq!(
        compute_v1_tx_set_hash(&generalized).unwrap(),
        Hash(expected)
    );
}

#[test]
fn test_empty_v1_tx_set_hash_shapes() {
    let prev = Hash(hash_of("prev"));
    let seq_set = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: prev.clone(),
        phases: vec![
            TransactionPhase::V0(VecM::default()),
            TransactionPhase::V0(VecM::default()),
        ]
        .try_into()
        .unwrap(),
    });
    let par_set = GeneralizedTransactionSet::V1(TransactionSetV1 {
        previous_ledger_hash: prev.clone(),
        phases: vec![
            TransactionPhase::V0(VecM::default()),
            TransactionPhase::V1(ParallelTxsComponent {
                base_fee: None,
                execution_stages: VecM::default(),
            }),
        ]
        .try_into()
        .unwrap(),
    });
    assert_eq!(
        compute_empty_v1_sequential_tx_set_hash(&prev),
        compute_v1_tx_set_hash(&seq_set).unwrap()
    );
    assert_eq!(
        compute_empty_v1_parallel_tx_set_hash(&prev),
        compute_v1_tx_set_hash(&par_set).unwrap()
    );
    assert_ne!(
        compute_empty_v1_sequential_tx_set_hash(&prev),
        compute_empty_v1_parallel_tx_set_hash(&prev)
    );
}

#[test]
fn test_is_empty_tx_set_hash_recognizes_all_shapes() {
    let prev = Hash(hash_of("prev"));
    assert!(is_empty_tx_set_hash(&Hash([0; 32]), &prev));
    assert!(is_empty_tx_set_hash(
        &compute_empty_v0_tx_set_hash(&prev),
        &prev
    ));
    assert!(is_empty_tx_set_hash(
        &compute_empty_v1_sequential_tx_set_hash(&prev),
        &prev
    ));
    assert!(is_empty_tx_set_hash(
        &compute_empty_v1_parallel_tx_set_hash(&prev),
        &prev
    ));
    assert!(!is_empty_tx_set_hash(&Hash(hash_of("random")), &prev));
}

#[test]
fn test_empty_xdr_array_hash_constant_matches_hash() {
    let expected: [u8; 32] = Sha256::digest([0_u8, 0, 0, 0]).into();
    assert_eq!(EMPTY_XDR_ARRAY_HASH, Hash(expected));
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

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in header_data.values_mut() {
        // Non-zero tx-set hash so the CAP-0083 zero-tx-set-without-ext-arm
        // check stays quiet; this test only exercises result-hash matching.
        data.expected_tx_set_hash = Hash(hash_of("txset"));
        data.expected_result_hash = Hash(result_hash);
    }

    let result_hashes: BTreeMap<u32, Hash> = header_data
        .keys()
        .map(|&seq| (seq, Hash(result_hash)))
        .collect();

    manager.record_header_data(checkpoint, header_data);
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

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    header_data
        .get_mut(&corrupted_ledger)
        .unwrap()
        .prev_ledger_hash = Hash([0xff; 32]);

    manager.record_header_data(checkpoint, header_data);
    manager.verify_and_release(checkpoint);

    assert!(manager.get_errors().iter().any(|e| matches!(
        e.kind,
        VerificationErrorType::Ledger(seq) if seq == corrupted_ledger
    ) && e.message.contains("hash chain break")));
}

#[rstest]
#[case::genesis(63, 63)]
#[case::regular(127, 64)]
fn test_complete_checkpoint_passes(#[case] checkpoint: u32, #[case] expected_count: usize) {
    let manager = XdrVerificationManager::new();
    let header_data =
        with_empty_ledger_hashes(create_complete_checkpoint_data(checkpoint, [0; 32]));
    assert_eq!(header_data.len(), expected_count);

    manager.record_header_data(checkpoint, header_data);
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
fn test_missing_ledger_header_entries(#[case] checkpoint: u32, #[case] missing: Vec<u32>) {
    let manager = XdrVerificationManager::new();
    manager.record_header_data(
        checkpoint,
        create_checkpoint_data_missing(checkpoint, &missing),
    );
    manager.verify_and_release(checkpoint);

    assert_has_error(&manager, "missing");
}

#[test]
fn test_ledger_outside_expected_checkpoint_range() {
    let manager = XdrVerificationManager::new();
    let mut header_data = create_complete_checkpoint_data(127, [0; 32]);
    header_data.insert(
        200,
        LedgerHeaderVerificationData {
            computed_hash: Hash([0xaa; 32]),
            prev_ledger_hash: Hash([0xbb; 32]),
            expected_tx_set_hash: Hash([0; 32]),
            expected_result_hash: Hash([0; 32]),
            ledger_version: 21,
            empty_tx_set: None,
        },
    );

    manager.record_header_data(127, header_data);
    manager.verify_and_release(127);

    assert_has_error(&manager, "unexpected ledger-header entries outside range");
}

#[rstest]
#[case::valid_chain(false)]
#[case::broken_chain(true)]
fn test_cross_checkpoint_chain(#[case] break_chain: bool) {
    let manager = XdrVerificationManager::new();
    let header_data_63 = single_ledger_header_data(63, hash_of("ledger63"), [0; 32]);
    let prev_hash = if break_chain {
        [0xff; 32]
    } else {
        hash_of("ledger63")
    };
    let header_data_127 = single_ledger_header_data(64, hash_of("ledger127"), prev_hash);

    manager.record_header_data(63, header_data_63);
    manager.record_header_data(127, header_data_127);
    manager.verify_and_release(63);
    manager.verify_and_release(127);

    let errors_before_chain = manager.get_errors().len();
    manager.verify_checkpoint_chain();
    let chain_errors_added = manager.get_errors().len() - errors_before_chain;
    if break_chain {
        assert_eq!(chain_errors_added, 1);
    } else {
        assert_eq!(chain_errors_added, 0);
    }
}

#[rstest]
#[case::valid(false)]
#[case::broken(true)]
fn test_consecutive_checkpoints_full(#[case] break_chain: bool) {
    let manager = XdrVerificationManager::new();
    let header_data_63 = with_empty_ledger_hashes(create_complete_checkpoint_data(63, [0; 32]));
    let last_hash_of_63 = header_data_63.get(&63).unwrap().computed_hash.clone();

    let mut header_data_127 = BTreeMap::new();
    let mut prev_hash = if break_chain {
        Hash([0xff; 32])
    } else {
        last_hash_of_63
    };
    for seq in 64_u32..=127 {
        let computed_hash = Hash(Sha256::digest(seq.to_le_bytes()).into());
        header_data_127.insert(
            seq,
            LedgerHeaderVerificationData {
                computed_hash: computed_hash.clone(),
                prev_ledger_hash: prev_hash.clone(),
                expected_tx_set_hash: Hash(hash_of(&format!("txset{seq}"))),
                expected_result_hash: Hash([0; 32]),
                ledger_version: 21,
                empty_tx_set: None,
            },
        );
        prev_hash = computed_hash;
    }

    manager.record_header_data(63, header_data_63);
    manager.record_header_data(127, header_data_127);
    manager.verify_and_release(63);
    manager.verify_and_release(127);

    assert!(manager.get_errors().is_empty());
    manager.verify_checkpoint_chain();
    if break_chain {
        assert!(!manager.get_errors().is_empty());
    } else {
        assert!(manager.get_errors().is_empty());
    }
}

#[test]
fn test_non_consecutive_checkpoint_scanning() {
    let manager = XdrVerificationManager::new();
    manager.record_header_data(
        63,
        with_empty_ledger_hashes(create_complete_checkpoint_data(63, [0; 32])),
    );
    manager.record_header_data(
        191,
        with_empty_ledger_hashes(create_complete_checkpoint_data(191, [0; 32])),
    );
    manager.verify_and_release(63);
    manager.verify_and_release(191);

    assert!(manager.get_errors().is_empty());
    manager.verify_checkpoint_chain();
    assert!(manager.get_errors().is_empty());
}

#[test]
fn test_cross_checkpoint_missing_last_ledger_breaks_chain() {
    let manager = XdrVerificationManager::new();
    let mut header_data_63 = create_complete_checkpoint_data(63, [0; 32]);
    header_data_63.remove(&63);

    manager.record_header_data(63, header_data_63);
    manager.record_header_data(127, create_complete_checkpoint_data(127, [0; 32]));
    manager.verify_and_release(63);
    manager.verify_and_release(127);
    manager.verify_checkpoint_chain();

    assert!(!manager.get_errors().is_empty());
}

#[test]
fn test_manager_memory_freed_after_verification() {
    let manager = XdrVerificationManager::new();
    let checkpoint = 63;
    let header_data =
        with_empty_ledger_hashes(create_complete_checkpoint_data(checkpoint, [0; 32]));

    manager.record_header_data(checkpoint, header_data.clone());
    manager.verify_and_release(checkpoint);
    manager.record_header_data(checkpoint, header_data);
    manager.verify_and_release(checkpoint);

    assert!(manager.get_errors().is_empty());
}

#[test]
fn test_verify_and_release_with_only_tx_hashes_records_error_and_is_idempotent() {
    let manager = XdrVerificationManager::new();
    manager.record_tx_set_hashes(127, BTreeMap::from([(100u32, Hash([1; 32]))]));
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
    manager.record_result_hashes(127, BTreeMap::from([(100u32, Hash([1; 32]))]));
    manager.verify_and_release(127);

    assert_has_error(&manager, "missing ledger verification data");
}

#[test]
fn test_verify_checkpoint_chain_with_three_consecutive_checkpoints() {
    let manager = XdrVerificationManager::new();
    let header_data_63 = with_empty_ledger_hashes(create_complete_checkpoint_data(63, [0; 32]));
    let hash_63 = header_data_63.get(&63).unwrap().computed_hash.clone();
    let header_data_127 = with_empty_ledger_hashes(create_complete_checkpoint_data(127, hash_63.0));
    let hash_127 = header_data_127.get(&127).unwrap().computed_hash.clone();
    let header_data_191 =
        with_empty_ledger_hashes(create_complete_checkpoint_data(191, hash_127.0));

    manager.record_header_data(63, header_data_63);
    manager.record_header_data(127, header_data_127);
    manager.record_header_data(191, header_data_191);
    manager.verify_and_release(63);
    manager.verify_and_release(127);
    manager.verify_and_release(191);
    manager.verify_checkpoint_chain();

    assert!(manager.get_errors().is_empty());
}

#[test]
fn test_verify_checkpoint_chain_empty_manager() {
    let manager = XdrVerificationManager::new();
    manager.verify_checkpoint_chain();
    assert!(manager.get_errors().is_empty());
}

#[test]
fn test_verify_checkpoint_chain_single_checkpoint_is_noop() {
    let manager = XdrVerificationManager::new();
    manager.record_header_data(
        63,
        with_empty_ledger_hashes(create_complete_checkpoint_data(63, [0; 32])),
    );
    manager.verify_and_release(63);
    manager.verify_checkpoint_chain();

    assert!(manager.get_errors().is_empty());
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

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in header_data.values_mut() {
        match hash_type {
            "tx set" => data.expected_tx_set_hash = Hash(expected),
            "result set" => data.expected_result_hash = Hash(expected),
            _ => unreachable!(),
        }
    }

    let wrong_hashes: BTreeMap<u32, Hash> =
        header_data.keys().map(|&seq| (seq, Hash(wrong))).collect();

    manager.record_header_data(checkpoint, header_data);
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

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in header_data.values_mut() {
        // Non-empty hashes for both fields so neither side's missing-entry
        // tolerance (empty tx-set sentinel / empty result hash) applies.
        data.expected_tx_set_hash = Hash(non_empty_hash);
        data.expected_result_hash = Hash(non_empty_hash);
    }

    manager.record_header_data(checkpoint, header_data);
    match hash_type {
        "tx set" => manager.record_tx_set_hashes(checkpoint, BTreeMap::new()),
        "result" => manager.record_result_hashes(checkpoint, BTreeMap::new()),
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
#[case::result_empty_xdr_array("result", "empty_xdr_array")]
#[case::result_zero_genesis("result", "zero")]
fn test_manager_allows_missing_entry_for_empty_hash(
    #[case] hash_type: &str,
    #[case] empty_variant: &str,
) {
    let manager = XdrVerificationManager::new();
    // A zero result hash is tolerated only on genesis (ledger 1), so the
    // "zero" case runs on the genesis checkpoint; the others use an ordinary
    // checkpoint.
    let checkpoint = if empty_variant == "zero" { 63 } else { 127 };

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in header_data.values_mut() {
        // Baseline non-zero tx-set hash so the CAP-0083 check (which flags a
        // zero tx-set hash without the ext arm) stays quiet; cases exercising
        // empty tx-set hashes override this.
        data.expected_tx_set_hash = Hash(hash_of("txset_baseline"));
        match (hash_type, empty_variant) {
            ("tx set", "empty_v0") => {
                // A missing tx entry is tolerated only when the tx-set hash is
                // a recognized empty sentinel AND the result hash is the
                // empty-result-set hash — both must model an empty ledger.
                data.expected_tx_set_hash = compute_empty_v0_tx_set_hash(&data.prev_ledger_hash);
                data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
            }
            ("tx set", "empty_v1") => {
                data.expected_tx_set_hash =
                    compute_empty_v1_parallel_tx_set_hash(&data.prev_ledger_hash);
                data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
            }
            ("result", "empty_xdr_array") => {
                data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
            }
            ("result", "zero") => {
                data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
            }
            _ => unreachable!(),
        }
    }
    if empty_variant == "zero" {
        // Model the real genesis shape: ledger 1 alone carries the zero
        // result hash (its header is synthesized without an SCP round).
        header_data.get_mut(&1).unwrap().expected_result_hash = Hash([0; 32]);
        header_data.get_mut(&1).unwrap().expected_tx_set_hash = Hash([0; 32]);
    }

    manager.record_header_data(checkpoint, header_data);
    match hash_type {
        "tx set" => manager.record_tx_set_hashes(checkpoint, BTreeMap::new()),
        "result" => manager.record_result_hashes(checkpoint, BTreeMap::new()),
        _ => unreachable!(),
    }
    manager.verify_and_release(checkpoint);

    assert_no_errors_matching(&manager, hash_type);
}

#[test]
fn test_manager_flags_missing_result_entry_for_non_genesis_zero_result_hash() {
    // Only genesis (ledger 1) legitimately carries an all-zero result hash;
    // post-genesis headers always record SHA256 of the (possibly empty)
    // result set, which can never be zero. A zero result hash mid-chain with
    // no results entry must therefore be reported, not tolerated.
    let manager = XdrVerificationManager::new();
    let checkpoint = 127;

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in header_data.values_mut() {
        // Non-zero tx-set hashes keep the zero-tx-set-hash check quiet;
        // result hashes stay at the fixture default of all-zeros.
        data.expected_tx_set_hash = Hash(hash_of("txset_baseline"));
    }

    manager.record_header_data(checkpoint, header_data);
    manager.record_result_hashes(checkpoint, BTreeMap::new());
    manager.verify_and_release(checkpoint);

    assert_has_error(&manager, "missing result entry");
}

#[test]
fn test_manager_flags_missing_tx_set_when_tx_hash_is_not_a_recognized_empty_shape() {
    // An empty result hash alone must NOT excuse a missing transactions
    // entry: if the header's tx-set hash claims a non-empty set, `--verify`
    // can no longer establish that the transactions file agrees with the
    // header. Both the empty result hash AND a recognized empty-tx-set
    // sentinel are required to tolerate a missing entry.
    let manager = XdrVerificationManager::new();
    let checkpoint = 127;
    let non_empty_tx_hash = hash_of("non_empty_tx_set");

    let mut header_data = create_complete_checkpoint_data(checkpoint, [0; 32]);
    for data in header_data.values_mut() {
        data.expected_tx_set_hash = Hash(non_empty_tx_hash);
        data.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
    }

    manager.record_header_data(checkpoint, header_data);
    manager.record_tx_set_hashes(checkpoint, BTreeMap::new());
    manager.verify_and_release(checkpoint);

    assert_has_error(&manager, "missing tx set entry");
}

#[rstest]
#[case::ledger_header("ledger")]
#[case::transaction("transaction")]
#[case::result("result")]
fn test_parse_rejects_ledger_outside_checkpoint_range(#[case] file_type: &str) {
    let data = match file_type {
        "ledger" => frame_xdr(&create_valid_ledger_header_entry(
            200, [0; 32], [0; 32], [0; 32],
        )),
        "transaction" => frame_xdr(&v0_history_entry(200, [0; 32], vec![tx_v0_envelope(1)])),
        "result" => frame_xdr(&result_entry(200, &[1])),
        _ => unreachable!(),
    };
    let err = match file_type {
        "ledger" => parse_ledger_header_entries_for_checkpoint(&data, Some(127)).unwrap_err(),
        "transaction" => parse_transaction_entries_for_checkpoint(&data, Some(127)).unwrap_err(),
        "result" => parse_result_entries_for_checkpoint(&data, Some(127)).unwrap_err(),
        _ => unreachable!(),
    };
    assert!(err.message.contains("outside expected checkpoint range"));
}

//=============================================================================
// record_all_errors — drains manager errors into ArchiveStats.failures.checkpoints
//=============================================================================

#[test]
fn test_record_all_errors_empty_manager_is_noop() {
    let manager = XdrVerificationManager::new();
    let mut failures = crate::utils::FailureTracker::default();

    manager.drain_all_errors(&mut failures);

    assert!(failures.is_empty());
    assert!(failures.checkpoints.is_empty());
    assert!(failures.files.is_empty());
    assert!(failures.buckets.is_empty());
    assert!(failures.well_known.is_none());
}

#[test]
fn test_record_all_errors_drains_each_variant() {
    let manager = XdrVerificationManager::new();

    // Trigger a `Checkpoint(cp)` error via a completeness failure.
    manager.record_header_data(127, create_checkpoint_data_missing(127, &[100]));
    manager.verify_and_release(127);

    // Trigger a `Ledger(seq)` error via an internal chain break.
    let mut chain_break_data = create_complete_checkpoint_data(191, [0; 32]);
    chain_break_data.get_mut(&150).unwrap().prev_ledger_hash = Hash([0xff; 32]);
    manager.record_header_data(191, chain_break_data);
    manager.verify_and_release(191);

    // Sanity: manager has accumulated errors.
    assert!(!manager.get_errors().is_empty());

    let mut failures = crate::utils::FailureTracker::default();
    manager.drain_all_errors(&mut failures);

    // Checkpoint(127) → cp 127.
    assert!(failures.checkpoints.contains(&127));
    // Ledger(150) → round_to_upper_checkpoint(150) = 191.
    assert!(failures.checkpoints.contains(&191));
    // Nothing should land in files/buckets/well_known — all manager errors are
    // cp-level cross-file/chain inconsistencies.
    assert!(failures.files.is_empty());
    assert!(failures.buckets.is_empty());
    assert!(failures.well_known.is_none());
}

#[test]
fn test_record_all_errors_boundary_inserts_both_cps() {
    let manager = XdrVerificationManager::new();

    // Build two adjacent cps (127 and 191) where the boundary hash chain
    // breaks: cp 127 ends with one computed hash, cp 191's first prev_hash
    // doesn't match.
    let data_127 = create_complete_checkpoint_data(127, [0; 32]);
    manager.record_header_data(127, data_127);
    manager.verify_and_release(127);

    // cp 191's first ledger (128) has prev_hash that won't match cp 127's last
    // computed_hash.
    let mut data_191 = create_complete_checkpoint_data(191, [0xab; 32]);
    // Make the internal chain self-consistent but mismatched with cp 127.
    let mut prev_hash = [0xab; 32];
    for (seq, entry) in &mut data_191 {
        entry.prev_ledger_hash = Hash(prev_hash);
        prev_hash = entry.computed_hash.0;
        let _ = seq;
    }
    manager.record_header_data(191, data_191);
    manager.verify_and_release(191);

    manager.verify_checkpoint_chain();

    let mut failures = crate::utils::FailureTracker::default();
    manager.drain_all_errors(&mut failures);

    // Boundary(191) records both 191 and 191-64=127.
    assert!(failures.checkpoints.contains(&191));
    assert!(failures.checkpoints.contains(&127));
}

#[test]
fn test_record_all_errors_idempotent() {
    let manager = XdrVerificationManager::new();
    manager.record_header_data(127, create_checkpoint_data_missing(127, &[100]));
    manager.verify_and_release(127);

    let mut failures = crate::utils::FailureTracker::default();
    manager.drain_all_errors(&mut failures);
    let cps_after_first = failures.checkpoints.clone();

    // Calling again is a true no-op: the first call drained the manager's
    // errors, so the second finds nothing to record and the set is unchanged.
    manager.drain_all_errors(&mut failures);
    let cps_after_second = failures.checkpoints.clone();

    assert_eq!(cps_after_first, cps_after_second);
    assert!(cps_after_first.contains(&127));
}

//=============================================================================
// CAP-0083 empty-tx-set header-internal + adjacent-ledger verification
//=============================================================================

fn cap83_header_data(
    seq: u32,
    prev_hash: [u8; 32],
    ledger_version: u32,
) -> LedgerHeaderVerificationData {
    LedgerHeaderVerificationData {
        computed_hash: Hash(hash_of(&format!("ledger{seq}"))),
        prev_ledger_hash: Hash(prev_hash),
        expected_tx_set_hash: Hash([0; 32]),
        expected_result_hash: EMPTY_XDR_ARRAY_HASH,
        ledger_version,
        empty_tx_set: Some(EmptyTxSetInfo {
            proposed_prev_ledger_hash: Hash(prev_hash),
            proposed_prev_ledger_version: ledger_version,
        }),
    }
}

/// Give every ledger the shape of a genuinely-empty ledger: the canonical
/// empty-V0 tx-set hash (non-zero, so the CAP-0083 zero-hash rule stays
/// quiet) plus the empty result-set hash — the only combination for which
/// missing transactions/results entries are tolerated.
fn with_empty_ledger_hashes(
    mut data: BTreeMap<u32, LedgerHeaderVerificationData>,
) -> BTreeMap<u32, LedgerHeaderVerificationData> {
    for d in data.values_mut() {
        d.expected_tx_set_hash = compute_empty_v0_tx_set_hash(&d.prev_ledger_hash);
        d.expected_result_hash = EMPTY_XDR_ARRAY_HASH;
    }
    data
}

/// Build a checkpoint-127 header map whose ledger 100 is a valid CAP-83
/// empty-tx-set ledger, apply `mutate`, run the manager over it with empty
/// tx/result maps, and return the manager for error assertions.
fn run_cap83_checkpoint(
    mutate: impl FnOnce(&mut BTreeMap<u32, LedgerHeaderVerificationData>),
) -> XdrVerificationManager {
    let manager = XdrVerificationManager::new();
    let mut data = with_empty_ledger_hashes(create_complete_checkpoint_data(127, [9; 32]));
    // CAP-0083 eligibility is gated on the predecessor's protocol, so the
    // predecessor must itself be on protocol 28 for ledger 100 to be valid.
    data.get_mut(&99).unwrap().ledger_version = 28;
    let prev = data[&99].computed_hash.clone();
    let prev_version = data[&99].ledger_version;
    let mut entry = cap83_header_data(100, prev.0, 28);
    entry
        .empty_tx_set
        .as_mut()
        .unwrap()
        .proposed_prev_ledger_version = prev_version;
    data.insert(100, entry);
    mutate(&mut data);
    manager.record_header_data(127, data);
    manager.record_tx_set_hashes(127, BTreeMap::new());
    manager.record_result_hashes(127, BTreeMap::new());
    manager.verify_and_release(127);
    manager
}

#[test]
fn test_cap83_valid_header_data_passes() {
    let manager = run_cap83_checkpoint(|_| {});
    assert!(
        manager
            .get_errors()
            .iter()
            .all(|e| !e.message.contains("empty-tx-set")),
        "valid CAP-83 ledger must not produce empty-tx-set errors: {:?}",
        manager.get_errors()
    );
}

#[test]
fn test_cap83_nonzero_tx_set_hash_rejected() {
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&100).unwrap().expected_tx_set_hash = Hash([5; 32]);
    });
    assert_has_error(&manager, "empty-tx-set ledger has non-zero tx set hash");
}

#[test]
fn test_cap83_pre_protocol_28_rejected() {
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&100).unwrap().ledger_version = 27;
    });
    assert_has_error(&manager, "requires protocol >= 28");
}

#[test]
fn test_cap83_pre_protocol_28_predecessor_rejected() {
    // Activation-boundary shape: the ledger's own header is protocol 28 but
    // its predecessor closed on 27. stellar-core gates empty-tx-set values on
    // the last-closed (predecessor) ledger's protocol, so this ext arm can
    // never legitimately appear on such a ledger.
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&99).unwrap().ledger_version = 27;
        d.get_mut(&100)
            .unwrap()
            .empty_tx_set
            .as_mut()
            .unwrap()
            .proposed_prev_ledger_version = 27;
    });
    assert_has_error(&manager, "requires predecessor protocol >= 28");
}

#[test]
fn test_cap83_proposed_prev_hash_mismatch_rejected() {
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&100)
            .unwrap()
            .empty_tx_set
            .as_mut()
            .unwrap()
            .proposed_prev_ledger_hash = Hash([1; 32]);
    });
    assert_has_error(&manager, "proposed previous_ledger_hash");
}

#[test]
fn test_cap83_nonempty_result_hash_rejected() {
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&100).unwrap().expected_result_hash = Hash([3; 32]);
    });
    assert_has_error(&manager, "empty-tx-set ledger has non-empty result hash");
}

#[test]
fn test_cap83_proposed_prev_version_mismatch_rejected() {
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&100)
            .unwrap()
            .empty_tx_set
            .as_mut()
            .unwrap()
            .proposed_prev_ledger_version = 27;
    });
    assert_has_error(&manager, "proposed previous ledger version");
}

#[test]
fn test_zero_tx_set_hash_without_ext_arm_rejected() {
    let manager = run_cap83_checkpoint(|d| {
        d.get_mut(&110).unwrap().expected_tx_set_hash = Hash([0; 32]);
    });
    assert_has_error(&manager, "zero tx set hash");
}

#[test]
fn test_genesis_zero_tx_set_hash_allowed() {
    let manager = XdrVerificationManager::new();
    // Genesis checkpoint covers ledgers 1..=63. Real genesis headers are
    // synthesized with BOTH hashes all-zeros (no SCP round produced them) —
    // model that shape for ledger 1 and make all others non-zero.
    let mut data = with_empty_ledger_hashes(create_complete_checkpoint_data(63, [0; 32]));
    data.get_mut(&1).unwrap().expected_tx_set_hash = Hash([0; 32]);
    data.get_mut(&1).unwrap().expected_result_hash = Hash([0; 32]);
    manager.record_header_data(63, data);
    manager.record_tx_set_hashes(63, BTreeMap::new());
    manager.record_result_hashes(63, BTreeMap::new());
    manager.verify_and_release(63);
    assert!(
        manager
            .get_errors()
            .iter()
            .all(|e| !e.message.contains("zero tx set hash")),
        "genesis ledger 1 must be allowed a zero tx set hash: {:?}",
        manager.get_errors()
    );
}

/// Like `run_cap83_checkpoint`, but with caller-supplied tx-set/result maps.
fn run_cap83_checkpoint_with_entries(
    tx_hashes: BTreeMap<u32, Hash>,
    result_hashes: BTreeMap<u32, Hash>,
) -> XdrVerificationManager {
    let manager = XdrVerificationManager::new();
    let mut data = with_empty_ledger_hashes(create_complete_checkpoint_data(127, [9; 32]));
    // As in `run_cap83_checkpoint`: the predecessor must be on protocol 28.
    data.get_mut(&99).unwrap().ledger_version = 28;
    let prev = data[&99].computed_hash.clone();
    let prev_version = data[&99].ledger_version;
    let mut entry = cap83_header_data(100, prev.0, 28);
    entry
        .empty_tx_set
        .as_mut()
        .unwrap()
        .proposed_prev_ledger_version = prev_version;
    data.insert(100, entry);
    manager.record_header_data(127, data);
    manager.record_tx_set_hashes(127, tx_hashes);
    manager.record_result_hashes(127, result_hashes);
    manager.verify_and_release(127);
    manager
}

#[test]
fn test_cap83_present_tx_entry_rejected() {
    let manager = run_cap83_checkpoint_with_entries(
        BTreeMap::from([(100u32, Hash(hash_of("some-tx-set")))]),
        BTreeMap::new(),
    );
    assert_has_error(
        &manager,
        "transactions entry present for empty-tx-set ledger",
    );
}

#[test]
fn test_cap83_present_result_entry_rejected() {
    // The entry's hash MATCHES the header's expected (empty) result hash —
    // presence alone must be the error.
    let manager = run_cap83_checkpoint_with_entries(
        BTreeMap::new(),
        BTreeMap::from([(100u32, EMPTY_XDR_ARRAY_HASH)]),
    );
    assert_has_error(&manager, "results entry present for empty-tx-set ledger");
}

#[test]
fn test_cap83_absent_entries_ok() {
    let manager = run_cap83_checkpoint_with_entries(BTreeMap::new(), BTreeMap::new());
    assert!(
        manager.get_errors().is_empty(),
        "absent entries for an empty-tx-set ledger must verify clean: {:?}",
        manager.get_errors()
    );
}

#[test]
fn test_cap83_boundary_proposed_version_mismatch_rejected() {
    let manager = XdrVerificationManager::new();

    // Checkpoint 127 (ledgers 64..=127), all protocol 21.
    let cp1 = with_empty_ledger_hashes(create_complete_checkpoint_data(127, [9; 32]));
    let last_hash = cp1[&127].computed_hash.clone();
    manager.record_header_data(127, cp1);
    manager.record_tx_set_hashes(127, BTreeMap::new());
    manager.record_result_hashes(127, BTreeMap::new());
    manager.verify_and_release(127);

    // Checkpoint 191 whose FIRST ledger (128) is an empty-tx-set ledger
    // claiming proposed previous version 28, while cp 127's last version is 21.
    let mut cp2 = with_empty_ledger_hashes(create_complete_checkpoint_data(191, [0; 32]));
    let mut first = cap83_header_data(128, last_hash.0, 28);
    first
        .empty_tx_set
        .as_mut()
        .unwrap()
        .proposed_prev_ledger_version = 28;
    cp2.insert(128, first);
    // keep the intra-checkpoint hash chain quiet for ledger 129 (and refresh
    // its canonical empty tx-set hash, which is derived from the prev hash)
    let first_hash = cp2[&128].computed_hash.clone();
    cp2.get_mut(&129).unwrap().prev_ledger_hash = first_hash.clone();
    cp2.get_mut(&129).unwrap().expected_tx_set_hash = compute_empty_v0_tx_set_hash(&first_hash);
    manager.record_header_data(191, cp2);
    manager.record_tx_set_hashes(191, BTreeMap::new());
    manager.record_result_hashes(191, BTreeMap::new());
    manager.verify_and_release(191);

    manager.verify_checkpoint_chain();
    assert_has_error(&manager, "proposed previous ledger version");
}

#[test]
fn test_cap83_boundary_proposed_version_match_ok() {
    let manager = XdrVerificationManager::new();

    // Checkpoint 127 (ledgers 64..=127) whose last ledger closed on protocol
    // 28 — the predecessor of a valid empty-tx-set ledger must be on 28.
    let mut cp1 = with_empty_ledger_hashes(create_complete_checkpoint_data(127, [9; 32]));
    cp1.get_mut(&127).unwrap().ledger_version = 28;
    let last_hash = cp1[&127].computed_hash.clone();
    manager.record_header_data(127, cp1);
    manager.record_tx_set_hashes(127, BTreeMap::new());
    manager.record_result_hashes(127, BTreeMap::new());
    manager.verify_and_release(127);

    // Checkpoint 191 whose FIRST ledger (128) is an empty-tx-set ledger whose
    // proposed previous version (28) matches cp 127's last ledger version (28).
    let mut cp2 = with_empty_ledger_hashes(create_complete_checkpoint_data(191, [0; 32]));
    let mut first = cap83_header_data(128, last_hash.0, 28);
    first
        .empty_tx_set
        .as_mut()
        .unwrap()
        .proposed_prev_ledger_version = 28;
    cp2.insert(128, first);
    // keep the intra-checkpoint hash chain quiet for ledger 129 (and refresh
    // its canonical empty tx-set hash, which is derived from the prev hash)
    let first_hash = cp2[&128].computed_hash.clone();
    cp2.get_mut(&129).unwrap().prev_ledger_hash = first_hash.clone();
    cp2.get_mut(&129).unwrap().expected_tx_set_hash = compute_empty_v0_tx_set_hash(&first_hash);
    manager.record_header_data(191, cp2);
    manager.record_tx_set_hashes(191, BTreeMap::new());
    manager.record_result_hashes(191, BTreeMap::new());
    manager.verify_and_release(191);

    manager.verify_checkpoint_chain();
    assert_no_errors_matching(&manager, "proposed previous ledger version");
}
