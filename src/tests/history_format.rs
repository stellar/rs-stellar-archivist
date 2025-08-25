use crate::history_format::{
    checkpoint_prefix, count_checkpoints_in_range, is_checkpoint, is_valid_bucket_hash,
    round_to_lower_checkpoint, round_to_upper_checkpoint, HistoryFileState,
    GENESIS_CHECKPOINT_LEDGER,
};
use rstest::*;
use std::fs;
use std::io::Write;
use tempfile::NamedTempFile;

#[fixture]
fn canonical_v1_json_str() -> String {
    fs::read_to_string("testdata/canonical-formats/history-archive-state-v1.json")
        .expect("Failed to read canonical v1 HAS file")
}

#[fixture]
fn canonical_v2_json_str() -> String {
    fs::read_to_string("testdata/canonical-formats/history-archive-state-v2.json")
        .expect("Failed to read canonical v2 HAS file")
}

#[fixture]
fn canonical_v1_json(canonical_v1_json_str: String) -> serde_json::Value {
    serde_json::from_str(&canonical_v1_json_str).expect("Failed to parse v1 JSON")
}

#[fixture]
fn canonical_v2_json(canonical_v2_json_str: String) -> serde_json::Value {
    serde_json::from_str(&canonical_v2_json_str).expect("Failed to parse v2 JSON")
}

#[fixture]
fn canonical_v1_has(canonical_v1_json_str: String) -> HistoryFileState {
    serde_json::from_str(&canonical_v1_json_str).expect("Failed to parse canonical v1 HAS")
}

#[fixture]
fn canonical_v2_has(canonical_v2_json_str: String) -> HistoryFileState {
    serde_json::from_str(&canonical_v2_json_str).expect("Failed to parse canonical v2 HAS")
}

#[rstest]
fn test_canonical_v1_file_deserialization(canonical_v1_has: HistoryFileState) {
    assert_eq!(canonical_v1_has.version, 1);
    assert_eq!(canonical_v1_has.current_ledger, 67199);
    assert_eq!(
        canonical_v1_has.server.as_deref(),
        Some("stellar-core 22.4.1 (89b9af01e705e076cdc607177d7bb953d36c8d97)")
    );
    assert_eq!(
        canonical_v1_has.network_passphrase.as_deref(),
        Some("Test SDF Network ; September 2015")
    );

    // Validate all bucket levels
    assert_eq!(canonical_v1_has.current_buckets.len(), 11);

    // Level 0
    assert_eq!(
        canonical_v1_has.current_buckets[0].curr,
        "5a7e9c8d4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[0].snap,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(canonical_v1_has.current_buckets[0].next.state, 0);

    // Level 1
    assert_eq!(
        canonical_v1_has.current_buckets[1].curr,
        "9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[1].snap,
        "7f6e5d4c3b2a1098765432109876543210987654321098765432109876543210"
    );
    assert_eq!(canonical_v1_has.current_buckets[1].next.state, 1);
    assert_eq!(
        canonical_v1_has.current_buckets[1].next.output.as_deref(),
        Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
    );

    // Level 2
    assert_eq!(
        canonical_v1_has.current_buckets[2].curr,
        "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[2].snap,
        "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
    );
    assert_eq!(canonical_v1_has.current_buckets[2].next.state, 2);
    assert_eq!(
        canonical_v1_has.current_buckets[2].next.curr.as_deref(),
        Some("aaaa567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
    );
    assert_eq!(
        canonical_v1_has.current_buckets[2].next.snap.as_deref(),
        Some("bbbb567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
    );
    assert_eq!(
        canonical_v1_has.current_buckets[2]
            .next
            .shadow
            .as_ref()
            .map(|v| v.len()),
        Some(2)
    );
    assert_eq!(
        canonical_v1_has.current_buckets[2]
            .next
            .shadow
            .as_ref()
            .unwrap()[0],
        "cccc567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[2]
            .next
            .shadow
            .as_ref()
            .unwrap()[1],
        "dddd567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );

    // Level 3
    assert_eq!(
        canonical_v1_has.current_buckets[3].curr,
        "2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[3].snap,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(canonical_v1_has.current_buckets[3].next.state, 0);

    // Level 4
    assert_eq!(
        canonical_v1_has.current_buckets[4].curr,
        "3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[4].snap,
        "4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e"
    );
    assert_eq!(canonical_v1_has.current_buckets[4].next.state, 0);

    // Level 5
    assert_eq!(
        canonical_v1_has.current_buckets[5].curr,
        "5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[5].snap,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(canonical_v1_has.current_buckets[5].next.state, 0);

    // Level 6
    assert_eq!(
        canonical_v1_has.current_buckets[6].curr,
        "6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[6].snap,
        "7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b"
    );
    assert_eq!(canonical_v1_has.current_buckets[6].next.state, 1);
    assert_eq!(
        canonical_v1_has.current_buckets[6].next.output.as_deref(),
        Some("8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c")
    );

    // Level 7
    assert_eq!(
        canonical_v1_has.current_buckets[7].curr,
        "9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[7].snap,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(canonical_v1_has.current_buckets[7].next.state, 0);

    // Level 8
    assert_eq!(
        canonical_v1_has.current_buckets[8].curr,
        "ad1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[8].snap,
        "be2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f"
    );
    assert_eq!(canonical_v1_has.current_buckets[8].next.state, 0);

    // Level 9
    assert_eq!(
        canonical_v1_has.current_buckets[9].curr,
        "cf3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[9].snap,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(canonical_v1_has.current_buckets[9].next.state, 0);

    // Level 10
    assert_eq!(
        canonical_v1_has.current_buckets[10].curr,
        "d04b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b"
    );
    assert_eq!(
        canonical_v1_has.current_buckets[10].snap,
        "e15c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c"
    );
    assert_eq!(canonical_v1_has.current_buckets[10].next.state, 2);
    assert_eq!(
        canonical_v1_has.current_buckets[10].next.curr.as_deref(),
        Some("f26d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d")
    );
    assert_eq!(
        canonical_v1_has.current_buckets[10].next.snap.as_deref(),
        Some("037e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e")
    );

    // Test that no unexpected fields are present in NextState when state is 0
    for i in [0, 3, 4, 5, 7, 8, 9] {
        assert!(canonical_v1_has.current_buckets[i].next.output.is_none());
        assert!(canonical_v1_has.current_buckets[i].next.curr.is_none());
        assert!(canonical_v1_has.current_buckets[i].next.snap.is_none());
        assert!(canonical_v1_has.current_buckets[i].next.shadow.is_none());
    }

    // Test that only output is present for state 1
    for i in [1, 6] {
        assert!(canonical_v1_has.current_buckets[i].next.output.is_some());
        assert!(canonical_v1_has.current_buckets[i].next.curr.is_none());
        assert!(canonical_v1_has.current_buckets[i].next.snap.is_none());
        assert!(canonical_v1_has.current_buckets[i].next.shadow.is_none());
    }

    // Test that curr and snap are present for state 2
    for i in [2, 10] {
        assert!(canonical_v1_has.current_buckets[i].next.output.is_none());
        assert!(canonical_v1_has.current_buckets[i].next.curr.is_some());
        assert!(canonical_v1_has.current_buckets[i].next.snap.is_some());
    }
}

#[rstest]
fn test_canonical_v1_has_validation(canonical_v1_has: HistoryFileState) {
    assert!(
        canonical_v1_has.validate().is_ok(),
        "Canonical v1 HAS should be valid"
    );
    // Verify v1 doesn't have hotArchiveBuckets
    assert!(canonical_v1_has.hot_archive_buckets.is_none());
}

#[rstest]
fn test_canonical_v2_has_validation(canonical_v2_has: HistoryFileState) {
    assert!(
        canonical_v2_has.validate().is_ok(),
        "Canonical v2 HAS should be valid"
    );
    // Verify v2 has hotArchiveBuckets
    assert!(canonical_v2_has.hot_archive_buckets.is_some());
}

#[rstest]
#[case::remove_server(vec!["server"], true, false)]
#[case::remove_network_passphrase(vec!["networkPassphrase"], false, true)]
#[case::remove_both(vec!["server", "networkPassphrase"], true, true)]
fn test_optional_fields(
    mut canonical_v1_json: serde_json::Value,
    #[case] fields_to_remove: Vec<&str>,
    #[case] expect_server_none: bool,
    #[case] expect_network_passphrase_none: bool,
) {
    // Remove specified optional fields
    for field in &fields_to_remove {
        canonical_v1_json.as_object_mut().unwrap().remove(*field);
    }

    let json_str = serde_json::to_string(&canonical_v1_json).unwrap();
    let has: HistoryFileState = serde_json::from_str(&json_str).unwrap();

    // Validate should pass
    assert!(
        has.validate().is_ok(),
        "HAS should be valid after removing optional fields"
    );

    // Check that excluded fields are None
    if expect_server_none {
        assert!(has.server.is_none(), "server field should be None");
    } else {
        assert!(has.server.is_some(), "server field should be present");
    }

    if expect_network_passphrase_none {
        assert!(
            has.network_passphrase.is_none(),
            "network_passphrase field should be None"
        );
    } else {
        assert!(
            has.network_passphrase.is_some(),
            "network_passphrase field should be present"
        );
    }

    // Also verify that serialization doesn't include None fields
    let serialized = serde_json::to_string(&has).unwrap();
    for field in &fields_to_remove {
        assert!(
            !serialized.contains(&format!("\"{}\"", field)),
            "Serialized JSON should not contain removed field: {}",
            field
        );
    }
}

// Invalid HAS format tests

#[rstest]
fn test_invalid_version(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["version"] = serde_json::json!(3);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidVersion { .. }
    ));
}

#[rstest]
fn test_invalid_current_ledger_zero(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentLedger"] = serde_json::json!(0);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidCurrentLedger { .. }
    ));
}

#[rstest]
fn test_invalid_current_ledger_not_checkpoint(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentLedger"] = serde_json::json!(64);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidCurrentLedger { .. }
    ));
}

#[rstest]
fn test_invalid_bucket_hash_too_short(mut canonical_v1_json: serde_json::Value) {
    // 63 characters - one character too short
    canonical_v1_json["currentBuckets"][0]["curr"] =
        serde_json::json!("abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678");
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_invalid_bucket_hash_too_long(mut canonical_v1_json: serde_json::Value) {
    // 65 characters
    canonical_v1_json["currentBuckets"][0]["curr"] =
        serde_json::json!("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789a");
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_invalid_bucket_hash_non_hex(mut canonical_v1_json: serde_json::Value) {
    // 64 characters but contains non-hex characters
    canonical_v1_json["currentBuckets"][0]["curr"] =
        serde_json::json!("ghij567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_invalid_bucket_hash_empty_string(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["curr"] = serde_json::json!("");
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_valid_bucket_hashes(mut canonical_v1_json: serde_json::Value) {
    // Test valid lowercase hex hash
    canonical_v1_json["currentBuckets"][0]["curr"] =
        serde_json::json!("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789");
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json.clone()).unwrap();
    assert!(
        has.validate().is_ok(),
        "Valid lowercase hex hash should be valid"
    );

    // Test valid hex hash (mixed case)
    canonical_v1_json["currentBuckets"][0]["curr"] =
        serde_json::json!("AbCdEf0123456789abcdef0123456789abcdef0123456789abcdef0123456789");
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    assert!(
        has.validate().is_ok(),
        "Valid hex hash with mixed case should be valid"
    );
}

#[rstest]
fn test_next_state_0_with_output(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 0,
        "output": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidNextState { state: 0, .. }
    ));
}

#[rstest]
fn test_next_state_1_missing_output(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({"state": 1});
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidNextState { state: 1, .. }
    ));
}

#[rstest]
fn test_next_state_2_missing_curr(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 2,
        "snap": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidNextState { state: 2, .. }
    ));
}

#[rstest]
fn test_next_state_2_missing_snap(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 2,
        "curr": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidNextState { state: 2, .. }
    ));
}

#[rstest]
fn test_next_state_2_invalid_curr_hash(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 2,
        "curr": "invalid_hash",
        "snap": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_next_state_2_invalid_snap_hash(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 2,
        "curr": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "snap": "tooshort"
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_next_state_2_invalid_shadow_hash(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 2,
        "curr": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "snap": "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        "shadow": [
            "0000000000000000000000000000000000000000000000000000000000000000",
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678"
        ]
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

#[rstest]
fn test_next_state_2_valid_with_shadow(mut canonical_v1_json: serde_json::Value) {
    // Test with valid curr, snap, and shadow array
    canonical_v1_json["currentBuckets"][0]["next"] = serde_json::json!({
        "state": 2,
        "curr": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "snap": "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        "shadow": [
            "0000000000000000000000000000000000000000000000000000000000000000",
            "1111111111111111111111111111111111111111111111111111111111111111"
        ]
    });
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json.clone()).unwrap();
    assert!(
        has.validate().is_ok(),
        "State 2 with valid curr, snap, and shadow should be valid"
    );

    // Test with empty shadow array
    canonical_v1_json["currentBuckets"][0]["next"]["shadow"] = serde_json::json!([]);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json.clone()).unwrap();
    assert!(
        has.validate().is_ok(),
        "State 2 with empty shadow array should be valid"
    );
}

#[rstest]
fn test_invalid_next_state_value(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json["currentBuckets"][0]["next"]["state"] = serde_json::json!(3);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::InvalidNextStateValue { .. }
    ));
}

// JSON structure tests

#[rstest]
fn test_invalid_json_structure(mut canonical_v1_json: serde_json::Value) {
    canonical_v1_json
        .as_object_mut()
        .unwrap()
        .remove("currentBuckets");

    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        "{}",
        serde_json::to_string(&canonical_v1_json).unwrap()
    )
    .unwrap();

    let json_from_file = fs::read_to_string(temp_file.path()).unwrap();
    let has: Result<HistoryFileState, _> = serde_json::from_str(&json_from_file);
    assert!(
        has.is_err(),
        "Should fail when missing required currentBuckets field"
    );
}

#[rstest]
fn test_wrong_number_of_bucket_levels(mut canonical_v1_json: serde_json::Value) {
    // Replace currentBuckets with only 2 levels instead of 11
    let buckets = canonical_v1_json["currentBuckets"].as_array().unwrap();
    canonical_v1_json["currentBuckets"] =
        serde_json::json!([buckets[0].clone(), buckets[1].clone()]);

    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        "{}",
        serde_json::to_string(&canonical_v1_json).unwrap()
    )
    .unwrap();

    let json_from_file = fs::read_to_string(temp_file.path()).unwrap();
    let has: Result<HistoryFileState, _> = serde_json::from_str(&json_from_file);
    assert!(
        has.is_err(),
        "Should fail with wrong number of bucket levels"
    );
}

// Checkpoint calculation tests

#[test]
fn test_checkpoint_detection() {
    // Valid checkpoints (64n - 1)
    assert!(is_checkpoint(63));
    assert!(is_checkpoint(127));
    assert!(is_checkpoint(191));
    assert!(is_checkpoint(67199));

    // Invalid checkpoints
    assert!(!is_checkpoint(0));
    assert!(!is_checkpoint(1));
    assert!(!is_checkpoint(64));
    assert!(!is_checkpoint(65));
    assert!(!is_checkpoint(128));
    assert!(!is_checkpoint(192));
}

#[rstest]
fn test_get_checkpoint_range(mut canonical_v1_json: serde_json::Value) {
    // Helper function to get checkpoints for a given ledger number
    let mut checkpoints_up_to = |ledger: u32| -> Vec<u32> {
        canonical_v1_json["currentLedger"] = serde_json::json!(ledger);
        let has: HistoryFileState = serde_json::from_value(canonical_v1_json.clone()).unwrap();
        has.get_checkpoint_range()
    };

    // Test various ledger numbers
    assert_eq!(checkpoints_up_to(63), vec![63]);
    assert_eq!(checkpoints_up_to(64), vec![63]);
    assert_eq!(checkpoints_up_to(127), vec![63, 127]);
    assert_eq!(checkpoints_up_to(128), vec![63, 127]);
    assert_eq!(checkpoints_up_to(191), vec![63, 127, 191]);
    assert_eq!(checkpoints_up_to(200), vec![63, 127, 191]);
    assert_eq!(checkpoints_up_to(255), vec![63, 127, 191, 255]);
    assert_eq!(
        checkpoints_up_to(511),
        vec![63, 127, 191, 255, 319, 383, 447, 511]
    );

    // Verify all generated checkpoints are valid
    for ledger in [100, 500, 1000, 10000] {
        let checkpoints = checkpoints_up_to(ledger);
        for checkpoint in checkpoints {
            assert!(
                is_checkpoint(checkpoint),
                "Generated checkpoint {} for ledger {} should be valid",
                checkpoint,
                ledger
            );
        }
    }
}

#[test]
fn test_round_to_lower_checkpoint() {
    // Below genesis should return genesis
    assert_eq!(round_to_lower_checkpoint(0), GENESIS_CHECKPOINT_LEDGER);
    assert_eq!(round_to_lower_checkpoint(1), GENESIS_CHECKPOINT_LEDGER);
    assert_eq!(round_to_lower_checkpoint(62), GENESIS_CHECKPOINT_LEDGER);

    // Checkpoints should return unchanged
    assert_eq!(round_to_lower_checkpoint(63), 63);
    assert_eq!(round_to_lower_checkpoint(127), 127);
    assert_eq!(round_to_lower_checkpoint(191), 191);

    // Non-checkpoints should round down
    assert_eq!(round_to_lower_checkpoint(64), 63);
    assert_eq!(round_to_lower_checkpoint(100), 63);
    assert_eq!(round_to_lower_checkpoint(126), 63);
    assert_eq!(round_to_lower_checkpoint(128), 127);
    assert_eq!(round_to_lower_checkpoint(190), 127);
    assert_eq!(round_to_lower_checkpoint(192), 191);
}

#[test]
fn test_round_to_upper_checkpoint() {
    // Below genesis should return genesis
    assert_eq!(round_to_upper_checkpoint(0), GENESIS_CHECKPOINT_LEDGER);
    assert_eq!(round_to_upper_checkpoint(1), GENESIS_CHECKPOINT_LEDGER);
    assert_eq!(round_to_upper_checkpoint(62), GENESIS_CHECKPOINT_LEDGER);

    // Checkpoints should return unchanged
    assert_eq!(round_to_upper_checkpoint(63), 63);
    assert_eq!(round_to_upper_checkpoint(127), 127);
    assert_eq!(round_to_upper_checkpoint(191), 191);

    // Non-checkpoints should round up
    assert_eq!(round_to_upper_checkpoint(64), 127);
    assert_eq!(round_to_upper_checkpoint(100), 127);
    assert_eq!(round_to_upper_checkpoint(126), 127);
    assert_eq!(round_to_upper_checkpoint(128), 191);
    assert_eq!(round_to_upper_checkpoint(190), 191);
    assert_eq!(round_to_upper_checkpoint(192), 255);
}

#[test]
fn test_count_checkpoints_in_range() {
    // Single checkpoint
    assert_eq!(count_checkpoints_in_range(63, 63), 1);
    assert_eq!(count_checkpoints_in_range(127, 127), 1);

    // Multiple checkpoints
    assert_eq!(count_checkpoints_in_range(63, 127), 2);
    assert_eq!(count_checkpoints_in_range(63, 191), 3);
    assert_eq!(count_checkpoints_in_range(127, 255), 3);
    assert_eq!(count_checkpoints_in_range(63, 255), 4);

    // Invalid range (high < low)
    assert_eq!(count_checkpoints_in_range(127, 63), 0);

    // Large range
    assert_eq!(count_checkpoints_in_range(63, 639), 10);
}

#[test]
fn test_checkpoint_prefix_format() {
    assert_eq!(checkpoint_prefix(0x00000000), "00/00/00");
    assert_eq!(checkpoint_prefix(0x0000003f), "00/00/00");
    assert_eq!(checkpoint_prefix(0x0000007f), "00/00/00");
    assert_eq!(checkpoint_prefix(0x000000ff), "00/00/00");
    assert_eq!(checkpoint_prefix(0x00010000), "00/01/00");
    assert_eq!(checkpoint_prefix(0x01020304), "01/02/03");
    assert_eq!(checkpoint_prefix(0xaabbccdd), "aa/bb/cc");
}

// Unit tests for is_valid_bucket_hash function

#[test]
fn test_is_valid_bucket_hash() {
    // Valid cases
    assert!(
        is_valid_bucket_hash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"),
        "Valid lowercase hex should be valid"
    );
    assert!(
        is_valid_bucket_hash("ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"),
        "Valid uppercase hex should be valid"
    );
    assert!(
        is_valid_bucket_hash("AbCdEf0123456789aBcDeF0123456789AbCdEf0123456789aBcDeF0123456789"),
        "Valid mixed case hex should be valid"
    );

    // Invalid cases - wrong length
    assert!(!is_valid_bucket_hash(""), "Empty string should be invalid");
    assert!(
        !is_valid_bucket_hash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678"),
        "63 chars should be invalid"
    );
    assert!(
        !is_valid_bucket_hash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567890"),
        "65 chars should be invalid"
    );

    // Invalid cases - non-hex characters
    assert!(
        !is_valid_bucket_hash("ghij567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
        "Non-hex chars (g,h,i,j) should be invalid"
    );
    assert!(
        !is_valid_bucket_hash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678!"),
        "Special char (!) should be invalid"
    );
    assert!(
        !is_valid_bucket_hash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678 "),
        "Space should be invalid"
    );
}

// Version 2 HAS tests

#[rstest]
fn test_canonical_v2_file_deserialization(canonical_v2_has: HistoryFileState) {
    assert_eq!(canonical_v2_has.version, 2);
    assert_eq!(canonical_v2_has.current_ledger, 67199);
    assert_eq!(
        canonical_v2_has.server.as_deref(),
        Some("stellar-core 22.4.1 (89b9af01e705e076cdc607177d7bb953d36c8d97)")
    );
    assert_eq!(
        canonical_v2_has.network_passphrase.as_deref(),
        Some("Test SDF Network ; September 2015")
    );

    // Verify v2 has hotArchiveBuckets
    assert!(canonical_v2_has.hot_archive_buckets.is_some());
    let hot_buckets = canonical_v2_has.hot_archive_buckets.as_ref().unwrap();
    assert_eq!(hot_buckets.len(), 11);

    // Check a few hot bucket values
    assert_eq!(
        hot_buckets[0].curr,
        "1a2b3c4d5e6f7081928394a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5"
    );
    assert_eq!(
        hot_buckets[1].curr,
        "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a"
    );
    assert_eq!(hot_buckets[1].next.state, 1);
    assert_eq!(
        hot_buckets[1].next.output.as_deref(),
        Some("4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c")
    );
}

#[rstest]
fn test_version_2_missing_hot_archive_buckets(mut canonical_v2_json: serde_json::Value) {
    // Remove hotArchiveBuckets from a version 2 HAS
    canonical_v2_json
        .as_object_mut()
        .unwrap()
        .remove("hotArchiveBuckets");

    let has: HistoryFileState = serde_json::from_value(canonical_v2_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MissingHotBuckets { version: 2, .. }
    ));
}

#[rstest]
fn test_version_1_with_hot_archive_buckets(
    mut canonical_v1_json: serde_json::Value,
    canonical_v2_json: serde_json::Value,
) {
    // Add hotArchiveBuckets from v2 to a version 1 HAS
    canonical_v1_json["hotArchiveBuckets"] = canonical_v2_json["hotArchiveBuckets"].clone();

    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MissingHotBuckets { version: 1, .. }
    ));
}

#[rstest]
fn test_version_1_buckets_method(canonical_v1_has: HistoryFileState) {
    let buckets = canonical_v1_has.buckets();

    // Should not contain zero hashes
    assert!(!buckets
        .contains(&"0000000000000000000000000000000000000000000000000000000000000000".to_string()));

    // Check some known buckets from v1
    assert!(buckets
        .contains(&"5a7e9c8d4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d".to_string()));
    assert!(buckets
        .contains(&"9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c".to_string()));

    // Verify we have a reasonable number of unique buckets
    assert!(
        buckets.len() > 10,
        "Should have collected many unique bucket hashes"
    );
}

#[rstest]
fn test_version_2_buckets_method(canonical_v2_has: HistoryFileState) {
    let buckets = canonical_v2_has.buckets();

    // The buckets() method should collect all unique non-zero hashes from both
    // currentBuckets and hotArchiveBuckets

    // Should not contain zero hashes
    assert!(!buckets
        .contains(&"0000000000000000000000000000000000000000000000000000000000000000".to_string()));

    // Check that we got buckets from both arrays
    // From currentBuckets
    assert!(buckets
        .contains(&"5a7e9c8d4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d".to_string()));

    // From hotArchiveBuckets
    assert!(buckets
        .contains(&"1a2b3c4d5e6f7081928394a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5".to_string()));

    // Verify we have a reasonable number of unique buckets
    assert!(
        buckets.len() > 20,
        "Should have collected many unique bucket hashes"
    );
}

#[rstest]
fn test_version_2_hot_bucket_validation(mut canonical_v2_json: serde_json::Value) {
    // Modify first hot bucket to have an invalid hash
    canonical_v2_json["hotArchiveBuckets"][0]["curr"] = serde_json::json!("invalid_hash");

    let has: HistoryFileState = serde_json::from_value(canonical_v2_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}
