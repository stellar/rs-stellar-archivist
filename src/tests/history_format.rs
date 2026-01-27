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
    // Verify top-level fields
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
    assert_eq!(canonical_v1_has.current_buckets.len(), 11);

    // Spot-check representative bucket levels for each next.state value:
    // - State 0: no next fields
    // - State 1: has output field
    // - State 2: has curr, snap, and optionally shadow

    // Level 0: state 0, no next fields
    let level0 = &canonical_v1_has.current_buckets[0];
    assert_eq!(
        level0.curr,
        "5a7e9c8d4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d6c5a4b3f2e1d"
    );
    assert_eq!(
        level0.snap,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(level0.next.state, 0);
    assert!(level0.next.output.is_none());
    assert!(level0.next.curr.is_none());

    // Level 1: state 1, has output field
    let level1 = &canonical_v1_has.current_buckets[1];
    assert_eq!(level1.next.state, 1);
    assert_eq!(
        level1.next.output.as_deref(),
        Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
    );
    assert!(level1.next.curr.is_none());
    assert!(level1.next.snap.is_none());

    // Level 2: state 2, has curr, snap, and shadow
    let level2 = &canonical_v1_has.current_buckets[2];
    assert_eq!(level2.next.state, 2);
    assert!(level2.next.output.is_none());
    assert_eq!(
        level2.next.curr.as_deref(),
        Some("aaaa567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
    );
    assert_eq!(
        level2.next.snap.as_deref(),
        Some("bbbb567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
    );
    let shadow = level2
        .next
        .shadow
        .as_ref()
        .expect("Level 2 should have shadow");
    assert_eq!(shadow.len(), 2);
    assert_eq!(
        shadow[0],
        "cccc567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
    );

    // Level 10: state 2, has curr and snap but no shadow
    let level10 = &canonical_v1_has.current_buckets[10];
    assert_eq!(level10.next.state, 2);
    assert!(level10.next.curr.is_some());
    assert!(level10.next.snap.is_some());
    assert!(level10.next.shadow.is_none());
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
            !serialized.contains(&format!("\"{field}\"")),
            "Serialized JSON should not contain removed field: {field}"
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
#[case::zero(0, "zero is not a valid ledger")]
#[case::not_checkpoint(64, "64 is not on a checkpoint boundary")]
#[case::off_by_one(62, "62 is one less than valid checkpoint 63")]
fn test_invalid_current_ledger(
    mut canonical_v1_json: serde_json::Value,
    #[case] ledger: u32,
    #[case] _description: &str,
) {
    canonical_v1_json["currentLedger"] = serde_json::json!(ledger);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(
        matches!(
            error,
            crate::history_format::Error::InvalidCurrentLedger { .. }
        ),
        "Expected InvalidCurrentLedger for ledger {ledger}, got {error:?}"
    );
}

// Valid hash constant for tests
const VALID_HASH: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
const VALID_HASH2: &str = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";
const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Helper to build next state JSON for testing
fn next_state_json(state: u32, fields: &[(&str, serde_json::Value)]) -> serde_json::Value {
    let mut obj = serde_json::json!({"state": state});
    for (key, value) in fields {
        obj[*key] = value.clone();
    }
    obj
}

// Tests for invalid next state structure (missing/extra fields)
#[rstest]
// State 0 (idle) should have no fields - output is not allowed
#[case::state0_with_output(0, &[("output", serde_json::json!(VALID_HASH))], 0)]
// State 1 (running) requires output field
#[case::state1_missing_output(1, &[], 1)]
// State 2 (merging) requires both curr and snap fields
#[case::state2_missing_curr(2, &[("snap", serde_json::json!(VALID_HASH))], 2)]
#[case::state2_missing_snap(2, &[("curr", serde_json::json!(VALID_HASH))], 2)]
fn test_invalid_next_state_structure(
    mut canonical_v1_json: serde_json::Value,
    #[case] state: u32,
    #[case] fields: &[(&str, serde_json::Value)],
    #[case] expected_state: u32,
) {
    canonical_v1_json["currentBuckets"][0]["next"] = next_state_json(state, fields);
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(
        matches!(error, crate::history_format::Error::InvalidNextState { state: s, .. } if s == expected_state),
        "Expected InvalidNextState with state {expected_state}, got {error:?}"
    );
}

// Tests for invalid hashes in next state fields (state 2 merging)
#[rstest]
// curr field must be valid 64-char hex
#[case::invalid_curr("invalid_hash", VALID_HASH, None)]
// snap field must be valid 64-char hex
#[case::invalid_snap(VALID_HASH, "tooshort", None)]
// shadow array entries must all be valid 64-char hex
#[case::invalid_shadow(VALID_HASH, VALID_HASH2, Some(vec![ZERO_HASH, "bad_shadow_hash"]))]
fn test_invalid_hash_in_next_state(
    mut canonical_v1_json: serde_json::Value,
    #[case] curr: &str,
    #[case] snap: &str,
    #[case] shadow: Option<Vec<&str>>,
) {
    let mut next = serde_json::json!({"state": 2, "curr": curr, "snap": snap});
    if let Some(s) = shadow {
        next["shadow"] = serde_json::json!(s);
    }
    canonical_v1_json["currentBuckets"][0]["next"] = next;
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    let error = has.validate().unwrap_err();
    assert!(matches!(
        error,
        crate::history_format::Error::MalformedBucketHash { .. }
    ));
}

// Tests for valid next state 2 (merging) configurations
#[rstest]
// Shadow array with valid hashes is allowed
#[case::with_shadow(Some(vec![ZERO_HASH, "1111111111111111111111111111111111111111111111111111111111111111"]))]
// Empty shadow array is allowed
#[case::empty_shadow(Some(vec![]))]
// Shadow field is optional
#[case::no_shadow(None)]
fn test_valid_next_state_2(
    mut canonical_v1_json: serde_json::Value,
    #[case] shadow: Option<Vec<&str>>,
) {
    let mut next = serde_json::json!({"state": 2, "curr": VALID_HASH, "snap": VALID_HASH2});
    if let Some(s) = shadow {
        next["shadow"] = serde_json::json!(s);
    }
    canonical_v1_json["currentBuckets"][0]["next"] = next;
    let has: HistoryFileState = serde_json::from_value(canonical_v1_json).unwrap();
    assert!(has.validate().is_ok());
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
                "Generated checkpoint {checkpoint} for ledger {ledger} should be valid"
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
