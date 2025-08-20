//! Tests for mirror edge cases combining cli flags with interesting destination/source states

use std::path::PathBuf;
use stellar_archivist::test_helpers::{run_mirror, run_scan, MirrorConfig, ScanConfig};
use tempfile::TempDir;

/// Get the test archive path
fn get_test_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

// Gap Validation Tests

#[tokio::test]
async fn test_mirror_allow_gap_flag() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Mirror up to ledger 500
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(500),
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(config).await.unwrap();

    // Test with low == 400 (before existing destination end of 500)
    // Should succeed without --allow-mirror-gaps flag since no gap is created
    let continue_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(400), // Before destination's end - no gap created
        high: Some(1000),
        overwrite: false,
        allow_mirror_gaps: false, // Not needed since no gap is created
    };

    run_mirror(continue_config).await.unwrap();

    // Verify continuity - the archive should continue from checkpoint 511 (for ledger 500)
    let checkpoint_after_500 = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("03")
        .join("history-000003ff.json"); // Checkpoint 1023 (around ledger 1000)
    assert!(
        checkpoint_after_500.exists(),
        "Archive should continue past original end"
    );

    // Try to mirror with --low that would create a gap
    // This should fail because allow_mirror_gaps is false
    let gap_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // This creates a gap
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false, // Default: gaps not allowed
    };

    let result = run_mirror(gap_config).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Cannot mirror") && err_msg.contains("would create a gap"));

    // Try with --overwrite flag (should still fail)
    let overwrite_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // Still creates a gap
        high: Some(3000),
        overwrite: true,          // Overwrite does not bypass gap checks
        allow_mirror_gaps: false, // Gap not allowed
    };

    let overwrite_result = run_mirror(overwrite_config).await;
    assert!(overwrite_result.is_err());

    let overwrite_err_msg = overwrite_result.unwrap_err().to_string();
    assert!(
        overwrite_err_msg.contains("Cannot mirror")
            && overwrite_err_msg.contains("would create a gap")
    );

    // Now try the same mirror with --allow-mirror-gaps flag
    // This should succeed with a warning
    let gap_allowed_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // This creates a gap
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: true, // Allow the gap
    };

    // Should succeed despite the gap
    run_mirror(gap_allowed_config).await.unwrap();

    // Verify that files from the new range exist
    let later_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("07")
        .join("history-000007bf.json");
    assert!(later_history.exists());

    // But files in the gap should NOT exist
    let gap_history = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("05")
        .join("history-000005ff.json"); // Ledger ~1500 in the gap
    assert!(!gap_history.exists());

    // Test with both --overwrite and --allow-mirror-gaps flags
    let both_flags_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(3500), // Would create a gap
        high: Some(4000),
        overwrite: true,         // Overwrite files
        allow_mirror_gaps: true, // Allow the gap
    };

    // Should succeed with both flags
    run_mirror(both_flags_config).await.unwrap();
}

#[tokio::test]
async fn test_mirror_allows_gaps_for_empty_destination() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("empty_dest");

    // Mirror with --low to an empty destination
    // Should succeed without gap check since there's no existing archive
    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000), // High starting point
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false, // Not needed for empty destination
    };

    // Should succeed - no existing archive means no gap
    run_mirror(config).await.unwrap();

    // Check that checkpoint below low does not exist, e.g. 1023 (0x3ff)
    let before_low_checkpoint = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("03")
        .join("history-000003ff.json");
    assert!(
        !before_low_checkpoint.exists(),
        "Files before the low value should not exist"
    );

    // Scan with no low should fail because there's a gap
    let scan_no_low = ScanConfig {
        archive: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: None,
    };

    let scan_result = run_scan(scan_no_low).await;
    assert!(
        scan_result.is_err(),
        "Scan without low should fail when archive has gap at beginning"
    );

    // Followup scan with low 2000 should succeed
    let scan_with_low = ScanConfig {
        archive: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(2000),
        high: None,
    };

    run_scan(scan_with_low).await.unwrap();
}

//=============================================================================
// Low/Resume Behavior Tests
//=============================================================================

#[tokio::test]
async fn test_mirror_overwrite() {
    let test_archive_path = get_test_archive_path();
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");

    // Mirror up to ledger 2000 to establish destination state
    let initial_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(2000),
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(initial_config).await.unwrap();

    // Get files right before and after low=500
    let before_low_file = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("01")
        .join("history-0000017f.json");

    let after_low_file = dest_path
        .join("history")
        .join("00")
        .join("00")
        .join("01")
        .join("history-000001bf.json");

    // Capture initial timestamps
    let before_low_metadata_initial = std::fs::metadata(&before_low_file).unwrap();
    let before_low_modified_initial = before_low_metadata_initial.modified().unwrap();

    let after_low_metadata_initial = std::fs::metadata(&after_low_file).unwrap();
    let after_low_modified_initial = after_low_metadata_initial.modified().unwrap();

    // Sleep briefly to ensure timestamp difference if files were to be rewritten
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Mirror with --low 500 without --overwrite
    // This should ignore --low and resume from 2001
    let resume_without_overwrite = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(500),
        high: Some(3000),
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(resume_without_overwrite).await.unwrap();

    // Verify both files were not re-downloaded
    let before_low_metadata_after_resume = std::fs::metadata(&before_low_file).unwrap();
    let before_low_modified_after_resume = before_low_metadata_after_resume.modified().unwrap();

    let after_low_metadata_after_resume = std::fs::metadata(&after_low_file).unwrap();
    let after_low_modified_after_resume = after_low_metadata_after_resume.modified().unwrap();

    assert_eq!(
        before_low_modified_initial,
        before_low_modified_after_resume
    );
    assert_eq!(after_low_modified_initial, after_low_modified_after_resume);

    // Sleep again before overwrite test
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Mirror with --low 500 with --overwrite
    let overwrite_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(500), // Will round down to checkpoint 447
        high: Some(3000),
        overwrite: true, // WITH overwrite - should honor --low
        allow_mirror_gaps: false,
    };

    run_mirror(overwrite_config).await.unwrap();

    // Verify file before low was not re-downloaded
    let before_low_metadata_after_overwrite = std::fs::metadata(&before_low_file).unwrap();
    let before_low_modified_after_overwrite =
        before_low_metadata_after_overwrite.modified().unwrap();

    // Verify file after low was re-downloaded
    let after_low_metadata_after_overwrite = std::fs::metadata(&after_low_file).unwrap();
    let after_low_modified_after_overwrite = after_low_metadata_after_overwrite.modified().unwrap();

    assert_eq!(
        before_low_modified_initial,
        before_low_modified_after_overwrite
    );
    assert_ne!(
        after_low_modified_initial,
        after_low_modified_after_overwrite
    );
}
