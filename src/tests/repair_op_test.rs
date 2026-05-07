//! Tests for repair operation
//!
//! Covers: existence-only repair, verify mode repair, cross-validation repair,
//! manual mode, dry run, error handling, idempotency, and CLI validation.

use super::utils::{
    copy_testnet_small_archive, file_url_from_path, get_files_by_pattern, start_http_server,
    testnet_small_archive_path,
};
use crate::history_format;
use crate::test_helpers::{
    run_mirror, run_repair, run_scan, MirrorConfig, RepairConfig, ScanConfig,
};
use flate2::write::GzEncoder;
use flate2::Compression;
use rstest::rstest;
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

//=============================================================================
// Helper Functions
//=============================================================================

/// Mirror the testnet-small archive to a temp directory and return (source_url, dest_dir, dest_url)
async fn mirror_testnet_small() -> (String, TempDir, String) {
    let src_url = file_url_from_path(&testnet_small_archive_path());
    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());

    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("Mirror setup should succeed");

    (src_url, dest_dir, dest_url)
}

/// Delete a specific file pattern from the archive, returns the deleted file path (relative)
fn delete_first_file(archive_path: &Path, pattern: &str) -> String {
    let files = get_files_by_pattern(archive_path, pattern);
    assert!(!files.is_empty(), "No files matching pattern '{pattern}'");
    let file = &files[0];
    let relative = file
        .strip_prefix(archive_path)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::remove_file(file).expect("Failed to delete file");
    relative
}

/// Corrupt a bucket file with garbage bytes (invalid gzip)
fn corrupt_bucket_invalid_gzip(archive_path: &Path) -> String {
    let files = get_files_by_pattern(archive_path, "/bucket-");
    assert!(!files.is_empty(), "No bucket files found");
    let file = &files[0];
    let relative = file
        .strip_prefix(archive_path)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::write(file, b"this is not valid gzip data at all").unwrap();
    relative
}

/// Corrupt a bucket file with valid gzip but wrong hash
fn corrupt_bucket_hash_mismatch(archive_path: &Path) -> String {
    let files = get_files_by_pattern(archive_path, "/bucket-");
    assert!(!files.is_empty(), "No bucket files found");
    let file = &files[0];
    let relative = file
        .strip_prefix(archive_path)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(b"Valid gzip but wrong hash content!")
        .unwrap();
    let compressed = encoder.finish().unwrap();
    std::fs::write(file, compressed).unwrap();
    relative
}

/// Corrupt an XDR file with garbage bytes
fn corrupt_xdr_file(archive_path: &Path, pattern: &str) -> String {
    let files = get_files_by_pattern(archive_path, pattern);
    assert!(!files.is_empty(), "No files matching pattern '{pattern}'");
    let file = &files[0];
    let relative = file
        .strip_prefix(archive_path)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::write(file, b"garbage invalid gzip bytes").unwrap();
    relative
}

/// Corrupt an XDR file by flipping bytes (XOR with 0xFF)
fn corrupt_xdr_flip_bytes(archive_path: &Path, pattern: &str) -> String {
    let files = get_files_by_pattern(archive_path, pattern);
    assert!(!files.is_empty(), "No files matching pattern '{pattern}'");
    let file = &files[0];
    let relative = file
        .strip_prefix(archive_path)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let mut data = std::fs::read(file).unwrap();
    for byte in &mut data {
        *byte ^= 0xFF;
    }
    std::fs::write(file, data).unwrap();
    relative
}

//=============================================================================
// A. Existence-Only Mode (no --verify)
//=============================================================================

#[rstest]
#[case::history("/history-", "history")]
#[case::ledger("/ledger-", "ledger")]
#[case::transactions("/transactions-", "transactions")]
#[case::results("/results-", "results")]
#[case::scp("/scp-", "scp")]
#[tokio::test]
async fn test_repair_missing_checkpoint_file(#[case] pattern: &str, #[case] category: &str) {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Delete a file
    let deleted = delete_first_file(dest_dir.path(), pattern);

    // Verify scan detects the missing file
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err(&format!("Scan should detect missing {category}"));

    // Repair
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    // Verify scan passes after repair
    run_scan(ScanConfig::new(&dest_url))
        .await
        .unwrap_or_else(|e| panic!("Scan should pass after repairing {category}: {deleted}: {e}"));
}

#[tokio::test]
async fn test_repair_missing_bucket() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    delete_first_file(dest_dir.path(), "/bucket-");

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err("Scan should detect missing bucket");

    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repair");
}

#[tokio::test]
async fn test_repair_missing_multiple_categories() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    delete_first_file(dest_dir.path(), "/ledger-");
    delete_first_file(dest_dir.path(), "/transactions-");
    delete_first_file(dest_dir.path(), "/bucket-");

    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repairing multiple categories");
}

//=============================================================================
// .well-known Edge Cases
//=============================================================================

#[tokio::test]
async fn test_repair_missing_well_known() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json"))
        .expect("Failed to delete .well-known");

    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed with missing .well-known");

    // Verify .well-known was restored
    assert!(
        dest_dir
            .path()
            .join(".well-known/stellar-history.json")
            .exists(),
        ".well-known should be restored"
    );

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repair");
}

#[tokio::test]
async fn test_repair_corrupt_well_known() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    std::fs::write(
        dest_dir.path().join(".well-known/stellar-history.json"),
        "{ invalid json }}",
    )
    .unwrap();

    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed with corrupt .well-known");

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repair");
}

#[tokio::test]
async fn test_repair_does_not_update_well_known() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Record the .well-known content before repair
    let wk_path = dest_dir.path().join(".well-known/stellar-history.json");
    let before = std::fs::read_to_string(&wk_path).unwrap();

    // Delete a non-.well-known file and repair
    delete_first_file(dest_dir.path(), "/ledger-");
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    // .well-known should be unchanged
    let after = std::fs::read_to_string(&wk_path).unwrap();
    assert_eq!(
        before, after,
        ".well-known should not be modified by repair"
    );
}

//=============================================================================
// Empty File Handling
//=============================================================================

#[tokio::test]
async fn test_repair_replaces_empty_files() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Truncate a ledger file to 0 bytes
    let files = get_files_by_pattern(dest_dir.path(), "/ledger-");
    std::fs::write(&files[0], "").unwrap();

    // Scan detects empty files as corrupt (even without --verify)
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err("Scan should detect empty ledger file");

    // Repair should replace the empty file
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    let metadata = std::fs::metadata(&files[0]).unwrap();
    assert!(
        metadata.len() > 0,
        "Empty file should be replaced by repair"
    );

    // Archive should now be valid
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repair");
}

#[tokio::test]
async fn test_repair_verify_replaces_empty_files() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let files = get_files_by_pattern(dest_dir.path(), "/ledger-");
    std::fs::write(&files[0], "").unwrap();

    // Repair with --verify should also replace empty files
    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("Repair --verify should succeed");

    let metadata = std::fs::metadata(&files[0]).unwrap();
    assert!(
        metadata.len() > 0,
        "Empty file should be replaced with --verify"
    );
}

//=============================================================================
// Nothing to Repair
//=============================================================================

#[tokio::test]
async fn test_repair_healthy_archive() {
    let (src_url, _dest_dir, dest_url) = mirror_testnet_small().await;

    // Repair on healthy archive = nothing to do
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed on healthy archive");
}

//=============================================================================
// Range Constraints
//=============================================================================

#[tokio::test]
async fn test_repair_skip_optional_skips_scp() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Delete both SCP and ledger files
    delete_first_file(dest_dir.path(), "/scp-");
    let deleted_ledger = delete_first_file(dest_dir.path(), "/ledger-");

    // Repair with --skip-optional: only ledger should be fixed
    run_repair(RepairConfig::new(&src_url, &dest_url).skip_optional())
        .await
        .expect("Repair should succeed");

    // Ledger should be restored
    assert!(
        dest_dir.path().join(&deleted_ledger).exists(),
        "Ledger should be restored"
    );

    // SCP should still be missing (scan without skip-optional should fail)
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err("SCP file should still be missing");

    // But scan with skip-optional should pass
    run_scan(ScanConfig::new(&dest_url).skip_optional())
        .await
        .expect("Scan should pass with skip-optional after repair");
}

//=============================================================================
// B. Verification Mode — Per-file corruption
//=============================================================================

#[tokio::test]
async fn test_repair_verify_bucket_invalid_gzip() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_bucket_invalid_gzip(dest_dir.path());

    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("Repair --verify should fix invalid gzip bucket");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("Scan --verify should pass after repair");
}

#[tokio::test]
async fn test_repair_verify_bucket_hash_mismatch() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_bucket_hash_mismatch(dest_dir.path());

    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("Repair --verify should fix hash-mismatched bucket");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("Scan --verify should pass after repair");
}

#[tokio::test]
async fn test_repair_verify_empty_bucket() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let files = get_files_by_pattern(dest_dir.path(), "/bucket-");
    std::fs::write(&files[0], "").unwrap();

    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("Repair --verify should fix empty bucket");

    let metadata = std::fs::metadata(&files[0]).unwrap();
    assert!(
        metadata.len() > 0,
        "Bucket should have content after repair"
    );
}

#[rstest]
#[case::ledger("/ledger-", "ledger")]
#[case::transactions("/transactions-", "transactions")]
#[case::results("/results-", "results")]
#[tokio::test]
async fn test_repair_verify_corrupt_xdr_invalid_gzip(
    #[case] pattern: &str,
    #[case] category: &str,
) {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_xdr_file(dest_dir.path(), pattern);

    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .unwrap_or_else(|e| panic!("Repair --verify should fix corrupt {category}: {e}"));

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("Scan --verify should pass after repair");
}

#[rstest]
#[case::ledger("/ledger-", "ledger")]
#[case::transactions("/transactions-", "transactions")]
#[case::results("/results-", "results")]
#[tokio::test]
async fn test_repair_verify_corrupt_xdr_flipped_bytes(
    #[case] pattern: &str,
    #[case] category: &str,
) {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_xdr_flip_bytes(dest_dir.path(), pattern);

    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .unwrap_or_else(|e| {
            panic!("Repair --verify should fix {category} with flipped bytes: {e}")
        });

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("Scan --verify should pass after repair");
}

#[tokio::test]
async fn test_repair_verify_corrupt_history_json() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Corrupt a history file (not .well-known)
    let history_files: Vec<_> = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();
    assert!(!history_files.is_empty());
    std::fs::write(&history_files[0], "{ invalid json }}").unwrap();

    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("Repair --verify should fix corrupt history JSON");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("Scan --verify should pass after repair");
}

#[tokio::test]
async fn test_repair_verify_corrupt_scp() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_xdr_file(dest_dir.path(), "/scp-");

    // Repair with verify and skip_optional=false
    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("Repair --verify should fix corrupt SCP");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("Scan --verify should pass after SCP repair");
}

//=============================================================================
// Without --verify Ignores Corruption (Regression Guard)
//=============================================================================

#[tokio::test]
async fn test_repair_no_verify_ignores_corrupt_bucket() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_bucket_hash_mismatch(dest_dir.path());

    // Repair without --verify should NOT fix this
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed (file exists, no verification)");

    // The file should still be corrupt (scan --verify should fail)
    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect_err("Scan --verify should still fail on corrupt bucket");
}

#[tokio::test]
async fn test_repair_no_verify_ignores_corrupt_xdr() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_xdr_flip_bytes(dest_dir.path(), "/ledger-");

    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed (file exists, no verification)");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect_err("Scan --verify should still fail on corrupt ledger");
}

//=============================================================================
// D. Manual Mode
//=============================================================================

#[tokio::test]
async fn test_repair_manual_specific_files() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Delete 3 files
    let deleted_ledger = delete_first_file(dest_dir.path(), "/ledger-");
    let deleted_tx = delete_first_file(dest_dir.path(), "/transactions-");
    let _deleted_results = delete_first_file(dest_dir.path(), "/results-");

    // Only repair 2 of the 3
    run_repair(
        RepairConfig::new(&src_url, &dest_url)
            .files(vec![deleted_ledger.clone(), deleted_tx.clone()]),
    )
    .await
    .expect("Manual repair should succeed");

    // Ledger and transactions should be restored
    assert!(dest_dir.path().join(&deleted_ledger).exists());
    assert!(dest_dir.path().join(&deleted_tx).exists());

    // Results should still be missing (wasn't in the repair list)
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err("Results file should still be missing");
}

#[tokio::test]
async fn test_repair_manual_empty_list() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    delete_first_file(dest_dir.path(), "/ledger-");

    // Repair with empty file list
    run_repair(RepairConfig::new(&src_url, &dest_url).files(vec![]))
        .await
        .expect("Repair with empty list should succeed (no-op)");

    // File should still be missing
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err("Deleted file should still be missing");
}

#[tokio::test]
async fn test_repair_manual_nonexistent_source_file() {
    let (src_url, _dest_dir, dest_url) = mirror_testnet_small().await;

    // Try to repair a file that doesn't exist in source
    let result = run_repair(RepairConfig::new(&src_url, &dest_url).files(vec![
        "ledger/00/00/00/ledger-nonexistent.xdr.gz".to_string(),
    ]))
    .await;

    assert!(
        result.is_err(),
        "Repair should fail for nonexistent source file"
    );
}

//=============================================================================
// E. Dry Run
//=============================================================================

#[tokio::test]
async fn test_repair_dry_run_reports_but_no_download() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let deleted = delete_first_file(dest_dir.path(), "/ledger-");

    // History files: dry-run must not write history-*.json regardless of
    // whether dst is missing or corrupt (Pipeline used to write the src
    // buffer back via process_buffer even in dry-run).
    let history_files = get_files_by_pattern(dest_dir.path(), "/history-");
    assert!(
        history_files.len() >= 2,
        "Need at least two history files to exercise both missing and corrupt cases"
    );

    let deleted_history = history_files[0].clone();
    let deleted_history_relative = deleted_history
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::remove_file(&deleted_history).unwrap();

    let corrupted_history = history_files[1].clone();
    let corrupted_content: &[u8] = b"{ invalid json garbage }}";
    std::fs::write(&corrupted_history, corrupted_content).unwrap();

    // Dry run should not actually download anything
    run_repair(RepairConfig::new(&src_url, &dest_url).dry_run())
        .await
        .expect("Dry run should succeed");

    // Ledger file should still be missing
    assert!(
        !dest_dir.path().join(&deleted).exists(),
        "Dry run should not restore deleted file"
    );

    // Deleted history file should still be missing
    assert!(
        !dest_dir.path().join(&deleted_history_relative).exists(),
        "Dry run should not restore deleted history file"
    );

    // Corrupted history file should still have its corrupt content
    assert_eq!(
        std::fs::read(&corrupted_history).unwrap(),
        corrupted_content,
        "Dry run should not overwrite corrupted history file"
    );
}

#[tokio::test]
async fn test_repair_dry_run_with_verify() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    corrupt_bucket_invalid_gzip(dest_dir.path());

    // Dry run with verify should detect but not fix
    run_repair(RepairConfig::new(&src_url, &dest_url).verify().dry_run())
        .await
        .expect("Dry run with verify should succeed");

    // Bucket should still be corrupt
    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect_err("Bucket should still be corrupt after dry run");
}

#[tokio::test]
async fn test_repair_dry_run_manual_mode() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let deleted = delete_first_file(dest_dir.path(), "/ledger-");

    run_repair(
        RepairConfig::new(&src_url, &dest_url)
            .files(vec![deleted.clone()])
            .dry_run(),
    )
    .await
    .expect("Dry run manual mode should succeed");

    assert!(
        !dest_dir.path().join(&deleted).exists(),
        "Dry run should not download anything"
    );
}

//=============================================================================
// H. Idempotency and Preserving Good State
//=============================================================================

#[tokio::test]
async fn test_repair_idempotent() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    delete_first_file(dest_dir.path(), "/ledger-");

    // First repair
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("First repair should succeed");

    // Second repair (nothing to fix)
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Second repair should succeed (idempotent)");

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass");
}

#[tokio::test]
async fn test_repair_preserves_good_files() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Record mtime of a good file
    let good_files = get_files_by_pattern(dest_dir.path(), "/results-");
    assert!(!good_files.is_empty());
    let good_file_mtime = std::fs::metadata(&good_files[0])
        .unwrap()
        .modified()
        .unwrap();

    // Sleep briefly to ensure mtime would differ if file were rewritten
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Delete a different file and repair
    delete_first_file(dest_dir.path(), "/ledger-");
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    // Good file should not have been touched
    let after_mtime = std::fs::metadata(&good_files[0])
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        good_file_mtime, after_mtime,
        "Good files should not be modified by repair"
    );
}

//=============================================================================
// I. CLI Validation
//=============================================================================

#[tokio::test]
async fn test_repair_rejects_readonly_destination() {
    let src_url = file_url_from_path(&testnet_small_archive_path());

    let result = run_repair(RepairConfig::new(&src_url, "http://example.com/archive")).await;
    assert!(
        result.is_err(),
        "Repair should reject read-only destination"
    );
}

//=============================================================================
// J. HTTP Source Integration
//=============================================================================

#[tokio::test]
async fn test_repair_http_source_to_filesystem() {
    let archive_path = testnet_small_archive_path();
    let (server_url, server_handle) = start_http_server(&archive_path).await;

    // Mirror from HTTP to local
    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());

    run_mirror(MirrorConfig::new(&server_url, &dest_url))
        .await
        .expect("Mirror from HTTP should succeed");

    // Delete a file
    delete_first_file(dest_dir.path(), "/ledger-");

    // Repair from HTTP source
    run_repair(RepairConfig::new(&server_url, &dest_url))
        .await
        .expect("Repair from HTTP source should succeed");

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after HTTP repair");

    server_handle.abort();
}

//=============================================================================
// G. Download Verification (Phase 2 integrity)
//=============================================================================

#[tokio::test]
async fn test_repair_verify_validates_downloads() {
    // Set up: destination missing a bucket, source has corrupt bucket
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());

    // Mirror from source to destination first
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("Mirror should succeed");

    // Now corrupt the bucket in source AND delete it from destination
    let bucket_files = get_files_by_pattern(src_dir.path(), "/bucket-");
    let bucket_file = &bucket_files[0];
    let relative = bucket_file
        .strip_prefix(src_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    // Corrupt source bucket
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(b"wrong content for hash").unwrap();
    let compressed = encoder.finish().unwrap();
    std::fs::write(bucket_file, compressed).unwrap();

    // Delete destination bucket
    std::fs::remove_file(dest_dir.path().join(&relative)).unwrap();

    // Repair with --verify should fail (source provides corrupt data)
    let result = run_repair(RepairConfig::new(&src_url, &dest_url).verify()).await;
    assert!(
        result.is_err(),
        "Repair --verify should fail when source has corrupt data"
    );
}

//=============================================================================
// K. Two-Wave Bucket Discovery (history file → bucket reference)
//=============================================================================

/// Helper: parse a history file and return the bucket hashes it references
fn buckets_from_history_file(path: &std::path::Path) -> Vec<String> {
    let content = std::fs::read_to_string(path).expect("Failed to read history file");
    let state: history_format::HistoryFileState =
        serde_json::from_str(&content).expect("Failed to parse history JSON");
    state.buckets()
}

/// Helper: find a bucket hash that is referenced ONLY by the given history file
/// and not by any other history file in the archive. Returns the bucket hash
/// and the bucket file's absolute path.
fn find_bucket_unique_to_history(
    archive_path: &std::path::Path,
    target_history: &std::path::Path,
) -> Option<(String, std::path::PathBuf)> {
    use std::collections::HashMap;

    // Build a map: bucket_hash → count of history files referencing it
    let all_history_files = get_files_by_pattern(archive_path, "/history-");
    let mut ref_counts: HashMap<String, usize> = HashMap::new();

    for hf in &all_history_files {
        for bucket in buckets_from_history_file(hf) {
            *ref_counts.entry(bucket).or_insert(0) += 1;
        }
    }

    // Find a bucket referenced by target_history that has ref_count == 1
    let target_buckets = buckets_from_history_file(target_history);
    for hash in target_buckets {
        if ref_counts.get(&hash) == Some(&1) {
            // This bucket is only referenced by target_history
            let bucket_files = get_files_by_pattern(archive_path, &format!("bucket-{hash}"));
            if let Some(bf) = bucket_files.into_iter().next() {
                return Some((hash, bf));
            }
        }
    }

    None
}

/// Test that repair correctly handles the case where a missing history file
/// hides bucket references. The two-wave approach should:
/// 1. Wave 1: repair the history file
/// 2. Re-scan: discover the now-visible bucket references
/// 3. Wave 2: repair the missing bucket
#[tokio::test]
async fn test_repair_missing_history_discovers_newly_visible_buckets() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Find a non-.well-known history file that has a uniquely-referenced bucket
    let history_files: Vec<_> = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();

    let mut target_history = None;
    let mut unique_bucket_path = None;

    for hf in &history_files {
        if let Some((_hash, bucket_path)) = find_bucket_unique_to_history(dest_dir.path(), hf) {
            target_history = Some(hf.clone());
            unique_bucket_path = Some(bucket_path);
            break;
        }
    }

    let target_history = target_history.expect(
        "Test archive must have a history file with at least one uniquely-referenced bucket",
    );
    let unique_bucket = unique_bucket_path.unwrap();

    // Delete both the history file AND the unique bucket
    std::fs::remove_file(&target_history).expect("Failed to delete history file");
    std::fs::remove_file(&unique_bucket).expect("Failed to delete bucket file");

    // Verify scan detects failures
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect_err("Scan should detect missing files");

    // Repair should fix BOTH: history in Wave 1, bucket discovered via re-scan in Wave 2
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed (two-wave)");

    // Both files should be restored
    assert!(
        target_history.exists(),
        "History file should be restored in Wave 1"
    );
    assert!(
        unique_bucket.exists(),
        "Bucket file should be restored in Wave 2 (discovered after history repair)"
    );

    // Full scan should pass
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after two-wave repair");
}

/// Simpler test: delete a history file and a bucket it references (not necessarily unique).
/// Repair should restore both.
#[tokio::test]
async fn test_repair_missing_history_also_repairs_its_buckets() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Pick a non-.well-known history file and one of its referenced buckets
    let history_files: Vec<_> = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();
    let target_history = &history_files[0];
    let bucket_hashes = buckets_from_history_file(target_history);
    assert!(
        !bucket_hashes.is_empty(),
        "History file must reference at least one bucket"
    );

    // Find the actual bucket file on disk
    let bucket_hash = &bucket_hashes[0];
    let bucket_files = get_files_by_pattern(dest_dir.path(), &format!("bucket-{bucket_hash}"));
    assert!(!bucket_files.is_empty(), "Bucket file must exist on disk");
    let target_bucket = &bucket_files[0];

    // Delete both
    std::fs::remove_file(target_history).expect("Failed to delete history file");
    std::fs::remove_file(target_bucket).expect("Failed to delete bucket file");

    // Repair
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    // Both should be restored
    assert!(target_history.exists(), "History file should be restored");
    assert!(target_bucket.exists(), "Bucket file should be restored");

    // Scan should pass
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repair");
}

//=============================================================================
// L. Wave 1 partial failure doesn't block Wave 2
//=============================================================================

/// Test that if Wave 1 has some download failures (e.g., source missing a file),
/// Wave 2 (bucket repair) still proceeds. This matches Go's behavior of counting
/// errors and continuing rather than aborting on first failure.
#[tokio::test]
async fn test_repair_wave1_failure_does_not_block_wave2() {
    // Set up: separate source directory so we can corrupt the source
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());

    // Mirror from source to destination
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("Mirror should succeed");

    // Delete a ledger file from destination (Wave 1 target)
    let deleted_ledger = delete_first_file(dest_dir.path(), "/ledger-");

    // Delete a bucket file from destination (Wave 2 target)
    let bucket_files = get_files_by_pattern(dest_dir.path(), "/bucket-");
    let deleted_bucket = bucket_files[0]
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::remove_file(&bucket_files[0]).expect("Failed to delete bucket");

    // NOW corrupt the source's ledger file so Wave 1 download will fail
    let src_ledger = src_dir.path().join(&deleted_ledger);
    std::fs::remove_file(&src_ledger).expect("Failed to remove source ledger");

    // Repair should fail overall (ledger can't be fixed) but Wave 2 should still run
    let result = run_repair(RepairConfig::new(&src_url, &dest_url)).await;
    assert!(
        result.is_err(),
        "Repair should report failure (ledger unfixable)"
    );

    // The bucket should still have been repaired despite Wave 1 failure
    assert!(
        dest_dir.path().join(&deleted_bucket).exists(),
        "Bucket should be repaired even when Wave 1 has failures"
    );

    // The ledger should still be missing (source doesn't have it)
    assert!(
        !dest_dir.path().join(&deleted_ledger).exists(),
        "Ledger should still be missing (source doesn't have it)"
    );
}
