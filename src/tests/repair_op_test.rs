//! Tests for repair operation
//!
//! Covers: existence-only repair, verify mode repair, cross-validation repair,
//! manual mode, dry run, error handling, idempotency, and CLI validation.

use super::utils::{
    copy_testnet_small_archive, corrupt_ledger_cross_file_hash, file_url_from_path,
    get_files_by_pattern, start_http_server, testnet_small_archive_path,
};
use crate::history_format;
use crate::test_helpers::{
    run_mirror, run_repair, run_scan, MirrorConfig, RepairConfig, ScanConfig,
};
use flate2::write::GzEncoder;
use flate2::Compression;
use rstest::rstest;
use std::collections::BTreeSet;
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
// D. Plan Mode (--plan)
//=============================================================================

#[tokio::test]
async fn test_repair_plan_round_trip_fixes_archive() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Damage: delete a ledger and corrupt a bucket.
    let deleted_ledger = delete_first_file(dest_dir.path(), "/ledger-");
    corrupt_bucket_invalid_gzip(dest_dir.path());

    // 1. Dry-run produces a plan.
    let plan_path = dest_dir.path().join("plan.json");
    run_repair(
        RepairConfig::new(&src_url, &dest_url)
            .verify()
            .dry_run()
            .report(&plan_path),
    )
    .await
    .expect("dry-run ok");
    let plan = crate::report::read_from_path(&plan_path).unwrap();

    // 2. Apply the plan.
    run_repair(RepairConfig::new(&src_url, &dest_url).verify().plan(plan))
        .await
        .expect("plan apply ok");

    // 3. Archive is healthy again.
    assert!(
        dest_dir.path().join(&deleted_ledger).exists(),
        "ledger restored"
    );
    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("scan --verify passes after plan repair");
}

#[tokio::test]
async fn test_repair_plan_restores_well_known_from_cp() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Break only the dst .well-known.
    let wk_path = dest_dir.path().join(".well-known/stellar-history.json");
    std::fs::remove_file(&wk_path).expect("delete .well-known");

    // Dry-run -> plan carrying well_known: Some(K).
    let plan_path = dest_dir.path().join("plan.json");
    run_repair(
        RepairConfig::new(&src_url, &dest_url)
            .dry_run()
            .report(&plan_path),
    )
    .await
    .expect("dry-run ok");
    let plan = crate::report::read_from_path(&plan_path).unwrap();
    let k = plan.section.well_known.expect("plan flags .well-known");

    // Apply the plan — restoration sources .well-known from the plan's checkpoint.
    run_repair(RepairConfig::new(&src_url, &dest_url).plan(plan))
        .await
        .expect("plan apply ok");

    // .well-known restored and equals history-K on dst.
    assert!(wk_path.exists(), ".well-known restored");
    let history_k = dest_dir
        .path()
        .join(history_format::checkpoint_path("history", k));
    assert_eq!(
        std::fs::read(&wk_path).unwrap(),
        std::fs::read(&history_k).unwrap(),
        ".well-known should be a copy of history-K"
    );
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("scan passes after .well-known restored");
}

#[tokio::test]
async fn test_repair_rejects_invalid_plan() {
    let (src_url, _dest_dir, dest_url) = mirror_testnet_small().await;

    // 100 is not a checkpoint boundary -> into_failures rejects it.
    let mut bad = crate::report::ArchiveReport::from_failures_and_summary(
        &crate::utils::FailureTracker::default(),
        crate::report::Summary::default(),
    );
    bad.section.checkpoints.push(100);

    let result = run_repair(RepairConfig::new(&src_url, &dest_url).plan(bad)).await;
    assert!(result.is_err(), "invalid plan should be rejected");
}

#[tokio::test]
async fn test_repair_empty_plan_is_noop() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;
    let deleted = delete_first_file(dest_dir.path(), "/ledger-");

    let empty = crate::report::ArchiveReport::from_failures_and_summary(
        &crate::utils::FailureTracker::default(),
        crate::report::Summary::default(),
    );
    run_repair(RepairConfig::new(&src_url, &dest_url).plan(empty))
        .await
        .expect("empty plan is a no-op");

    assert!(
        !dest_dir.path().join(&deleted).exists(),
        "empty plan must not repair anything"
    );
}

#[tokio::test]
async fn test_repair_plan_unfetchable_file_fails() {
    let (src_url, _dest_dir, dest_url) = mirror_testnet_small().await;

    // A well-formed checkpoint boundary beyond the archive: the ledger file is
    // absent in source, so applying the plan must fail.
    let mut tracker = crate::utils::FailureTracker::default();
    tracker.record_file(16383, crate::utils::FileFlags::LEDGER);
    let plan = crate::report::ArchiveReport::from_failures_and_summary(
        &tracker,
        crate::report::Summary::default(),
    );

    let result = run_repair(RepairConfig::new(&src_url, &dest_url).plan(plan)).await;
    assert!(result.is_err(), "plan with unfetchable file should fail");
}

//=============================================================================
// E. Dry Run
//=============================================================================

#[tokio::test]
async fn test_repair_dry_run_reports_but_no_download() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let deleted = delete_first_file(dest_dir.path(), "/ledger-");

    // History files: dry-run must not write history-*.json regardless of
    // whether dst is missing or corrupt. process_history returns NeedsRepair
    // (no source fetch, no write) under dry-run.
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

#[tokio::test]
async fn test_repair_preserves_healthy_history_file() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Record mtime of a healthy per-checkpoint history file (exclude .well-known).
    let history_files: Vec<_> = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();
    assert!(
        !history_files.is_empty(),
        "need a per-checkpoint history file"
    );
    let history_file = history_files[0].clone();
    let before = std::fs::metadata(&history_file)
        .unwrap()
        .modified()
        .unwrap();

    // Sleep so mtime would differ if the file were rewritten.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Trigger a real repair by deleting an unrelated file.
    delete_first_file(dest_dir.path(), "/ledger-");
    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("Repair should succeed");

    let after = std::fs::metadata(&history_file)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        before, after,
        "healthy history file must not be rewritten by repair"
    );

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("Scan should pass after repair");
}

#[tokio::test]
async fn test_repair_phase1_refetches_listed_history_from_source() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Two per-checkpoint history files: one to LIST + tamper, one healthy + UNLISTED.
    let history_files: Vec<_> = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();
    assert!(history_files.len() >= 2, "need at least two history files");
    let listed_file = history_files[0].clone();
    let unlisted_file = history_files[1].clone();

    // Derive the listed file's checkpoint from its (dst-relative) path.
    let listed_rel = listed_file
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let listed_cp = history_format::checkpoint_from_path(&listed_rel)
        .expect("history path carries a checkpoint");

    // Tamper the listed file's dst copy: still valid HAS JSON, but marked.
    {
        let bytes = std::fs::read(&listed_file).unwrap();
        let mut state: history_format::HistoryFileState = serde_json::from_slice(&bytes).unwrap();
        state.server = Some("tampered-by-test".to_string());
        std::fs::write(&listed_file, serde_json::to_vec(&state).unwrap()).unwrap();
    }
    let unlisted_before = std::fs::metadata(&unlisted_file)
        .unwrap()
        .modified()
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Plan lists ONLY the tampered history -> drives phase 1 (retry_failed_files).
    let mut tracker = crate::utils::FailureTracker::default();
    tracker.record_file(listed_cp, crate::utils::FileFlags::HISTORY);
    let plan = crate::report::ArchiveReport::from_failures_and_summary(
        &tracker,
        crate::report::Summary::default(),
    );

    run_repair(RepairConfig::new(&src_url, &dest_url).plan(plan))
        .await
        .expect("plan repair should succeed");

    // (RED on current) The listed history must be re-fetched from SOURCE, not
    // kept/rewritten from the tampered dst copy.
    let after = std::fs::read_to_string(&listed_file).unwrap();
    assert!(
        !after.contains("tampered-by-test"),
        "phase 1 must re-fetch a listed history from source, not keep the dst copy"
    );

    // (characterization) A healthy, unlisted history must be left untouched.
    let unlisted_after = std::fs::metadata(&unlisted_file)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        unlisted_before, unlisted_after,
        "phase 1 must not touch history files that are not in the plan"
    );
}

#[tokio::test]
async fn test_repair_replaces_broken_history_from_source() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Corrupt a per-checkpoint history file on the destination (still present,
    // but unparseable) and capture the canonical source bytes.
    let history_file = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .find(|p| !p.to_string_lossy().contains(".well-known"))
        .expect("a per-checkpoint history file");
    let rel = history_file
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_path_buf();
    let source_content = std::fs::read(testnet_small_archive_path().join(&rel)).unwrap();
    std::fs::write(&history_file, b"{ broken json }}").unwrap();

    run_repair(RepairConfig::new(&src_url, &dest_url))
        .await
        .expect("repair should succeed");

    // The broken history must have been replaced with the source content.
    assert_eq!(
        std::fs::read(&history_file).unwrap(),
        source_content,
        "broken destination history must be replaced from source"
    );

    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("scan should pass after repair");
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
// G. Download Verification (checkpoint-retry integrity)
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

/// Helper: parse a history file and return the bucket hashes it references.
fn buckets_from_history_file(path: &std::path::Path) -> BTreeSet<String> {
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
    let bucket_hash = bucket_hashes.iter().next().unwrap();
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

//=============================================================================
// M. File-retry HISTORY special case — re-fetches history file AND walks
// referenced buckets to surface buckets the main pass never probed.
//=============================================================================

/// Build a `RepairOperation` directly (no pipeline) for in-isolation
/// testing of `retry_failed_files` / `retry_failed_checkpoints`.
fn build_repair_op(
    src_url: &str,
    dst_url: &str,
    verify: bool,
) -> crate::repair_operation::RepairOperation {
    build_repair_op_full(src_url, dst_url, verify, /*dry_run=*/ false)
}

/// Like [`build_repair_op`], but with an explicit `dry_run` flag.
fn build_repair_op_full(
    src_url: &str,
    dst_url: &str,
    verify: bool,
    dry_run: bool,
) -> crate::repair_operation::RepairOperation {
    let storage_config = crate::test_helpers::test_storage_config();
    let src_store = crate::storage::from_url_with_config(src_url, &storage_config).unwrap();
    let dst_store = crate::storage::from_url_with_config(dst_url, &storage_config).unwrap();
    let pipeline_config = crate::pipeline::PipelineConfig {
        concurrency: 4,
        skip_optional: false,
        skip_history_and_buckets: false,
        verify,
        storage_config,
    };
    crate::repair_operation::RepairOperation::new(
        src_store,
        dst_store,
        /*low=*/ None,
        /*high=*/ None,
        dry_run,
        pipeline_config,
    )
}

/// Locate a history file in dst, return (its absolute path, cp, bucket-hashes
/// it references, those bucket files' absolute paths).
fn pick_history_with_buckets(
    dst_dir: &Path,
) -> (
    std::path::PathBuf,
    u32,
    BTreeSet<String>,
    Vec<std::path::PathBuf>,
) {
    let history_files: Vec<_> = get_files_by_pattern(dst_dir, "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();
    assert!(
        !history_files.is_empty(),
        "need a non-well-known history file"
    );

    for hf in &history_files {
        let hashes = buckets_from_history_file(hf);
        if hashes.is_empty() {
            continue;
        }
        let bucket_paths: Vec<_> = hashes
            .iter()
            .filter_map(|h| {
                let matches = get_files_by_pattern(dst_dir, &format!("bucket-{h}"));
                matches.into_iter().next()
            })
            .collect();
        if bucket_paths.is_empty() {
            continue;
        }
        let relative = hf
            .strip_prefix(dst_dir)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let cp = history_format::checkpoint_from_path(&relative)
            .expect("history path should encode a checkpoint");
        return (hf.clone(), cp, hashes, bucket_paths);
    }
    panic!("no history file with discoverable bucket files found");
}

/// History was failed during main pass (manually marked) AND a referenced
/// bucket is missing in dst. The file-retry path must:
///   - re-fetch the history file from src,
///   - parse it, walk bucket refs,
///   - re-fetch the missing bucket,
///   - clear both the HISTORY flag and any bucket failure entry from stats.
#[tokio::test]
async fn test_file_retry_history_repair_fetches_history_and_referenced_buckets() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let (history_abs, cp, _bucket_hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let bucket_to_break = &bucket_abs[0];
    let bucket_relative = bucket_to_break
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .to_string();

    // Setup the broken state: history + one referenced bucket gone from dst.
    std::fs::remove_file(&history_abs).expect("delete history");
    std::fs::remove_file(bucket_to_break).expect("delete bucket");

    // Build a RepairOperation directly; populate a fresh ArchiveStats with
    // the HISTORY failure (simulating the main pass's outcome).
    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;

    // Drive `retry_failed_files` in isolation.
    let stats = op.retry_failed_files(&parent_stats).await;

    // History file is back.
    assert!(history_abs.exists(), "history file should be restored");
    // The deleted bucket is back too (discovered transitively).
    assert!(
        bucket_to_break.exists(),
        "referenced bucket should be restored"
    );

    // Stats should be clean: HISTORY flag cleared, no bucket failure recorded.
    let f = stats.failures.lock().await;
    assert!(!f.files.contains_key(&cp), "HISTORY flag should be cleared");
    let _ = bucket_relative;
    assert!(
        f.buckets.is_empty(),
        "no bucket failures should remain in stats"
    );
}

/// A bucket listed directly in `failures.buckets` AND referenced by a failed
/// HISTORY must be fetched from source exactly once during file-retry — the
/// direct work item and the HISTORY-discovered sighting must share the bucket
/// scheduling gate. Counts GET requests only (HEAD probes are irrelevant).
#[tokio::test]
async fn test_file_retry_dedupes_bucket_listed_directly_and_via_history() {
    use super::utils::{start_http_server_with_app, RequestTracker};
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use tower::util::ServiceExt;
    use tower_http::services::ServeDir;

    // Separate src and dst archives (both full copies of testnet-small).
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).expect("copy src archive");
    let dst_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(dst_dir.path()).expect("copy dst archive");

    // Pick a HISTORY file and one bucket it references.
    let (history_abs, cp, _hashes, bucket_abs) = pick_history_with_buckets(dst_dir.path());
    let history_relative = history_abs
        .strip_prefix(dst_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let bucket_relative = bucket_abs[0]
        .strip_prefix(dst_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    // Break both on dst so the retry must fetch them from source.
    std::fs::remove_file(&history_abs).expect("delete dst history");
    std::fs::remove_file(&bucket_abs[0]).expect("delete dst bucket");

    // Serve src over HTTP, counting GET requests per path.
    let tracker = RequestTracker::new();
    let tracker_for_app = tracker.clone();
    let serve = ServeDir::new(src_dir.path());
    let app = Router::new().fallback(any(move |req: Request<Body>| {
        let serve = serve.clone();
        let tracker = tracker_for_app.clone();
        async move {
            if req.method() == Method::GET {
                tracker.increment(req.uri().path().trim_start_matches('/'));
            }
            match serve.oneshot(req).await {
                Ok(resp) => resp.into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    }));
    let (src_url, _handle) = start_http_server_with_app(app).await;
    let dst_url = file_url_from_path(dst_dir.path());

    // Plan lists BOTH the failed HISTORY and the same bucket directly.
    let op = build_repair_op(&src_url, &dst_url, /*verify=*/ false);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    parent_stats.record_failure(0, &bucket_relative).await;

    let stats = op.retry_failed_files(&parent_stats).await;

    // The chosen bucket must have been GETed from source exactly once.
    let counts = tracker.get_counts();
    let bucket_gets = counts.get(&bucket_relative).copied().unwrap_or(0);
    assert_eq!(
        bucket_gets, 1,
        "bucket listed directly AND via HISTORY must be fetched once; counts={counts:?}"
    );

    // Both files restored; retry stats clean.
    assert!(history_abs.exists(), "history restored");
    assert!(bucket_abs[0].exists(), "bucket restored");
    let f = stats.failures.lock().await;
    assert!(!f.files.contains_key(&cp), "HISTORY flag cleared");
    assert!(f.buckets.is_empty(), "no bucket failures remain");
}

/// History was failed (manually marked) but its referenced buckets are already
/// present and valid in dst. The HISTORY-retry path re-fetches the history file,
/// then probes each discovered bucket dst-first and finds them valid — so they
/// are NOT re-fetched (see
/// `test_file_retry_history_does_not_refetch_buckets_valid_on_dst`). End state:
/// history restored, all buckets present and valid, stats clean.
#[tokio::test]
async fn test_file_retry_history_repair_with_intact_buckets() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let (history_abs, cp, _hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    // Break only the history file; leave buckets intact.
    std::fs::remove_file(&history_abs).expect("delete history");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    let stats = op.retry_failed_files(&parent_stats).await;

    assert!(history_abs.exists(), "history file should be restored");
    for bucket in &bucket_abs {
        assert!(bucket.exists(), "bucket {bucket:?} should remain present");
    }

    let f = stats.failures.lock().await;
    assert!(!f.files.contains_key(&cp), "HISTORY flag should be cleared");
    assert!(
        f.buckets.is_empty(),
        "no bucket failures should remain in stats"
    );
}

/// History fetched OK but a referenced bucket can't be fetched (src 404s on
/// that bucket). The file-retry path must: still restore the history file, record the
/// bucket failure into stats, return the bucket as a failure for the caller.
#[tokio::test]
async fn test_file_retry_history_repair_fails_when_referenced_bucket_unavailable() {
    // Build a separate src dir so we can selectively delete a bucket from src.
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    let (history_abs, cp, hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let bucket_to_kill = &bucket_abs[0];
    let bucket_relative = bucket_to_kill
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .to_string();
    let hash_to_kill = hashes.iter().next().unwrap();

    // Delete the bucket from BOTH src and dst — `retry_failed_files` will
    // try src and fail.
    let src_bucket_files = get_files_by_pattern(src_dir.path(), &format!("bucket-{hash_to_kill}"));
    assert!(!src_bucket_files.is_empty());
    std::fs::remove_file(&src_bucket_files[0]).expect("delete src bucket");
    std::fs::remove_file(bucket_to_kill).expect("delete dst bucket");
    // Also delete dst's history so it's a `retry_failed_files` work item.
    std::fs::remove_file(&history_abs).expect("delete dst history");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    let stats = op.retry_failed_files(&parent_stats).await;

    // History file is back (src had it).
    assert!(history_abs.exists(), "history should be restored");
    // The bucket is still missing in dst.
    assert!(
        !bucket_to_kill.exists(),
        "bucket can't be repaired (src also missing)"
    );

    // Stats: HISTORY flag cleared (file repair succeeded), bucket failure
    // recorded so the user sees something is still broken.
    let f = stats.failures.lock().await;
    assert!(!f.files.contains_key(&cp));
    assert!(
        !f.buckets.is_empty(),
        "the unrecoverable bucket should be recorded as a failure"
    );
    let _ = bucket_relative;
}

/// Src's history file is corrupt (not valid JSON). The file-retry path must NOT overwrite
/// dst's history with the corrupt content, and HISTORY must remain in stats
/// so the run reports failure.
#[tokio::test]
async fn test_file_retry_history_repair_corrupt_history_from_src() {
    // Build separate src/dst so we can corrupt src's history file.
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    let (history_abs, cp, _, _) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let src_history = src_dir.path().join(&history_relative);

    // Corrupt src's history with junk text (won't parse as JSON).
    std::fs::write(&src_history, b"not valid json at all {").expect("write corrupt src history");
    // Pre-remove dst's history so `retry_failed_files` has to fetch.
    std::fs::remove_file(&history_abs).expect("delete dst history");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    let stats = op.retry_failed_files(&parent_stats).await;

    // History file must NOT have been written (parse-before-write contract).
    assert!(
        !history_abs.exists(),
        "dst history should not be overwritten with corrupt src content"
    );

    // HISTORY flag stays in stats because `retry_failed_files` failed.
    let f = stats.failures.lock().await;
    let flags = f
        .files
        .get(&cp)
        .expect("HISTORY flag should remain in stats after a failed retry_failed_files repair");
    assert!(flags.has(crate::utils::FileFlags::HISTORY));
    // No buckets touched (we never got past parse).
    assert!(f.buckets.is_empty());
}

//=============================================================================
// N. .well-known restoration ordering — restoration copies the highest
// checkpoint's history file, which the file-retry stage may itself be what
// restores. Restoration must therefore run after the retry stages.
//=============================================================================

/// Reproduces the ordering hazard: dst `.well-known` is missing AND the
/// highest-checkpoint history file was not present after the main pass (as if
/// its main-pass fetch failed), so it is only restored by the file-retry
/// stage. Because `.well-known` is restored from that history file, restoration
/// must run after the retry stage — otherwise it copies from an absent file,
/// logs an error, and silently leaves `.well-known` missing while repair still
/// reports success.
#[tokio::test]
async fn test_well_known_restored_after_history_retry() {
    use crate::pipeline::Operation;

    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // K = highest checkpoint present on dst; its history file drives .well-known.
    let (k, history_rel) = get_files_by_pattern(dest_dir.path(), "/history-")
        .iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .filter_map(|p| {
            let rel = p
                .strip_prefix(dest_dir.path())
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            history_format::checkpoint_from_path(&rel).map(|cp| (cp, rel))
        })
        .max_by_key(|(cp, _)| *cp)
        .expect("need a non-well-known history file");

    // Broken main-pass state: dst .well-known missing (so get_checkpoint_bounds
    // flags it for repair) and dst history-K missing (as if its main-pass fetch
    // failed and only the retry stage can restore it).
    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json"))
        .expect("delete .well-known");
    std::fs::remove_file(dest_dir.path().join(&history_rel)).expect("delete history-K");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);

    // Observe the missing dst .well-known so the needs-repair flag is set.
    op.get_checkpoint_bounds()
        .await
        .expect("bounds via src fallback");

    // Simulate the main pass having recorded a HISTORY failure for K — this is
    // the entry the retry stage acts on to restore history-K.
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(k, &history_rel).await;

    op.finalize(k, &parent_stats, None)
        .await
        .expect("finalize should succeed once history-K is restored by retry");

    let wk = dest_dir.path().join(".well-known/stellar-history.json");
    assert!(
        wk.exists(),
        ".well-known should be restored after the retry stage repairs history-K"
    );
    let wk_bytes = std::fs::read(&wk).unwrap();
    let history_bytes = std::fs::read(dest_dir.path().join(&history_rel)).unwrap();
    assert_eq!(
        wk_bytes, history_bytes,
        ".well-known must equal the repaired history-K byte-for-byte"
    );
}

/// A `.well-known` restoration that fails for a reason other than a missing
/// history file (here: the destination path is a directory, so the copy
/// errors) must fail the run rather than reporting success with `.well-known`
/// still broken. The highest history file stays healthy on dst, so this
/// failure is NOT visible through the retry stages' stats — repair must surface
/// it on its own.
#[tokio::test]
async fn test_repair_fails_when_well_known_restore_copy_fails() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Replace the .well-known file with a directory at the same path. The read
    // in get_checkpoint_bounds fails (flagging .well-known for repair), every
    // history file remains intact, and the restoring copy in finalize then
    // fails because its destination is a directory.
    let wk_path = dest_dir.path().join(".well-known/stellar-history.json");
    std::fs::remove_file(&wk_path).expect("delete .well-known file");
    std::fs::create_dir(&wk_path).expect("create .well-known directory");

    let result = run_repair(RepairConfig::new(&src_url, &dest_url)).await;
    assert!(
        result.is_err(),
        "repair must fail when .well-known restoration copy fails"
    );
}

//=============================================================================
// N. Two-stage retry partitioning — `build_failed_files_work_list` carve-out
//
// These exercise the load-bearing invariant of the two-stage retry design:
// when a checkpoint is in BOTH `failures.files` and `failures.checkpoints`,
// the file-retry stage keeps the HISTORY entry (it owns bucket discovery) but
// drops non-HISTORY per-cp files (the checkpoint-retry stage re-mirrors those).
//=============================================================================

/// HISTORY carve-out: a checkpoint with a HISTORY failure in `failures.files`
/// that is ALSO flagged in `failures.checkpoints` must still have its history
/// file re-fetched (and its bucket refs walked) by the file-retry stage, not
/// deferred entirely to the checkpoint-retry stage.
#[tokio::test]
async fn test_file_retry_keeps_history_when_cp_in_chain_retry() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let (history_abs, cp, _hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let bucket_to_break = &bucket_abs[0];

    // Break the history file + one referenced bucket on dst.
    std::fs::remove_file(&history_abs).expect("delete history");
    std::fs::remove_file(bucket_to_break).expect("delete bucket");

    // Simulate a main pass that recorded BOTH a HISTORY file failure AND a
    // chain failure for the same checkpoint.
    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    {
        let mut f = parent_stats.failures.lock().await;
        f.record_checkpoint(cp);
    }

    let _stats = op.retry_failed_files(&parent_stats).await;

    // Despite cp being in the chain-retry set, the file-retry stage repaired
    // the history file and walked its bucket refs.
    assert!(
        history_abs.exists(),
        "history must be repaired even though cp is in the chain-retry set"
    );
    assert!(
        bucket_to_break.exists(),
        "the referenced bucket must be repaired via the HISTORY bucket-walk"
    );
}

/// Non-HISTORY carve-out: a checkpoint with a non-HISTORY per-cp file failure
/// (LEDGER here) that is ALSO in `failures.checkpoints` must have that file
/// SKIPPED by the file-retry stage — the checkpoint-retry stage re-mirrors the
/// whole cp instead. Only the HISTORY entry survives the carve-out.
#[tokio::test]
async fn test_file_retry_skips_nonhistory_file_when_cp_in_chain_retry() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let (history_abs, cp, _hashes, _bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    // Locate this checkpoint's ledger file.
    let ledger_files = get_files_by_pattern(dest_dir.path(), &format!("ledger-{cp:08x}"));
    assert!(
        !ledger_files.is_empty(),
        "cp {cp} should have a ledger file"
    );
    let ledger_abs = ledger_files[0].clone();
    let ledger_relative = ledger_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    // Break the history + ledger on dst (leave buckets intact).
    std::fs::remove_file(&history_abs).expect("delete history");
    std::fs::remove_file(&ledger_abs).expect("delete ledger");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    parent_stats.record_failure(cp, &ledger_relative).await;
    {
        let mut f = parent_stats.failures.lock().await;
        f.record_checkpoint(cp);
    }

    let _stats = op.retry_failed_files(&parent_stats).await;

    // HISTORY is repaired (carve-out keeps it)...
    assert!(history_abs.exists(), "history must be repaired");
    // ...but the LEDGER is left for the checkpoint-retry stage.
    assert!(
        !ledger_abs.exists(),
        "ledger must be skipped by file-retry when its cp is in the chain-retry set"
    );
}

/// Checkpoint-retry skips HAS + buckets: the cp-retry inner pipeline is built
/// with `skip_history_and_buckets=true`, so re-mirroring a flagged checkpoint
/// re-fetches its per-cp xdr files (ledger/tx/results/scp) but NOT the history
/// file or any bucket. A bucket missing from dst stays missing after cp-retry.
#[tokio::test]
async fn test_checkpoint_retry_skips_buckets_and_has() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let (_history_abs, cp, _hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let bucket_to_break = &bucket_abs[0];

    let ledger_files = get_files_by_pattern(dest_dir.path(), &format!("ledger-{cp:08x}"));
    assert!(
        !ledger_files.is_empty(),
        "cp {cp} should have a ledger file"
    );
    let ledger_abs = ledger_files[0].clone();

    // Delete the cp's ledger (a per-cp xdr file) AND one referenced bucket.
    std::fs::remove_file(&ledger_abs).expect("delete ledger");
    std::fs::remove_file(bucket_to_break).expect("delete bucket");

    // Only a chain/cross-file failure is recorded for cp — no per-file, no
    // bucket. This routes the work exclusively through `retry_failed_checkpoints`.
    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ false);
    let parent_stats = crate::utils::ArchiveStats::new();
    {
        let mut f = parent_stats.failures.lock().await;
        f.record_checkpoint(cp);
    }

    let _stats = op.retry_failed_checkpoints(&parent_stats).await;

    // The per-cp ledger was re-mirrored...
    assert!(
        ledger_abs.exists(),
        "checkpoint-retry must re-fetch the per-cp ledger file"
    );
    // ...but the bucket was NOT (HAS + bucket work is skipped in cp-retry).
    assert!(
        !bucket_to_break.exists(),
        "checkpoint-retry must skip bucket fetches (skip_history_and_buckets)"
    );
}

//=============================================================================
// O. Pure chain failure (no coinciding per-file failure)
//=============================================================================

/// Tamper a ledger file so it breaks the intra-checkpoint hash chain while
/// every per-file self-hash still verifies. We bump an innocuous header field
/// (`total_coins`) on the checkpoint's first ledger and recompute that entry's
/// self-hash, so the file parses cleanly but the *next* ledger's
/// `previous_ledger_hash` no longer matches the (now-changed) computed hash.
/// Returns the affected checkpoint.
fn tamper_ledger_break_chain(archive_path: &Path) -> u32 {
    use flate2::read::GzDecoder;
    use sha2::{Digest, Sha256};
    use std::io::Read;
    use stellar_xdr::curr::{
        Frame, Hash, LedgerHeaderHistoryEntry, Limited, Limits, ReadXdr, WriteXdr,
    };

    let files = get_files_by_pattern(archive_path, "/ledger-");
    assert!(!files.is_empty(), "no ledger files found");
    let file = files[0].clone();
    let relative = file
        .strip_prefix(archive_path)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let cp =
        history_format::checkpoint_from_path(&relative).expect("ledger path encodes a checkpoint");

    // Decompress.
    let compressed = std::fs::read(&file).unwrap();
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();

    // Parse every ledger-header entry, in file (ascending-seq) order.
    let mut limited = Limited::new(std::io::Cursor::new(&decompressed[..]), Limits::none());
    let mut entries: Vec<LedgerHeaderHistoryEntry> =
        Frame::<LedgerHeaderHistoryEntry>::read_xdr_iter(&mut limited)
            .map(|r| r.expect("parse ledger header entry").0)
            .collect();
    assert!(
        entries.len() >= 2,
        "need >= 2 ledgers for an intra-cp chain window"
    );

    // Mutate the first entry's header, then recompute its self-hash so the
    // entry stays internally consistent (still passes the per-file hash check).
    entries[0].header.total_coins = entries[0].header.total_coins.wrapping_add(1);
    let header_xdr = entries[0].header.to_xdr(Limits::none()).unwrap();
    entries[0].hash = Hash(Sha256::digest(&header_xdr).into());

    // Re-encode with RFC 5531 record marking (high bit set + payload length).
    let mut out = Vec::new();
    for entry in &entries {
        let payload = entry.to_xdr(Limits::none()).unwrap();
        let marker = (payload.len() as u32 | 0x8000_0000).to_be_bytes();
        out.extend_from_slice(&marker);
        out.extend_from_slice(&payload);
    }

    // Recompress and write back.
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&out).unwrap();
    std::fs::write(&file, encoder.finish().unwrap()).unwrap();

    cp
}

/// A pure hash-chain failure: the ledger file is structurally valid and
/// self-consistent (so existence-only scan AND per-file verification both
/// pass), but the chain is broken. This exercises the `finalize` →
/// `retry_failed_checkpoints` path for a checkpoint that appears ONLY in
/// `failures.checkpoints` (no `failures.files` / `failures.buckets` entry) —
/// the case no byte-corruption test can produce, since corruption also breaks
/// the file's own self-hash.
#[tokio::test]
async fn test_repair_pure_chain_failure() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    tamper_ledger_break_chain(dest_dir.path());

    // Existence-only scan passes: the file is present and structurally valid.
    run_scan(ScanConfig::new(&dest_url))
        .await
        .expect("existence-only scan should pass (file is structurally valid)");

    // Verifying scan fails: the hash chain is broken.
    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect_err("verifying scan should detect the chain break");

    // Repair (with verify) must fix it via the checkpoint-retry stage.
    run_repair(RepairConfig::new(&src_url, &dest_url).verify())
        .await
        .expect("repair --verify should fix a pure chain failure");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("verifying scan should pass after repair");
}

//=============================================================================
// P. .well-known bounds + restoration failure paths
//=============================================================================

/// Both src and dst `.well-known` unreadable → `get_checkpoint_bounds` has no
/// way to determine the range and must return an error.
#[tokio::test]
async fn test_repair_both_well_known_unreadable_errors() {
    use crate::pipeline::Operation;

    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());
    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    std::fs::remove_file(src_dir.path().join(".well-known/stellar-history.json")).unwrap();
    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json")).unwrap();

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ false);
    let result = op.get_checkpoint_bounds().await;
    assert!(
        result.is_err(),
        "get_checkpoint_bounds must error when both .well-known are unreadable"
    );
}

/// `repair_well_known` failure path: when the highest checkpoint's history file
/// is itself unrecoverable (missing from both src and dst), repair must NOT
/// fabricate a `.well-known` from a history file it could not restore.
#[tokio::test]
async fn test_repair_well_known_not_restored_when_history_missing() {
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());
    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    // Determine the highest checkpoint (the one repair_well_known copies from).
    let wk =
        std::fs::read_to_string(src_dir.path().join(".well-known/stellar-history.json")).unwrap();
    let state: history_format::HistoryFileState = serde_json::from_str(&wk).unwrap();
    let highest_cp = history_format::round_to_lower_checkpoint(state.current_ledger);
    let history_rel = history_format::checkpoint_path("history", highest_cp);

    // Make the highest history file unavailable in BOTH src and dst, and drop
    // dst's .well-known so repair attempts to restore it.
    std::fs::remove_file(src_dir.path().join(&history_rel)).expect("rm src history");
    std::fs::remove_file(dest_dir.path().join(&history_rel)).expect("rm dst history");
    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json"))
        .expect("rm dst .well-known");

    // Repair fails (highest history unrecoverable) and must not restore
    // .well-known from a history file it couldn't repair.
    let result = run_repair(RepairConfig::new(&src_url, &dest_url)).await;
    assert!(
        result.is_err(),
        "repair should fail when the highest history file is unrecoverable"
    );
    assert!(
        !dest_dir
            .path()
            .join(".well-known/stellar-history.json")
            .exists(),
        ".well-known must not be restored when its source history file is missing"
    );
}

//=============================================================================
// Q. Dry-run write suppression with --verify (corrupt history left untouched)
//=============================================================================

/// Dry-run + verify must detect a corrupt history file but leave it untouched
/// on dst: `process_history` reports `NeedsRepair` under dry-run without
/// fetching or writing. Complements `test_repair_dry_run_with_verify`, which
/// only covered a corrupt bucket.
#[tokio::test]
async fn test_repair_dry_run_with_verify_leaves_corrupt_history_untouched() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let history_files: Vec<_> = get_files_by_pattern(dest_dir.path(), "/history-")
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();
    assert!(!history_files.is_empty());
    let corrupt_content: &[u8] = b"{ not valid history json }}";
    std::fs::write(&history_files[0], corrupt_content).unwrap();

    run_repair(RepairConfig::new(&src_url, &dest_url).verify().dry_run())
        .await
        .expect("dry-run + verify should succeed");

    assert_eq!(
        std::fs::read(&history_files[0]).unwrap(),
        corrupt_content,
        "dry-run + verify must not overwrite a corrupt history file"
    );

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect_err("history should still be corrupt after dry-run");
}

/// Dry-run + verify must account for cross-file/chain failures, not just the
/// per-file dst probe. A ledger file is made wrong-but-well-formed: every entry
/// parses and self-hashes correctly (so the dst probe accepts it and nothing is
/// added to `needs_repair_count`), but its tx-set hash no longer matches the
/// transactions file. That mismatch lands in the manager's checkpoint-level
/// tracker — which `finalize`'s dry-run summary now reports — and a real run
/// would re-fetch the checkpoint in the checkpoint-retry stage. Dry-run must not
/// write.
#[tokio::test]
async fn test_repair_dry_run_verify_surfaces_cross_file_failure() {
    use crate::pipeline::{Operation, Pipeline, PipelineConfig};

    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Pick a non-genesis ledger file and break its tx-set hash cross-file.
    let (ledger_abs, cp) = get_files_by_pattern(dest_dir.path(), "/ledger-")
        .iter()
        .filter_map(|p| {
            let rel = p
                .strip_prefix(dest_dir.path())
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            history_format::checkpoint_from_path(&rel).map(|c| (p.clone(), c))
        })
        .find(|(_, c)| *c != history_format::GENESIS_CHECKPOINT_LEDGER)
        .expect("need a non-genesis ledger file");

    corrupt_ledger_cross_file_hash(&ledger_abs, "tx_set");
    let corrupted_bytes = std::fs::read(&ledger_abs).unwrap();

    let storage_config = crate::test_helpers::test_storage_config();

    let op = build_repair_op_full(
        &src_url, &dest_url, /*verify=*/ true, /*dry_run=*/ true,
    );
    let (low, high) = op
        .get_checkpoint_bounds()
        .await
        .expect("bounds from dst .well-known");

    let config = PipelineConfig {
        concurrency: 4,
        skip_optional: false,
        skip_history_and_buckets: false,
        verify: true,
        storage_config,
    };
    let pipeline = Pipeline::new(op, config, None);
    let cps = (low..=high).step_by(history_format::CHECKPOINT_FREQUENCY as usize);
    pipeline
        .run_checkpoints(cps)
        .await
        .expect("run_checkpoints");

    {
        let f = pipeline.stats().failures.lock().await;
        assert!(
            f.checkpoints.contains(&cp),
            "cross-file failure for checkpoint {cp} should be tracked at checkpoint level"
        );
        // The well-formed ledger was accepted by the per-file probe, so it is
        // NOT recorded as a per-file failure — this is exactly why the per-file
        // count alone misses it.
        assert!(
            f.files
                .get(&cp)
                .is_none_or(|flags| !flags.has(crate::utils::FileFlags::LEDGER)),
            "wrong-but-well-formed ledger must not be recorded as a per-file failure"
        );
    }

    pipeline
        .finish(high)
        .await
        .expect("dry-run finalize should return Ok");

    assert_eq!(
        std::fs::read(&ledger_abs).unwrap(),
        corrupted_bytes,
        "dry-run must not modify dst"
    );
}

#[tokio::test]
async fn test_repair_dry_run_report_lists_inventory() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Damage: delete a ledger, corrupt a bucket, delete .well-known.
    let deleted_ledger = delete_first_file(dest_dir.path(), "/ledger-");
    corrupt_bucket_invalid_gzip(dest_dir.path());
    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json"))
        .expect("delete .well-known");

    let report_path = dest_dir.path().join("plan.json");
    crate::test_helpers::run_repair(
        crate::test_helpers::RepairConfig::new(&src_url, &dest_url)
            .verify()
            .dry_run()
            .report(&report_path),
    )
    .await
    .expect("dry-run should return Ok");

    let report = crate::report::read_from_path(&report_path).unwrap();
    assert!(
        report.section.well_known.is_some(),
        "missing .well-known should be in the plan"
    );

    let ledger_cp = history_format::checkpoint_from_path(&deleted_ledger).unwrap();
    let listed = report
        .section
        .files
        .get(&ledger_cp.to_string())
        .is_some_and(|types| types.iter().any(|t| t == "ledger"));
    assert!(
        listed,
        "deleted ledger should be in the plan: {:?}",
        report.section.files
    );
    assert!(
        !report.section.buckets.is_empty(),
        "corrupt bucket should be in the plan"
    );

    // Dry-run must not modify dst: the deleted ledger is still gone.
    assert!(
        !dest_dir.path().join(&deleted_ledger).exists(),
        "dry-run must not write to dst"
    );
}

#[tokio::test]
async fn test_repair_dry_run_well_known_records_high_checkpoint() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Break only the dst .well-known; everything else stays intact.
    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json"))
        .expect("delete .well-known");

    let report_path = dest_dir.path().join("plan.json");
    crate::test_helpers::run_repair(
        RepairConfig::new(&src_url, &dest_url)
            .dry_run()
            .report(&report_path),
    )
    .await
    .expect("dry-run should return Ok");

    // The plan must carry the checkpoint to restore .well-known from — the
    // archive's highest checkpoint, sourced from the (canonical) source archive.
    let src_wk = std::fs::read_to_string(
        testnet_small_archive_path().join(".well-known/stellar-history.json"),
    )
    .unwrap();
    let src_current_ledger = serde_json::from_str::<serde_json::Value>(&src_wk).unwrap()
        ["currentLedger"]
        .as_u64()
        .unwrap() as u32;
    let expected_k = history_format::round_to_lower_checkpoint(src_current_ledger);

    let report = crate::report::read_from_path(&report_path).unwrap();
    assert_eq!(
        report.section.well_known,
        Some(expected_k),
        "plan must record the archive's high checkpoint for .well-known"
    );
}

#[tokio::test]
async fn test_repair_dry_run_well_known_none_when_healthy() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let report_path = dest_dir.path().join("plan.json");
    crate::test_helpers::run_repair(
        RepairConfig::new(&src_url, &dest_url)
            .dry_run()
            .report(&report_path),
    )
    .await
    .expect("dry-run should return Ok");

    let report = crate::report::read_from_path(&report_path).unwrap();
    assert_eq!(
        report.section.well_known, None,
        "healthy .well-known must not be flagged"
    );
}

#[tokio::test]
async fn test_repair_report_three_sections_clean_on_success() {
    use crate::report::MultiSectionReport;

    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;
    delete_first_file(dest_dir.path(), "/ledger-");

    let report_path = dest_dir.path().join("done.json");
    crate::test_helpers::run_repair(
        crate::test_helpers::RepairConfig::new(&src_url, &dest_url)
            .verify()
            .report(&report_path),
    )
    .await
    .expect("repair should succeed");

    let json = std::fs::read_to_string(&report_path).unwrap();
    let report: MultiSectionReport =
        serde_json::from_str(&json).expect("multi-section report parses");
    assert!(report.sections.contains_key("main_pass"));

    // Retry stages cleared everything they attempted: empty bodies, no failures.
    let fr = &report.sections["file_retry"];
    let cr = &report.sections["checkpoint_retry"];
    assert_eq!(fr.summary.failed, 0);
    assert_eq!(cr.summary.failed, 0);
    assert!(fr.files.is_empty() && fr.buckets.is_empty());
    assert!(cr.checkpoints.is_empty());
}

/// A non-dry-run repair whose `.well-known` restoration FAILS must surface that
/// residual in the report — not just via the exit code. The run exits
/// `RepairFailed`, but a consumer reading `--report` would otherwise see every
/// section clean (`well_known: null`, `failed: 0`) with no indication of what
/// broke. The failed restoration is recorded into the `file_retry` section.
#[tokio::test]
async fn test_repair_report_surfaces_failed_well_known_restoration() {
    use crate::report::MultiSectionReport;

    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Replace .well-known with a directory: discovery flags it for repair, and
    // the restoring copy in finalize fails because its destination is a dir.
    let wk_path = dest_dir.path().join(".well-known/stellar-history.json");
    std::fs::remove_file(&wk_path).expect("delete .well-known file");
    std::fs::create_dir(&wk_path).expect("create .well-known directory");

    let report_path = dest_dir.path().join("report.json");
    let result = run_repair(RepairConfig::new(&src_url, &dest_url).report(&report_path)).await;
    assert!(
        result.is_err(),
        "repair must fail when .well-known restoration copy fails"
    );

    let json = std::fs::read_to_string(&report_path).unwrap();
    let report: MultiSectionReport =
        serde_json::from_str(&json).expect("multi-section report parses");
    assert!(
        report.sections["file_retry"].well_known.is_some(),
        "a failed .well-known restoration must be surfaced in the report, got: {json}"
    );
}

/// A non-dry-run repair that discovers a broken `.well-known` and successfully
/// restores it must reflect that discovery in the `main_pass` section (parity
/// with the dry-run plan, which already carries `well_known`), while the retry
/// sections stay clean because there is no residual.
#[tokio::test]
async fn test_repair_report_records_well_known_discovery_in_main_pass() {
    use crate::report::MultiSectionReport;

    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Break only the dst .well-known; everything else is intact, so restoration
    // succeeds.
    std::fs::remove_file(dest_dir.path().join(".well-known/stellar-history.json"))
        .expect("delete .well-known");

    let report_path = dest_dir.path().join("report.json");
    run_repair(RepairConfig::new(&src_url, &dest_url).report(&report_path))
        .await
        .expect("repair should succeed once .well-known is restored");

    let json = std::fs::read_to_string(&report_path).unwrap();
    let report: MultiSectionReport =
        serde_json::from_str(&json).expect("multi-section report parses");

    assert!(
        report.sections["main_pass"].well_known.is_some(),
        "main_pass should record the discovered .well-known break, got: {json}"
    );
    // Restoration succeeded → no residual in the retry sections.
    assert!(report.sections["file_retry"].well_known.is_none());
    assert!(report.sections["checkpoint_retry"].well_known.is_none());
}

/// HISTORY-retry must not re-download buckets already valid on dst. A bucket
/// referenced by the retried history file is deleted from SRC only (it stays
/// valid on dst). The old overwrite path would re-fetch it, hit a 404 on src,
/// and spuriously record a bucket failure; the dst-first probe finds it valid
/// on dst and skips it — no fetch, no failure.
#[tokio::test]
async fn test_file_retry_history_does_not_refetch_buckets_valid_on_dst() {
    // Separate src dir so we can delete a bucket from src only.
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    let (history_abs, cp, hashes, _bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let hash = hashes.iter().next().unwrap();

    // Delete that bucket from SRC only; dst keeps a valid copy.
    let src_bucket = get_files_by_pattern(src_dir.path(), &format!("bucket-{hash}"));
    assert!(!src_bucket.is_empty());
    std::fs::remove_file(&src_bucket[0]).expect("delete src bucket");
    // Delete dst history so it's a file-retry work item.
    std::fs::remove_file(&history_abs).expect("delete dst history");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    let stats = op.retry_failed_files(&parent_stats).await;

    assert!(history_abs.exists(), "history should be restored");
    let dst_bucket = get_files_by_pattern(dest_dir.path(), &format!("bucket-{hash}"));
    assert!(
        !dst_bucket.is_empty(),
        "valid dst bucket must remain present"
    );
    let f = stats.failures.lock().await;
    assert!(
        f.buckets.is_empty(),
        "a bucket valid on dst must not be re-fetched or recorded as a failure, got: {:?}",
        f.buckets
    );
}

/// A bucket listed in `failures.buckets` (known broken) skips the dst probe and
/// is fetched directly. We prove the probe was skipped by deleting the bucket
/// from src while leaving a valid copy on dst: a direct fetch 404s and records a
/// failure, whereas the dst-first probe (Task 1) would have skipped it. This is
/// the deliberate inverse of `..._does_not_refetch_buckets_valid_on_dst`.
#[tokio::test]
async fn test_file_retry_known_broken_bucket_fetched_directly() {
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    let (_history_abs, _cp, hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let hash = hashes.iter().next().unwrap().clone();
    let bucket_rel = bucket_abs[0]
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");

    // Delete the bucket from src only; dst keeps a valid copy.
    let src_bucket = get_files_by_pattern(src_dir.path(), &format!("bucket-{hash}"));
    std::fs::remove_file(&src_bucket[0]).expect("delete src bucket");

    // Mark the bucket itself as broken (no history work item this time).
    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(0, &bucket_rel).await;
    let stats = op.retry_failed_files(&parent_stats).await;

    // The probe was skipped: a direct fetch was attempted, hit src's 404, and
    // recorded a failure (rather than skipping the valid dst copy).
    let f = stats.failures.lock().await;
    assert!(
        !f.buckets.is_empty(),
        "a known-broken bucket should be fetched directly (probe skipped), failing on src 404"
    );
}

/// HISTORY-retry under `--verify` repairs a referenced bucket that is
/// present-but-invalid on dst and NOT pre-listed (discovered via the re-walk):
/// the dst-first probe validates content, fails, and re-fetches from src.
#[tokio::test]
async fn test_file_retry_history_repairs_invalid_discovered_bucket_with_verify() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    let (history_abs, cp, _hashes, bucket_abs) = pick_history_with_buckets(dest_dir.path());
    let history_relative = history_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let bucket_to_corrupt = &bucket_abs[0];

    // Corrupt the bucket on dst (invalid gzip) but keep it present; delete the
    // dst history so it becomes a file-retry work item. The bucket is NOT in the
    // failure set — it's discovered by re-walking the repaired history.
    std::fs::write(bucket_to_corrupt, b"not a valid gzip bucket").expect("corrupt bucket");
    std::fs::remove_file(&history_abs).expect("delete dst history");

    let op = build_repair_op(&src_url, &dest_url, /*verify=*/ true);
    let parent_stats = crate::utils::ArchiveStats::new();
    parent_stats.record_failure(cp, &history_relative).await;
    let stats = op.retry_failed_files(&parent_stats).await;

    assert!(history_abs.exists(), "history should be restored");
    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("scan --verify should pass after the invalid bucket is repaired");
    let f = stats.failures.lock().await;
    assert!(
        f.buckets.is_empty(),
        "invalid discovered bucket should be repaired, not failed"
    );
}

/// Plan mode trusts the plan: a listed item is fetched from src directly —
/// never skipped because dst happens to have a (valid) copy, and never
/// expensively re-verified on dst. Proven by deleting the listed file from
/// src: the direct fetch 404s and the repair fails. The failed fetch must
/// also leave the existing dst copy untouched.
#[tokio::test]
async fn test_repair_plan_listed_item_is_refetched_not_skipped() {
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    // A ledger that is valid on dst; delete it from src only.
    let (ledger_abs, cp) = get_files_by_pattern(dest_dir.path(), "/ledger-")
        .iter()
        .find_map(|p| {
            let rel = p
                .strip_prefix(dest_dir.path())
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            history_format::checkpoint_from_path(&rel).map(|c| (p.clone(), c))
        })
        .expect("a ledger file");
    let ledger_rel = ledger_abs
        .strip_prefix(dest_dir.path())
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::remove_file(src_dir.path().join(&ledger_rel)).expect("delete src ledger");

    // A plan listing that (still-valid-on-dst) ledger.
    let mut tracker = crate::utils::FailureTracker::default();
    tracker.record_file(cp, crate::utils::FileFlags::LEDGER);
    let plan = crate::report::ArchiveReport::from_failures_and_summary(
        &tracker,
        crate::report::Summary::default(),
    );

    // Failed-list mode: the listed ledger is fetched from src directly (no
    // dst probe). src no longer has it, so the repair fails — a listed item
    // is never silently skipped just because dst has a copy.
    run_repair(RepairConfig::new(&src_url, &dest_url).verify().plan(plan))
        .await
        .expect_err("listed item is re-fetched; missing on src fails the repair");
    assert!(
        ledger_abs.exists(),
        "failed re-fetch must not delete the existing dst copy"
    );
}

/// A scan report feeds directly into repair: scan --verify lists a deleted
/// file AND a corrupt-but-present bucket; applying that report with
/// `repair --plan` and NO CLI --verify still repairs both, because every
/// listed item is re-fetched unconditionally (the plan is fully respected —
/// no mode needs to be carried in it).
#[tokio::test]
async fn test_scan_report_feeds_into_repair() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Damage dst: delete a ledger and corrupt a bucket (present-but-invalid).
    delete_first_file(dest_dir.path(), "/ledger-");
    corrupt_bucket_invalid_gzip(dest_dir.path());

    // Scan --verify writes a report of what's broken.
    let scan_json = dest_dir.path().join("findings.json");
    run_scan(ScanConfig::new(&dest_url).verify().report(&scan_json))
        .await
        .expect_err("scan --verify should fail on the damaged archive");

    let report = crate::report::read_from_path(&scan_json).unwrap();
    assert!(
        !report.section.files.is_empty() || !report.section.buckets.is_empty(),
        "scan report should list the failures"
    );

    // Apply the scan report as a repair plan, WITHOUT a CLI --verify. Listed
    // items are re-fetched unconditionally, so the corrupt-but-present bucket
    // is repaired too.
    run_repair(RepairConfig::new(&src_url, &dest_url).plan(report))
        .await
        .expect("repair --plan should fix every listed item");

    run_scan(ScanConfig::new(&dest_url).verify())
        .await
        .expect("scan --verify should pass after plan-driven repair");
}

/// A mirror report feeds into repair: mirroring from an incomplete source
/// records the un-copyable file (mirror writes its report before the failure
/// gate); applying that report with `repair --plan` against a complete source
/// restores the file. Confirms mirror output is a valid plan.
#[tokio::test]
async fn test_mirror_report_feeds_into_repair() {
    // Incomplete source: a full archive minus one ledger file.
    let src_incomplete = TempDir::new().unwrap();
    copy_testnet_small_archive(src_incomplete.path()).unwrap();
    let deleted = delete_first_file(src_incomplete.path(), "/ledger-");
    let src_incomplete_url = file_url_from_path(src_incomplete.path());

    // Complete source for the repair step.
    let complete_src_url = file_url_from_path(&testnet_small_archive_path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());

    // Mirror from the incomplete source → records the missing ledger; writes the
    // report even though the run fails.
    let mirror_json = dest_dir.path().join("mirror.json");
    run_mirror(MirrorConfig::new(&src_incomplete_url, &dest_url).report(&mirror_json))
        .await
        .expect_err("mirror from an incomplete source should fail");

    let report = crate::report::read_from_path(&mirror_json).unwrap();
    let cp = history_format::checkpoint_from_path(&deleted).unwrap();
    assert!(
        report.section.files.contains_key(&cp.to_string()),
        "mirror report should list the un-copyable ledger's checkpoint: {:?}",
        report.section.files
    );

    // Apply the mirror report against the COMPLETE source → restores the ledger.
    run_repair(RepairConfig::new(&complete_src_url, &dest_url).plan(report))
        .await
        .expect("repair --plan should restore the file from the complete source");

    assert!(
        dest_dir.path().join(&deleted).exists(),
        "the ledger missing from mirror's source should be restored by repair"
    );
}

/// CLI `--verify` works in plan mode exactly as in regular repair: the
/// re-fetched content is validated before being written. Proven by corrupting
/// the source copy of a listed-missing file: the verified re-fetch rejects
/// the corrupt source and repair fails (without --verify it would have been
/// blind-copied).
#[tokio::test]
async fn test_plan_apply_with_cli_verify_validates_source() {
    // Separate src so we can corrupt it without touching the shared fixture.
    let src_dir = TempDir::new().unwrap();
    copy_testnet_small_archive(src_dir.path()).unwrap();
    let src_url = file_url_from_path(src_dir.path());

    let dest_dir = TempDir::new().unwrap();
    let dest_url = file_url_from_path(dest_dir.path());
    run_mirror(MirrorConfig::new(&src_url, &dest_url))
        .await
        .expect("mirror should succeed");

    // Delete a ledger on dst (missing → a no-verify scan would list it), then
    // corrupt that same ledger's content in src so a verified re-fetch rejects it.
    let deleted = delete_first_file(dest_dir.path(), "/ledger-");
    let cp = history_format::checkpoint_from_path(&deleted).unwrap();
    std::fs::write(src_dir.path().join(&deleted), b"not a valid gzip ledger")
        .expect("corrupt src ledger");

    // A plan listing that ledger.
    let mut tracker = crate::utils::FailureTracker::default();
    tracker.record_file(cp, crate::utils::FileFlags::LEDGER);
    let plan = crate::report::ArchiveReport::from_failures_and_summary(
        &tracker,
        crate::report::Summary::default(),
    );

    // Apply WITH --verify: the corrupt src ledger is rejected on re-fetch and
    // repair fails.
    let result = run_repair(RepairConfig::new(&src_url, &dest_url).verify().plan(plan)).await;
    assert!(
        result.is_err(),
        "--verify must validate re-fetched content; corrupt src should be rejected"
    );
}

/// `FailureTracker::contains_path` matches per-cp files by (checkpoint, type)
/// and buckets by content hash — the membership test behind repair's
/// failed-list mode (listed files are fetched directly, skipping the dst probe).
#[test]
fn test_failure_tracker_contains_path() {
    use crate::utils::{hex_to_hash, FailureTracker, FileFlags};

    let mut t = FailureTracker::default();
    t.record_file(127, FileFlags::LEDGER);

    let ledger_127 = history_format::checkpoint_path("ledger", 127);
    let tx_127 = history_format::checkpoint_path("transactions", 127);
    let ledger_191 = history_format::checkpoint_path("ledger", 191);
    assert!(t.contains_path(&ledger_127), "recorded (cp, flag) matches");
    assert!(!t.contains_path(&tx_127), "same cp, different file type");
    assert!(!t.contains_path(&ledger_191), "different cp");
    assert!(!t.contains_path("not/an/archive/path"));

    // Buckets are matched by content hash, independent of cp.
    let hash_hex = "ab".repeat(32);
    let bucket_path = history_format::bucket_path(&hash_hex).unwrap();
    assert!(!t.contains_path(&bucket_path), "bucket not recorded yet");
    t.record_bucket(hex_to_hash(&hash_hex).unwrap());
    assert!(
        t.contains_path(&bucket_path),
        "recorded bucket hash matches"
    );
}

/// Direct-fetch semantics: a listed file that is present-but-corrupt on dst is
/// overwritten from src even when the plan and the CLI are both non-verify —
/// listed items are re-fetched unconditionally, never existence-skipped.
#[tokio::test]
async fn test_plan_refetches_corrupt_present_file_without_verify() {
    let (src_url, dest_dir, dest_url) = mirror_testnet_small().await;

    // Corrupt a transactions file on dst (present but invalid).
    let (tx_abs, cp) = get_files_by_pattern(dest_dir.path(), "/transactions-")
        .iter()
        .find_map(|p| {
            let rel = p
                .strip_prefix(dest_dir.path())
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            history_format::checkpoint_from_path(&rel).map(|c| (p.clone(), c))
        })
        .expect("a transactions file");
    std::fs::write(&tx_abs, b"garbage").expect("corrupt dst transactions");

    // A plan listing it.
    let mut tracker = crate::utils::FailureTracker::default();
    tracker.record_file(cp, crate::utils::FileFlags::TRANSACTIONS);
    let plan = crate::report::ArchiveReport::from_failures_and_summary(
        &tracker,
        crate::report::Summary::default(),
    );

    // No --verify anywhere — the listed file must still be re-fetched.
    run_repair(RepairConfig::new(&src_url, &dest_url).plan(plan))
        .await
        .expect("listed corrupt-present file is re-fetched");

    // The dst copy now matches src again.
    let rel = tx_abs.strip_prefix(dest_dir.path()).unwrap();
    let src_file = testnet_small_archive_path().join(rel);
    assert_eq!(
        std::fs::read(&tx_abs).unwrap(),
        std::fs::read(&src_file).unwrap(),
        "corrupt dst transactions file is overwritten with src content"
    );
}
