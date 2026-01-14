//! Filesystem-based tests for mirror command
//!
//! This module tests:
//! - Mirror correctness (file-by-file verification)
//! - Gap handling and the --allow-mirror-gaps flag
//! - Resume and --overwrite behavior

use super::utils::{
    copy_test_archive, start_http_server, start_http_server_with_app, test_archive_path,
};
use crate::{
    history_format,
    test_helpers::{run_mirror, run_scan, test_storage_config, MirrorConfig, ScanConfig},
};
use axum::{routing::get_service, Router};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tower_http::services::ServeDir;
use walkdir::WalkDir;

/// Collects expected bucket hashes from history files within the checkpoint bound.
fn collect_expected_buckets(source_path: &Path, max_checkpoint: Option<u32>) -> HashSet<String> {
    let mut expected = HashSet::new();

    for entry in WalkDir::new(source_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if !filename.starts_with("history-") || !filename.ends_with(".json") {
            continue;
        }

        let checkpoint = history_format::checkpoint_from_filename(&filename)
            .expect("Failed to extract checkpoint");

        if max_checkpoint.map_or(false, |max| checkpoint > max) {
            continue;
        }

        let content = std::fs::read_to_string(path).expect("Failed to read history file");
        let has: history_format::HistoryFileState =
            serde_json::from_str(&content).expect("Failed to parse history file");

        for hash in has.buckets() {
            expected.insert(hash);
        }
    }

    expected
}

/// Returns true if this is a checkpoint file (history, ledger, results, transactions, scp).
fn is_checkpoint_file(filename: &str) -> bool {
    filename.starts_with("history-")
        || filename.starts_with("ledger-")
        || filename.starts_with("results-")
        || filename.starts_with("transactions-")
        || filename.starts_with("scp-")
}

/// Verifies that source checkpoint files within bounds exist in destination,
/// and files beyond bounds do not exist.
fn verify_checkpoint_files(source_path: &Path, dest_path: &Path, max_checkpoint: Option<u32>) {
    for entry in WalkDir::new(source_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let src_file = entry.path();
        let relative_path = src_file.strip_prefix(source_path).unwrap();
        let path_str = relative_path.to_string_lossy();

        // Skip .well-known and bucket files
        if path_str == ".well-known/stellar-history.json" || path_str.starts_with("bucket/") {
            continue;
        }

        let dst_file = dest_path.join(&relative_path);
        let filename = src_file.file_name().unwrap().to_string_lossy();

        if let Some(max_cp) = max_checkpoint {
            if is_checkpoint_file(&filename) {
                let checkpoint = history_format::checkpoint_from_filename(&filename)
                    .expect("Failed to extract checkpoint");

                if checkpoint > max_cp {
                    assert!(
                        !dst_file.exists(),
                        "File beyond bound should not exist: {}",
                        path_str
                    );
                } else {
                    assert!(dst_file.exists(), "Missing file within bound: {}", path_str);
                }
                continue;
            }
        }

        // Non-checkpoint files or unbounded: should exist
        assert!(
            dst_file.exists(),
            "Missing file in destination: {}",
            path_str
        );
    }
}

/// Verifies destination files: buckets are expected, content matches source, nothing extra.
fn verify_destination_files(
    source_path: &Path,
    dest_path: &Path,
    max_checkpoint: Option<u32>,
    expected_buckets: &mut HashSet<String>,
) {
    for entry in WalkDir::new(dest_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let dst_file = entry.path();
        let relative_path = dst_file.strip_prefix(dest_path).unwrap();
        let path_str = relative_path.to_string_lossy();

        if path_str == ".well-known/stellar-history.json" {
            continue;
        }

        let filename = dst_file.file_name().unwrap().to_string_lossy();

        // Verify bucket files are in expected set
        if path_str.starts_with("bucket/") {
            let hash = history_format::bucket_hash_from_filename(&filename)
                .expect("Failed to extract bucket hash");
            assert!(
                expected_buckets.remove(&hash),
                "Unexpected bucket file: {} (hash: {})",
                path_str,
                hash
            );
        } else if let Some(max_cp) = max_checkpoint {
            // Verify checkpoint files are within bounds
            if let Some(checkpoint) = history_format::checkpoint_from_filename(&filename) {
                assert!(
                    checkpoint <= max_cp,
                    "File beyond bound: {} (checkpoint {} (0x{:08x}))",
                    path_str,
                    checkpoint,
                    checkpoint
                );
            }
        }

        // Verify content matches source
        let src_file = source_path.join(&relative_path);
        assert!(
            src_file.exists(),
            "No source for destination file: {}",
            path_str
        );

        let src_content = std::fs::read(&src_file).expect("Failed to read source");
        let dst_content = std::fs::read(&dst_file).expect("Failed to read dest");
        assert_eq!(src_content, dst_content, "Content mismatch: {}", path_str);
    }
}

/// Verifies mirror correctness by checking all files match between source and destination.
fn verify_mirror_correctness(source_path: &Path, dest_path: &Path, max_checkpoint: Option<u32>) {
    let mut expected_buckets = collect_expected_buckets(source_path, max_checkpoint);

    verify_checkpoint_files(source_path, dest_path, max_checkpoint);
    verify_destination_files(
        source_path,
        dest_path,
        max_checkpoint,
        &mut expected_buckets,
    );

    assert!(
        expected_buckets.is_empty(),
        "Missing {} expected bucket files",
        expected_buckets.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_mirror_full_archive() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest),
        concurrency: 20,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
        storage_config: test_storage_config(),
    };

    run_mirror(mirror_config).await.expect("Full mirror failed");

    // Verify with scan
    run_scan(ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
        storage_config: test_storage_config(),
    })
    .await
    .expect("Scan of full archive failed");

    // Verify file-by-file correctness
    verify_mirror_correctness(&test_archive_path, temp_dir.path(), None);

    // Verify .well-known matches source
    let src_has = test_archive_path.join(".well-known/stellar-history.json");
    let dst_has = temp_dir.path().join(".well-known/stellar-history.json");
    let src_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&src_has).unwrap()).unwrap();
    let dst_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&dst_has).unwrap()).unwrap();
    assert_eq!(src_json, dst_json);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_mirror_bounded() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest),
        concurrency: 20,
        skip_optional: false,
        high: Some(4991),
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
        storage_config: test_storage_config(),
    };

    run_mirror(mirror_config)
        .await
        .expect("Bounded mirror failed");

    // Verify with scan
    run_scan(ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
        storage_config: test_storage_config(),
    })
    .await
    .expect("Scan of bounded archive failed");

    // Verify file-by-file correctness (checkpoint 4991 = 0x137f)
    verify_mirror_correctness(&test_archive_path, temp_dir.path(), Some(0x137f));

    // Verify .well-known matches history file for checkpoint 4991
    let has_path = temp_dir.path().join(".well-known/stellar-history.json");
    let history_path = test_archive_path.join("history/00/00/13/history-0000137f.json");
    assert_eq!(
        std::fs::read(&has_path).unwrap(),
        std::fs::read(&history_path).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_mirror_skip_optional() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: true,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
        storage_config: test_storage_config(),
    };

    run_mirror(mirror_config)
        .await
        .expect("Mirror with skip_optional failed");

    // Verify scp directory doesn't exist (optional files skipped)
    assert!(!temp_dir.path().join("scp").exists());

    // Verify with scan (must also skip optional)
    run_scan(ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: None,
        storage_config: test_storage_config(),
    })
    .await
    .expect("Scan of archive without optional files failed");
}

//=============================================================================
// Gap Handling Tests
//=============================================================================

/// Tests that resuming a mirror without creating a gap succeeds
#[tokio::test]
async fn test_mirror_resume_without_gap() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror up to ledger 500
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(500))
        .await
        .unwrap();

    // Resume with low=400 (overlaps with existing end of 500) - should succeed
    run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(400)
            .high(1000),
    )
    .await
    .unwrap();

    // Verify archive continues past original end
    let checkpoint_after_500 = dest_path.join("history/00/00/03/history-000003ff.json");
    assert!(
        checkpoint_after_500.exists(),
        "Archive should continue past original end"
    );
}

/// Tests that creating a gap without --allow-mirror-gaps flag is rejected
#[tokio::test]
async fn test_mirror_rejects_gap_creation() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror up to ledger 1000 to establish destination state
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(1000))
        .await
        .unwrap();

    // Try to mirror with --low that would create a gap - should fail
    let result = run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(2000)
            .high(3000),
    )
    .await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Cannot mirror") && err_msg.contains("would create a gap"),
        "Expected gap error, got: {}",
        err_msg
    );

    // --overwrite flag should not bypass gap check
    let overwrite_result = run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(2000)
            .high(3000)
            .overwrite(),
    )
    .await;
    assert!(
        overwrite_result.is_err(),
        "--overwrite should not bypass gap check"
    );
}

/// Tests that --allow-mirror-gaps flag permits creating gaps
#[tokio::test]
async fn test_mirror_allows_gap_with_flag() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror up to ledger 1000 to establish destination state
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(1000))
        .await
        .unwrap();

    // Mirror with gap using --allow-mirror-gaps - should succeed
    run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(2000)
            .high(3000)
            .allow_mirror_gaps(),
    )
    .await
    .unwrap();

    // Verify files from the new range exist
    let later_history = dest_path.join("history/00/00/07/history-000007bf.json");
    assert!(later_history.exists(), "Files in new range should exist");

    // Verify files in the gap do not exist
    let gap_history = dest_path.join("history/00/00/05/history-000005ff.json");
    assert!(!gap_history.exists(), "Files in gap should not exist");
}

#[tokio::test]
async fn test_mirror_allows_gaps_for_empty_destination() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("empty_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror with --low to an empty destination
    // Should succeed without gap check since there's no existing archive
    run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(2000)
            .high(3000),
    )
    .await
    .unwrap();

    // Check that checkpoint below low does not exist, e.g. 1023 (0x3ff)
    let before_low_checkpoint = dest_path.join("history/00/00/03/history-000003ff.json");
    assert!(
        !before_low_checkpoint.exists(),
        "Files before the low value should not exist"
    );

    // Scan with no low should fail because there's a gap
    let scan_result = run_scan(ScanConfig::new(&dst).skip_optional()).await;
    assert!(
        scan_result.is_err(),
        "Scan without low should fail when archive has gap at beginning"
    );

    // Followup scan with low 2000 should succeed
    run_scan(ScanConfig::new(&dst).skip_optional().low(2000))
        .await
        .unwrap();
}

//=============================================================================
// Resume and Overwrite Behavior Tests
//=============================================================================

/// Tests that resume without --overwrite skips existing files (ignores --low)
#[tokio::test]
async fn test_mirror_resume_skips_existing_files() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror up to ledger 2000 to establish destination state
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(2000))
        .await
        .unwrap();

    // Get a file that would be in the --low range
    let existing_file = dest_path.join("history/00/00/01/history-000001bf.json");
    let initial_modified = std::fs::metadata(&existing_file)
        .unwrap()
        .modified()
        .unwrap();

    // Sleep to ensure timestamp difference if file were rewritten
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Resume with --low 500 without --overwrite - should ignore --low
    run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(500)
            .high(3000),
    )
    .await
    .unwrap();

    // Verify file was not re-downloaded
    let after_modified = std::fs::metadata(&existing_file)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        initial_modified, after_modified,
        "Existing file should not be re-downloaded without --overwrite"
    );
}

/// Tests that --overwrite re-downloads files in range but not before --low
#[tokio::test]
async fn test_mirror_overwrite_redownloads_in_range() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror up to ledger 2000 to establish destination state
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(2000))
        .await
        .unwrap();

    // File before low=500 (checkpoint 383 = 0x17f)
    let before_low_file = dest_path.join("history/00/00/01/history-0000017f.json");
    // File after low=500 (checkpoint 447 = 0x1bf)
    let after_low_file = dest_path.join("history/00/00/01/history-000001bf.json");

    let before_low_initial = std::fs::metadata(&before_low_file)
        .unwrap()
        .modified()
        .unwrap();
    let after_low_initial = std::fs::metadata(&after_low_file)
        .unwrap()
        .modified()
        .unwrap();

    // Sleep to ensure timestamp difference
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Mirror with --low 500 and --overwrite
    run_mirror(
        MirrorConfig::new(&src, &dst)
            .skip_optional()
            .low(500)
            .high(3000)
            .overwrite(),
    )
    .await
    .unwrap();

    let before_low_after = std::fs::metadata(&before_low_file)
        .unwrap()
        .modified()
        .unwrap();
    let after_low_after = std::fs::metadata(&after_low_file)
        .unwrap()
        .modified()
        .unwrap();

    // File before --low should NOT be re-downloaded
    assert_eq!(
        before_low_initial, before_low_after,
        "File before --low should not be re-downloaded"
    );

    // File after --low SHOULD be re-downloaded
    assert_ne!(
        after_low_initial, after_low_after,
        "File after --low should be re-downloaded with --overwrite"
    );
}

/// Tests that empty files (0 bytes) are treated as non-existent and get re-downloaded when encountered.
#[tokio::test]
async fn test_mirror_replaces_empty_files() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Create destination directory structure with an empty file before mirroring
    // This simulates a partial/failed previous mirror that left an empty file
    let ledger_dir = dest_path.join("ledger/00/00/00");
    std::fs::create_dir_all(&ledger_dir).unwrap();
    let ledger_file = ledger_dir.join("ledger-0000003f.xdr.gz");
    std::fs::write(&ledger_file, b"").unwrap();
    assert_eq!(
        std::fs::metadata(&ledger_file).unwrap().len(),
        0,
        "File should start empty"
    );

    // Mirror checkpoint 63 - the empty file should be treated as non-existent and downloaded
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(63))
        .await
        .unwrap();

    // The empty file should have been replaced with actual content
    let final_size = std::fs::metadata(&ledger_file).unwrap().len();
    assert!(
        final_size > 0,
        "Empty file should have been re-downloaded, got size {}",
        final_size
    );
}

//=============================================================================
// .well-known update tests
//=============================================================================

/// Tests that mirroring with --high lower than dest succeeds gracefully (destination already up to date).
#[tokio::test]
async fn test_mirror_lower_high_than_dest_succeeds() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // First mirror to checkpoint 2000 (rounds to 0x7ff = 2047)
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(2000))
        .await
        .unwrap();

    // Without --overwrite, trying to mirror to a lower checkpoint should succeed
    // but result in "destination is up to date" (0 checkpoints processed)
    let result = run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(1000)).await;
    assert!(
        result.is_ok(),
        "Mirror to lower --high should succeed (destination already up to date)"
    );
}

/// Tests that mirroring to same checkpoint succeeds but .well-known is preserved.
#[tokio::test]
async fn test_mirror_preserves_wellknown_when_dest_is_equal() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // First mirror to checkpoint 2000 (rounds to 0x7ff = 2047)
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(2000))
        .await
        .unwrap();

    let wellknown_path = dest_path.join(".well-known/stellar-history.json");
    let initial_modified = std::fs::metadata(&wellknown_path)
        .unwrap()
        .modified()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));

    // Re-mirror to same checkpoint - should succeed
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(2000))
        .await
        .unwrap();

    // .well-known should not be rewritten
    let after_modified = std::fs::metadata(&wellknown_path)
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        initial_modified, after_modified,
        ".well-known should not be rewritten when dest == target"
    );
}

/// Tests that .well-known is created when destination has data but missing .well-known.
#[tokio::test]
async fn test_mirror_creates_wellknown_when_missing() {
    let src = format!("file://{}", test_archive_path().display());
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("mirror_dest");
    let dst = format!("file://{}", dest_path.display());

    // Mirror to checkpoint 1000 (rounds to 0x3ff = 1023)
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(1000))
        .await
        .unwrap();

    // Delete the .well-known file (simulating crash or corruption)
    let wellknown_path = dest_path.join(".well-known/stellar-history.json");
    assert!(
        wellknown_path.exists(),
        ".well-known should exist after mirror"
    );
    std::fs::remove_file(&wellknown_path).unwrap();
    assert!(!wellknown_path.exists(), ".well-known should be deleted");

    // Resume mirror to a higher checkpoint - should recreate .well-known
    run_mirror(MirrorConfig::new(&src, &dst).skip_optional().high(2000))
        .await
        .unwrap();

    assert!(
        wellknown_path.exists(),
        ".well-known should be recreated when missing"
    );

    // Verify it has the correct checkpoint
    let content = std::fs::read_to_string(&wellknown_path).unwrap();
    let has: serde_json::Value = serde_json::from_str(&content).unwrap();
    let ledger = has["currentLedger"].as_u64().unwrap() as u32;
    let checkpoint = history_format::round_to_lower_checkpoint(ledger);

    // 2000 rounds to checkpoint 2047 (0x7ff)
    assert_eq!(
        checkpoint, 0x7ff,
        ".well-known should have checkpoint from highest mirrored range"
    );
}

//=============================================================================
// HTTP Mirror Tests
//=============================================================================

/// Tests basic HTTP mirror to filesystem.
#[tokio::test]
async fn test_mirror_http_to_filesystem() {
    let archive_path = test_archive_path();
    let (server_url, server_handle) = start_http_server(&archive_path).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    // Mirror from HTTP to filesystem
    let mirror_config = MirrorConfig {
        src: server_url.clone(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: None,
        low: None,
        skip_optional: false,
        overwrite: false,
        allow_mirror_gaps: false,
        storage_config: test_storage_config(),
    };

    run_mirror(mirror_config).await.unwrap_or_else(|e| {
        server_handle.abort();
        panic!("HTTP mirror failed: {}", e);
    });

    // Verify the mirrored archive
    let scan_config = ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
        storage_config: test_storage_config(),
    };

    run_scan(scan_config).await.unwrap_or_else(|e| {
        server_handle.abort();
        panic!("Scan of mirrored archive failed: {}", e);
    });

    server_handle.abort();
}

/// Tests that mirror handles race conditions when archive advances during mirroring.
/// The mirror should use the HAS from the start of the operation, not a newer one
/// that appears mid-mirror.
#[tokio::test]
async fn test_mirror_race_condition_with_advancing_archive() {
    // Create a temp archive that simulates a live archive that advances during mirror
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Read the history files for checkpoint 127 and 191 to use as our .well-known files
    let history_7f_path = archive_path.join("history/00/00/00/history-0000007f.json");
    let history_bf_path = archive_path.join("history/00/00/00/history-000000bf.json");

    // Use the history file at checkpoint 127 as our initial server .well-known file
    let initial_well_known = std::fs::read_to_string(&history_7f_path)
        .expect("Test data is broken: history-0000007f.json must exist in testnet-archive-small");

    // Use the actual history file at checkpoint 191 as the advanced .well-known file
    let advanced_well_known = std::fs::read_to_string(&history_bf_path)
        .expect("Test data is broken: history-000000bf.json must exist in testnet-archive-small");

    // Write the initial .well-known (from checkpoint 127) to .well-known
    let well_known_path = archive_path.join(".well-known/stellar-history.json");
    std::fs::write(&well_known_path, &initial_well_known)
        .expect("Failed to write initial .well-known");

    // Start HTTP server with special handler that advances .well-known after initial reads.
    // We use a counter instead of a boolean because OpenDAL may make multiple requests
    // (e.g., stat then read) for a single logical "read". We want to return the initial
    // .well-known for all requests during the first read operation, then advance.
    // Using threshold of 2 handles: stat + read = 2 requests for initial fetch.
    let request_count = Arc::new(AtomicUsize::new(0));
    let advance_threshold = 2; // After this many requests, start returning advanced state
    let app = Router::new()
        .route(
            "/.well-known/stellar-history.json",
            axum::routing::get(move || {
                let request_count = request_count.clone();
                let count = request_count.fetch_add(1, Ordering::Relaxed);
                let well_known_content = if count >= advance_threshold {
                    // Return advanced .well-known after threshold
                    advanced_well_known.to_string()
                } else {
                    // Initial reads get initial .well-known
                    initial_well_known.to_string()
                };
                async move { well_known_content }
            }),
        )
        .fallback(get_service(ServeDir::new(archive_path.to_path_buf())));

    let (server_url, server_handle) = start_http_server_with_app(app).await;

    // Mirror the archive - it will get initial .well-known first, then advanced .well-known at the end
    let temp_dest = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dest.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: server_url.clone(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: None, // Unbounded mirror
        low: None,
        skip_optional: true,
        overwrite: false,
        allow_mirror_gaps: false,
        storage_config: test_storage_config(),
    };

    run_mirror(mirror_config).await.unwrap_or_else(|e| {
        server_handle.abort();
        panic!("Mirror failed: {}", e);
    });

    // Check what .well-known was written with the original destination .well-known file (127)
    let dest_well_known_path =
        std::path::Path::new(mirror_dest).join(".well-known/stellar-history.json");
    let dest_well_known_content = std::fs::read_to_string(&dest_well_known_path)
        .expect("Failed to read destination .well-known");
    let dest_well_known: serde_json::Value = serde_json::from_str(&dest_well_known_content)
        .expect("Failed to parse destination .well-known");

    let dest_well_known_ledger = dest_well_known["currentLedger"].as_u64().unwrap() as u32;
    assert_eq!(dest_well_known_ledger, 127);

    // Verify checkpoint 191 files were not downloaded (mirror used initial .well-known at 127)
    let dest = std::path::Path::new(mirror_dest);
    assert!(
        !dest.join("history/00/00/00/history-000000bf.json").exists(),
        "checkpoint 191 should not exist"
    );
    assert!(
        !dest.join("ledger/00/00/00/ledger-000000bf.xdr.gz").exists(),
        "checkpoint 191 should not exist"
    );

    // Verify checkpoint 127 files were mirrored
    assert!(
        dest.join("history/00/00/00/history-0000007f.json").exists(),
        "checkpoint 127 should exist"
    );
    assert!(
        dest.join("ledger/00/00/00/ledger-0000007f.xdr.gz").exists(),
        "checkpoint 127 should exist"
    );

    server_handle.abort();
}
