//! Tests for scan command operations

use crate::test_helpers::{run_scan, ScanConfig};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use walkdir::WalkDir;

// General helper functions

/// Get the test archive path
fn get_test_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

/// Copy the test archive to a temporary directory
fn copy_test_archive(dst: &Path) -> Result<(), std::io::Error> {
    let src = get_test_archive_path();

    for entry in WalkDir::new(&src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(&src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path)?;
        } else {
            std::fs::copy(src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Get all files of a specific type from the archive
fn get_files_by_pattern(archive_path: &Path, pattern: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in WalkDir::new(archive_path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let path = entry.path();
            let path_str = path.to_string_lossy();
            if path_str.contains(pattern) {
                files.push(path.to_path_buf());
            }
        }
    }
    files
}

/// Randomly remove one file from the list
fn remove_random_file(files: &[PathBuf]) -> Option<PathBuf> {
    if files.is_empty() {
        return None;
    }

    let mut rng = StdRng::from_entropy();
    let index = rng.gen_range(0..files.len());
    let file_to_remove = &files[index];

    if std::fs::remove_file(file_to_remove).is_ok() {
        Some(file_to_remove.clone())
    } else {
        None
    }
}

// Bucket Validation Helper Functions

/// Helper function to collect bucket hashes from a history file JSON
fn collect_buckets_from_json(json: &serde_json::Value) -> std::collections::HashSet<String> {
    let mut buckets = std::collections::HashSet::new();
    if let Some(bucket_array) = json["currentBuckets"].as_array() {
        for bucket in bucket_array {
            if let Some(curr) = bucket["curr"].as_str() {
                if !curr.starts_with("0000000") {
                    buckets.insert(curr.to_string());
                }
            }
            if let Some(snap) = bucket["snap"].as_str() {
                if !snap.starts_with("0000000") {
                    buckets.insert(snap.to_string());
                }
            }
        }
    }
    buckets
}

/// Helper function to collect all buckets from the archive and categorize them
fn analyze_archive_buckets(
    archive_path: &Path,
) -> (
    std::collections::HashSet<String>, // buckets in .well-known
    std::collections::HashSet<String>, // buckets in historical files
) {
    // Read .well-known/stellar-history.json
    let well_known_path = archive_path.join(".well-known/stellar-history.json");
    let well_known_content =
        std::fs::read_to_string(&well_known_path).expect("Failed to read root HAS");
    let well_known: serde_json::Value =
        serde_json::from_str(&well_known_content).expect("Failed to parse root HAS");
    let well_known_buckets = collect_buckets_from_json(&well_known);

    // Collect buckets from all historical checkpoint files
    let all_history_files = get_files_by_pattern(archive_path, "/history-");
    let mut historical_buckets = std::collections::HashSet::new();

    for history_file in &all_history_files {
        if history_file.to_string_lossy().contains(".well-known") {
            continue;
        }

        let content = std::fs::read_to_string(history_file).expect("Failed to read history file");
        if let Ok(history_data) = serde_json::from_str::<serde_json::Value>(&content) {
            historical_buckets.extend(collect_buckets_from_json(&history_data));
        }
    }

    (well_known_buckets, historical_buckets)
}

/// Helper function to remove a bucket file and verify scan fails
async fn remove_bucket_and_verify_scan_fails(
    archive_path: &Path,
    bucket_hash: &str,
    error_msg: &str,
) {
    let bucket_files = get_files_by_pattern(archive_path, "/bucket-");

    for bucket_file in &bucket_files {
        if bucket_file.to_string_lossy().contains(bucket_hash) {
            std::fs::remove_file(bucket_file).expect("Failed to remove bucket file");

            let scan_config = ScanConfig {
                archive: format!("file://{}", archive_path.to_str().unwrap()),
                concurrency: 8,
                skip_optional: false,
                low: None,
                high: None,
            };

            run_scan(scan_config).await.expect_err(error_msg);
            return;
        }
    }
}

/// Helper function to test scan failure when a file type is missing
async fn test_scan_with_missing_file(file_pattern: &str, file_type: &str) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Get files matching the pattern, excluding .well-known for history files
    let files: Vec<_> = if file_pattern == "/history-" {
        get_files_by_pattern(archive_path, file_pattern)
            .into_iter()
            .filter(|p| !p.to_string_lossy().contains(".well-known"))
            .collect()
    } else {
        get_files_by_pattern(archive_path, file_pattern)
    };

    // Remove a random file of this type
    remove_random_file(&files);

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should fail with missing file
    run_scan(scan_config)
        .await
        .expect_err(&format!("Scan should fail with missing {} file", file_type));
}

//=============================================================================
// Range Validation Tests
//=============================================================================

#[tokio::test]
async fn test_scan_with_range() {
    let test_archive_path = get_test_archive_path();

    // Test scanning a specific range
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: false,
        low: Some(1000),
        high: Some(5000),
    };

    // Should succeed - range is within archive bounds
    let result = run_scan(config).await;
    assert!(result.is_ok(), "Scan with valid range should succeed");
}

#[tokio::test]
async fn test_scan_low_beyond_current() {
    let test_archive_path = get_test_archive_path();

    // Test with low beyond the archive's current ledger
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(20000),
        high: None,
    };

    // Should fail with appropriate error
    let result = run_scan(config).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("No available checkpoints"));
}

#[tokio::test]
async fn test_scan_high_beyond_current() {
    let test_archive_path = get_test_archive_path();

    // Test with high beyond the archive's current ledger
    // This should warn but not fail
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: Some(20000), // Beyond archive's current ledger
    };

    let result = run_scan(config).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_scan_empty_range() {
    let test_archive_path = get_test_archive_path();

    // Test with low > high
    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: true,
        low: Some(5000),
        high: Some(1000), // high < low
    };

    // Should fail with appropriate error
    let result = run_scan(config).await;
    assert!(result.is_err(), "Scan with low > high should fail");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("greater than high checkpoint"),
        "Error should mention low > high"
    );
}

// Bucket Validation Tests

#[tokio::test]
async fn test_scan_bucket_only_in_historical_checkpoints() {
    // Test that scan validates buckets referenced in historical checkpoints
    // even if they're not in the root .well-known file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    let (well_known_buckets, historical_buckets) = analyze_archive_buckets(archive_path);

    // Find buckets that are only in historical checkpoints, not root .well-known
    let buckets_only_in_historical: Vec<_> = historical_buckets
        .difference(&well_known_buckets)
        .cloned()
        .collect();

    if !buckets_only_in_historical.is_empty() {
        // Remove a bucket that's only referenced by historical checkpoints
        let mut rng = StdRng::from_entropy();
        let index = rng.gen_range(0..buckets_only_in_historical.len());
        remove_bucket_and_verify_scan_fails(
            archive_path,
            &buckets_only_in_historical[index],
            "Scan should fail with missing bucket from historical checkpoint",
        )
        .await;
    }
}

#[tokio::test]
async fn test_scan_bucket_only_in_well_known() {
    // Test that scan validates buckets referenced only in the root .well-known file
    // and not in any other history file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    let (well_known_buckets, historical_buckets) = analyze_archive_buckets(archive_path);

    // Find buckets that are ONLY in .well-known and NOT in any historical files
    let buckets_only_in_well_known: Vec<_> = well_known_buckets
        .difference(&historical_buckets)
        .cloned()
        .collect();

    if buckets_only_in_well_known.is_empty() {
        // Skip test if there are no buckets unique to .well-known
        // This is expected for a well-formed archive where .well-known matches the latest checkpoint
        return;
    }

    // Remove a bucket that's only referenced in .well-known
    let mut rng = StdRng::from_entropy();
    let index = rng.gen_range(0..buckets_only_in_well_known.len());
    remove_bucket_and_verify_scan_fails(
        archive_path,
        &buckets_only_in_well_known[index],
        "Scan should fail with missing bucket only in .well-known",
    )
    .await;
}

// File Corruption Tests

#[tokio::test]
async fn test_scan_missing_history() {
    test_scan_with_missing_file("/history-", "history").await;
}

#[tokio::test]
async fn test_scan_missing_ledger() {
    test_scan_with_missing_file("/ledger-", "ledger").await;
}

#[tokio::test]
async fn test_scan_missing_transactions() {
    test_scan_with_missing_file("/transactions-", "transactions").await;
}

#[tokio::test]
async fn test_scan_missing_results() {
    test_scan_with_missing_file("/results-", "results").await;
}

#[tokio::test]
async fn test_scan_missing_scp_with_optional_flag() {
    // Test that skip_optional flag controls whether missing SCP files cause scan failure
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Remove a random SCP file to corrupt the archive
    let scp_files = get_files_by_pattern(archive_path, "/scp-");
    remove_random_file(&scp_files);

    // First scan with skip_optional: false - should fail
    let scan_required = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false, // SCP files are required
        low: None,
        high: None,
    };

    run_scan(scan_required)
        .await
        .expect_err("Scan should fail with missing SCP file when skip_optional is false");

    // Second scan with skip_optional: true - should succeed
    let scan_optional = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: true, // SCP files are optional
        low: None,
        high: None,
    };

    run_scan(scan_optional)
        .await
        .expect("Scan should succeed with missing SCP file when skip_optional is true");
}

// Test ranges and cli flags

#[tokio::test]
async fn test_scan_complete_archive() {
    let test_archive_path = get_test_archive_path();

    let scan_config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should succeed on complete archive
    run_scan(scan_config)
        .await
        .expect("Scan should succeed on complete archive");
}

#[tokio::test]
async fn test_scan_missing_well_known() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Remove the .well-known/stellar-history.json file
    let well_known_path = archive_path.join(".well-known/stellar-history.json");
    std::fs::remove_file(&well_known_path).expect("Failed to remove .well-known file");

    // Scan should fail without the root HAS file
    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    run_scan(scan_config)
        .await
        .expect_err("Scan should fail when .well-known/stellar-history.json is missing");
}

#[tokio::test]
async fn test_scan_well_known_not_in_history() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Remove the history file that corresponds to the current checkpoint in .well-known
    // The test archive's .well-known has currentLedger: 10047 (checkpoint 0x273f)
    let history_path = archive_path.join("history/00/00/27/history-0000273f.json");
    std::fs::remove_file(&history_path)
        .expect("Failed to remove history file at current checkpoint");

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should fail because the history file referenced by .well-known is missing
    run_scan(scan_config)
        .await
        .expect_err("Scan should fail when current checkpoint's history file is missing");
}

#[tokio::test]
async fn test_scan_empty_file() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Pick a random history file and make it empty
    let history_files = get_files_by_pattern(archive_path, "/history-");
    let non_wellknown_history: Vec<_> = history_files
        .into_iter()
        .filter(|p| !p.to_string_lossy().contains(".well-known"))
        .collect();

    let mut rng = StdRng::from_entropy();
    let index = rng.gen_range(0..non_wellknown_history.len());
    let file_to_empty = &non_wellknown_history[index];
    std::fs::write(file_to_empty, "").expect("Failed to empty file");

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should fail because the file exists but is empty
    run_scan(scan_config)
        .await
        .expect_err("Scan should fail when a required file is empty");
}

#[tokio::test]
async fn test_scan_empty_bucket_file() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();

    copy_test_archive(archive_path).expect("Failed to copy test archive");

    // Pick a random bucket file and make it empty
    let bucket_files = get_files_by_pattern(archive_path, "/bucket-");
    let mut rng = StdRng::from_entropy();
    let index = rng.gen_range(0..bucket_files.len());
    let file_to_empty = &bucket_files[index];
    std::fs::write(file_to_empty, "").expect("Failed to empty bucket file");

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    // Scan should fail because the bucket file is empty
    run_scan(scan_config)
        .await
        .expect_err("Scan should fail when a bucket file is empty");
}
