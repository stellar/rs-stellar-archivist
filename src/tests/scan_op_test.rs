//! Tests for scan command operations

use super::utils::{copy_test_archive, get_files_by_pattern, start_http_server, test_archive_path};
use crate::storage::{OpendalStore, Storage, StorageConfig};
use crate::test_helpers::{run_scan, ScanConfig};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

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

//=============================================================================
// Range Validation Tests
//=============================================================================

#[tokio::test]
async fn test_scan_low_beyond_current() {
    let archive = format!("file://{}", test_archive_path().display());

    // Test with low beyond the archive's current ledger - should fail
    let result = run_scan(ScanConfig::new(&archive).skip_optional().low(20000)).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("No available checkpoints"));
}

#[tokio::test]
async fn test_scan_high_beyond_current() {
    let archive = format!("file://{}", test_archive_path().display());

    // Test with high beyond the archive's current ledger - should warn but not fail
    let result = run_scan(ScanConfig::new(&archive).skip_optional().high(20000)).await;
    assert!(result.is_ok());
}

// Bucket Validation Tests

/// Which bucket source to test
#[derive(Clone, Copy)]
enum BucketSource {
    /// Remove a bucket from the current state (.well-known)
    CurrentState,
    /// Remove a bucket only referenced by historical checkpoints (not in .well-known)
    HistoricalOnly,
}

#[rstest]
#[case::current_state(BucketSource::CurrentState, "bucket from current state")]
#[case::historical_only(BucketSource::HistoricalOnly, "bucket only in historical checkpoints")]
#[tokio::test]
async fn test_scan_detects_missing_bucket(#[case] source: BucketSource, #[case] description: &str) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();
    copy_test_archive(archive_path).expect("Failed to copy test archive");

    let (well_known_buckets, historical_buckets) = analyze_archive_buckets(archive_path);

    let bucket_to_remove = match source {
        BucketSource::CurrentState => {
            // Pick any bucket from .well-known (current state)
            assert!(
                !well_known_buckets.is_empty(),
                "Test archive must have buckets in .well-known"
            );
            let buckets: Vec<_> = well_known_buckets.iter().cloned().collect();
            let mut rng = StdRng::from_entropy();
            buckets[rng.gen_range(0..buckets.len())].clone()
        }
        BucketSource::HistoricalOnly => {
            // Pick a bucket only in historical checkpoints (not in current state)
            // This tests that scan collects buckets from all checkpoints, not just .well-known
            let only_historical: Vec<_> = historical_buckets
                .difference(&well_known_buckets)
                .cloned()
                .collect();
            assert!(
                !only_historical.is_empty(),
                "Test archive must have buckets only in historical checkpoints to verify \
                 scan collects from all sources"
            );
            let mut rng = StdRng::from_entropy();
            only_historical[rng.gen_range(0..only_historical.len())].clone()
        }
    };

    remove_bucket_and_verify_scan_fails(
        archive_path,
        &bucket_to_remove,
        &format!("Scan should fail with missing {}", description),
    )
    .await;
}

// File Corruption Tests

/// Corruption types for testing scan failure detection
enum Corruption {
    /// Remove a random file matching the pattern
    Remove(&'static str),
    /// Empty a random file matching the pattern
    Empty(&'static str),
    /// Remove an exact file path relative to archive root
    RemoveExact(&'static str),
}

// Verify that many different types of corruptions are caught. Empty files, missing files,
// and entire missing paths.
#[rstest]
#[case::missing_history(Corruption::Remove("/history-"), "missing history file")]
#[case::missing_ledger(Corruption::Remove("/ledger-"), "missing ledger file")]
#[case::missing_transactions(Corruption::Remove("/transactions-"), "missing transactions file")]
#[case::missing_results(Corruption::Remove("/results-"), "missing results file")]
#[case::missing_well_known(
    Corruption::RemoveExact(".well-known/stellar-history.json"),
    "missing .well-known"
)]
#[case::missing_checkpoint_history(
    Corruption::RemoveExact("history/00/00/27/history-0000273f.json"),
    "missing current checkpoint history"
)]
#[case::empty_history(Corruption::Empty("/history-"), "empty history file")]
#[case::empty_bucket(Corruption::Empty("/bucket-"), "empty bucket file")]
#[tokio::test]
async fn test_scan_detects_corrupt_files(
    #[case] corruption: Corruption,
    #[case] description: &str,
) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path();
    copy_test_archive(archive_path).expect("Failed to copy test archive");

    match corruption {
        Corruption::Remove(pattern) => {
            let files: Vec<_> = get_files_by_pattern(archive_path, pattern)
                .into_iter()
                .filter(|p| !p.to_string_lossy().contains(".well-known"))
                .collect();
            assert!(
                !files.is_empty(),
                "Test archive must have files matching pattern '{}'",
                pattern
            );
            remove_random_file(&files);
        }
        Corruption::Empty(pattern) => {
            let files: Vec<_> = get_files_by_pattern(archive_path, pattern)
                .into_iter()
                .filter(|p| !p.to_string_lossy().contains(".well-known"))
                .collect();
            assert!(
                !files.is_empty(),
                "Test archive must have files matching pattern '{}'",
                pattern
            );
            let mut rng = StdRng::from_entropy();
            let idx = rng.gen_range(0..files.len());
            std::fs::write(&files[idx], "").expect("Failed to empty file");
        }
        Corruption::RemoveExact(path) => {
            std::fs::remove_file(archive_path.join(path)).expect("Failed to remove file");
        }
    }

    let scan_config = ScanConfig {
        archive: format!("file://{}", archive_path.to_str().unwrap()),
        concurrency: 8,
        skip_optional: false,
        low: None,
        high: None,
    };

    run_scan(scan_config)
        .await
        .expect_err(&format!("Scan should fail with {}", description));
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
    let test_archive_path = test_archive_path();

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

//=============================================================================
// HTTP Scan Tests
//=============================================================================

/// Tests basic HTTP scan of a valid archive (happy path).
#[tokio::test]
async fn test_scan_http_archive() {
    let archive_path = test_archive_path();
    let (server_url, server_handle) = start_http_server(&archive_path).await;

    let scan_config = ScanConfig {
        archive: server_url.clone(),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
    };

    run_scan(scan_config).await.unwrap_or_else(|e| {
        server_handle.abort();
        panic!("HTTP scan failed: {}", e);
    });

    server_handle.abort();
}

/// Smoke test against live Stellar archive infrastructure.
/// Tests HTTP/HTTPS connections and trailing slash URL handling.
#[tokio::test]
#[ignore]
async fn smoke_live_stellar_archive_connection() {
    let urls = [
        "http://history.stellar.org/prd/core-live/core_live_001",
        "https://history.stellar.org/prd/core-live/core_live_001/",
    ];

    for url_str in urls {
        let config = StorageConfig::default();
        let store = OpendalStore::http(url_str, &config)
            .unwrap_or_else(|e| panic!("{url_str}: failed to create store: {e}"));
        let mut reader = store
            .open_reader(".well-known/stellar-history.json")
            .await
            .unwrap_or_else(|e| panic!("{url_str}: connection failed: {e}"));

        let mut buf = [0u8; 100];
        let n = tokio::io::AsyncReadExt::read(&mut reader, &mut buf)
            .await
            .unwrap_or_else(|e| panic!("{url_str}: read failed: {e}"));

        assert!(n > 0, "{url_str}: no data received");
    }
}
