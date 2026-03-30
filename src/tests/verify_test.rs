//! Tests for bucket hash verification (--verify flag)
//!
//! These tests verify that the --verify flag properly validates bucket file contents
//! by decompressing and checking SHA256 hashes against the filename.

use super::utils::{
    copy_test_archive, get_files_by_pattern, start_flaky_server, test_archive_path,
    FlakyServerConfig,
};
use crate::test_helpers::{run_mirror, run_scan, test_storage_config, MirrorConfig, ScanConfig};
use flate2::write::GzEncoder;
use flate2::Compression;
use rstest::rstest;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

//=============================================================================
// Test Enums and Types
//=============================================================================

/// Operation type for parameterized tests
#[derive(Clone, Copy, Debug)]
enum Operation {
    Scan,
    Mirror,
}

impl Operation {
    /// Run the operation with --verify enabled
    async fn run_with_verify(&self, src_url: &str) -> Result<(), crate::Error> {
        match self {
            Operation::Scan => run_scan(ScanConfig::new(src_url).skip_optional().verify()).await,
            Operation::Mirror => {
                let dest_dir = TempDir::new().unwrap();
                run_mirror(
                    MirrorConfig::new(src_url, format!("file://{}", dest_dir.path().display()))
                        .skip_optional()
                        .verify(),
                )
                .await
            }
        }
    }

    /// Run the operation without --verify
    async fn run_without_verify(&self, src_url: &str) -> Result<(), crate::Error> {
        match self {
            Operation::Scan => run_scan(ScanConfig::new(src_url).skip_optional()).await,
            Operation::Mirror => {
                let dest_dir = TempDir::new().unwrap();
                run_mirror(
                    MirrorConfig::new(src_url, format!("file://{}", dest_dir.path().display()))
                        .skip_optional(),
                )
                .await
            }
        }
    }
}

/// Corruption type for testing verification failures
#[derive(Clone, Copy, Debug)]
enum CorruptionType {
    /// Invalid gzip data (garbage bytes)
    InvalidGzip,
    /// Valid gzip but content doesn't match expected hash
    HashMismatch,
}

impl CorruptionType {
    fn description(&self) -> &'static str {
        match self {
            CorruptionType::InvalidGzip => "corrupted gzip",
            CorruptionType::HashMismatch => "hash mismatch",
        }
    }
}

//=============================================================================
// Helper Functions
//=============================================================================

/// Get the first bucket file from an archive
fn get_first_bucket_file(archive_path: &Path) -> PathBuf {
    let bucket_files = get_files_by_pattern(archive_path, "/bucket-");
    assert!(!bucket_files.is_empty(), "Test archive has no bucket files");
    bucket_files.into_iter().next().unwrap()
}

/// Corrupt a bucket file according to the specified corruption type
fn corrupt_bucket_file(path: &Path, corruption: CorruptionType) {
    match corruption {
        CorruptionType::InvalidGzip => {
            // Write random garbage bytes (invalid gzip)
            let garbage = b"this is not valid gzip data at all, just garbage bytes!";
            std::fs::write(path, garbage).expect("Failed to write garbage to file");
        }
        CorruptionType::HashMismatch => {
            // Create a valid gzip file with wrong content
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(b"This is valid compressed content but with wrong hash!")
                .expect("Failed to write to encoder");
            let compressed = encoder.finish().expect("Failed to finish compression");
            std::fs::write(path, compressed).expect("Failed to write wrong content to file");
        }
    }
}

/// Set up a corrupted archive and return the path
fn setup_corrupted_archive(corruption: CorruptionType) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path().to_path_buf();

    copy_test_archive(&archive_path).expect("Failed to copy test archive");

    let bucket_file = get_first_bucket_file(&archive_path);
    corrupt_bucket_file(&bucket_file, corruption);

    (temp_dir, archive_path)
}

//=============================================================================
// Valid Archive Tests
//=============================================================================

/// Test that operations with --verify succeed on a valid archive
#[rstest]
#[case::scan(Operation::Scan)]
#[case::mirror(Operation::Mirror)]
#[tokio::test]
async fn test_verify_valid_archive(#[case] op: Operation) {
    let archive = format!("file://{}", test_archive_path().display());

    let result = op.run_with_verify(&archive).await;

    result.unwrap_or_else(|e| {
        panic!(
            "{:?} with --verify should succeed on valid archive: {}",
            op, e
        )
    });
}

//=============================================================================
// Corruption Detection Tests
//=============================================================================

/// Test that operations with --verify fail on corrupted bucket files
#[rstest]
#[case::scan_invalid_gzip(Operation::Scan, CorruptionType::InvalidGzip)]
#[case::scan_hash_mismatch(Operation::Scan, CorruptionType::HashMismatch)]
#[case::mirror_invalid_gzip(Operation::Mirror, CorruptionType::InvalidGzip)]
#[case::mirror_hash_mismatch(Operation::Mirror, CorruptionType::HashMismatch)]
#[tokio::test]
async fn test_verify_detects_corruption(#[case] op: Operation, #[case] corruption: CorruptionType) {
    let (_temp_dir, archive_path) = setup_corrupted_archive(corruption);
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_with_verify(&archive_url).await;

    assert!(
        result.is_err(),
        "{:?} with --verify should fail on {} but succeeded",
        op,
        corruption.description()
    );
}

/// Test that operations WITHOUT --verify pass even with corrupted bucket files
#[rstest]
#[case::scan_invalid_gzip(Operation::Scan, CorruptionType::InvalidGzip)]
#[case::scan_hash_mismatch(Operation::Scan, CorruptionType::HashMismatch)]
#[case::mirror_invalid_gzip(Operation::Mirror, CorruptionType::InvalidGzip)]
#[case::mirror_hash_mismatch(Operation::Mirror, CorruptionType::HashMismatch)]
#[tokio::test]
async fn test_no_verify_ignores_corruption(
    #[case] op: Operation,
    #[case] corruption: CorruptionType,
) {
    let (_temp_dir, archive_path) = setup_corrupted_archive(corruption);
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_without_verify(&archive_url).await;

    result.unwrap_or_else(|e| {
        panic!(
            "{:?} without --verify should pass even with {}: {}",
            op,
            corruption.description(),
            e
        )
    });
}

//=============================================================================
// Mirror with Verify End-to-End Test
//=============================================================================

/// Test that mirroring with --verify produces a verifiable archive
#[tokio::test]
async fn test_mirror_verify_produces_verifiable_archive() {
    let src_archive = format!("file://{}", test_archive_path().display());

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    // Mirror with verification
    run_mirror(
        MirrorConfig::new(&src_archive, format!("file://{}", mirror_dest))
            .skip_optional()
            .verify(),
    )
    .await
    .expect("Mirror with --verify should succeed");

    // Verify the mirrored archive with scan --verify
    run_scan(ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: None,
        storage_config: test_storage_config(),
        verify: true,
    })
    .await
    .expect("Scan of mirrored archive should pass with --verify");
}

//=============================================================================
// Error Classification Tests
//=============================================================================

/// Test that verification errors are fatal (non-retryable)
#[tokio::test]
async fn test_verify_error_is_fatal_no_retry() {
    let (_temp_dir, archive_path) = setup_corrupted_archive(CorruptionType::HashMismatch);

    let bucket_file = get_first_bucket_file(&archive_path);
    let bucket_filename = bucket_file.file_name().unwrap().to_str().unwrap();

    // Serve via HTTP to test that retries don't help
    let config = FlakyServerConfig {
        archive_path: Some(archive_path),
        ..Default::default()
    };

    let (server_url, tracker, server_handle) = start_flaky_server(config).await;

    let result = run_scan(ScanConfig::new(&server_url).skip_optional().verify()).await;

    server_handle.abort();

    // Should fail
    assert!(
        result.is_err(),
        "Scan with --verify should fail on hash mismatch"
    );

    // Check that the corrupted bucket was not retried (fatal errors are not retryable)
    let counts = tracker.get_counts();
    for (path, count) in counts {
        if path.contains(bucket_filename) {
            assert!(
                count == 1,
                "Corrupted bucket should not be retried (got {} requests, expected 1)",
                count
            );
        }
    }
}

//=============================================================================
// Partial Failure Tests
//=============================================================================

/// Mirror with --verify should write all valid files (except for corrupt ones)
/// even when one bucket is corrupt
#[tokio::test]
async fn test_mirror_verify_continues_on_corruption() {
    let (_temp_src, src_path) = setup_corrupted_archive(CorruptionType::HashMismatch);
    let src_bucket_count = get_files_by_pattern(&src_path, "/bucket-").len();
    let corrupt_bucket = get_first_bucket_file(&src_path);
    let corrupt_relative = corrupt_bucket.strip_prefix(&src_path).unwrap();

    let temp_dest = TempDir::new().unwrap();
    let dest_path = temp_dest.path();
    let src_url = format!("file://{}", src_path.display());
    let dest_url = format!("file://{}", dest_path.display());

    let result = run_mirror(
        MirrorConfig::new(&src_url, &dest_url)
            .skip_optional()
            .verify(),
    )
    .await;

    assert!(result.is_err(), "should fail due to corrupt bucket");
    assert!(
        !dest_path.join(corrupt_relative).exists(),
        "corrupt file should not be committed"
    );
    let dest_bucket_count = get_files_by_pattern(dest_path, "/bucket-").len();
    assert_eq!(
        dest_bucket_count,
        src_bucket_count - 1,
        "all valid buckets should be written (expected {}, got {})",
        src_bucket_count - 1,
        dest_bucket_count
    );
}
