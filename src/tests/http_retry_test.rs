//! HTTP error handling tests for scan and mirror operations
//!
//! This module tests retry behavior for HTTP errors across both operations:
//! - Transient errors (5xx, 408, 429) trigger retries with exponential backoff
//! - Permanent errors (4xx except 408, 429) do not trigger retries
//! - Connection drops trigger retries

use super::utils::{
    file_url_from_path, start_flaky_server, start_http_server_with_app, test_archive_path,
    transient_http_errors, FlakyServerConfig, RequestTracker,
};
use crate::test_helpers::{run_mirror, run_scan, MirrorConfig, ScanConfig};
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get_service},
    Router,
};
use rstest::rstest;
use std::path::PathBuf;
use tempfile::TempDir;
use tower::util::ServiceExt;
use tower_http::services::ServeDir;

/// Permanent HTTP errors that should not trigger retry behavior.
const PERMANENT_HTTP_ERRORS: &[(u16, &str)] =
    &[(404, "Not Found"), (403, "Forbidden"), (400, "Bad Request")];

/// Operation type for parameterized tests
#[derive(Clone, Copy, Debug)]
pub enum Operation {
    Scan,
    Mirror,
}

impl Operation {
    fn path_prefix(&self) -> &'static str {
        match self {
            Operation::Scan => "history/",
            Operation::Mirror => "bucket/",
        }
    }

    async fn run(
        &self,
        server_url: &str,
        verify: bool,
        atomic_file_writes: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::storage::StorageConfig;
        use std::time::Duration;

        match self {
            Operation::Scan => {
                let mut config = ScanConfig::new(server_url).skip_optional().high(63);
                if verify {
                    config = config.verify();
                }
                run_scan(config)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
            Operation::Mirror => {
                let storage_config = StorageConfig::new(
                    3,
                    Duration::from_millis(100),
                    Duration::from_secs(30),
                    64,
                    Duration::from_secs(30),
                    Duration::from_secs(300),
                    0,
                    atomic_file_writes,
                );
                let dest_dir = TempDir::new().unwrap();
                let mut config = MirrorConfig::new(server_url, file_url_from_path(dest_dir.path()))
                    .skip_optional()
                    .concurrency(1)
                    .high(63)
                    .storage_config(storage_config);
                if verify {
                    config = config.verify();
                }
                run_mirror(config)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        }
    }
}

//=============================================================================
// Transient Error Tests (retries expected)
//=============================================================================

/// Tests that transient errors on .well-known/stellar-history.json trigger retries.
#[rstest]
#[case::scan(Operation::Scan, false, false)]
#[case::scan_verify(Operation::Scan, true, false)]
#[case::mirror(Operation::Mirror, false, false)]
#[case::mirror_verify(Operation::Mirror, true, false)]
#[case::mirror_atomic(Operation::Mirror, false, true)]
#[case::mirror_verify_atomic(Operation::Mirror, true, true)]
#[tokio::test]
async fn test_retries_on_transient_well_known_errors(
    #[case] op: Operation,
    #[case] verify: bool,
    #[case] atomic: bool,
) {
    let archive_path = test_archive_path();

    for (status_code, description) in transient_http_errors() {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            status_code,
            2,              // Fail twice, then succeed
            ".well-known/", // Target the .well-known file
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url, verify, atomic).await;
        handle.abort();

        // Verify retries occurred on .well-known
        let counts = tracker.get_counts();
        let well_known_count = counts
            .get(".well-known/stellar-history.json")
            .copied()
            .unwrap_or(0);

        assert!(
            well_known_count > 1,
            "{op:?} (verify={verify}, atomic={atomic}): HTTP {status_code} ({description}) on .well-known should trigger retries, got count: {well_known_count}"
        );

        assert!(
            result.is_ok(),
            "{:?} (verify={}, atomic={}) should succeed after retrying .well-known HTTP {} ({}): {:?}",
            op,
            verify,
            atomic,
            status_code,
            description,
            result.err()
        );
    }
}

#[rstest]
#[case::scan(Operation::Scan, false, false)]
#[case::scan_verify(Operation::Scan, true, false)]
#[case::mirror(Operation::Mirror, false, false)]
#[case::mirror_verify(Operation::Mirror, true, false)]
#[case::mirror_atomic(Operation::Mirror, false, true)]
#[case::mirror_verify_atomic(Operation::Mirror, true, true)]
#[tokio::test]
async fn test_retries_on_transient_http_errors(
    #[case] op: Operation,
    #[case] verify: bool,
    #[case] atomic: bool,
) {
    let archive_path = test_archive_path();

    for (status_code, description) in transient_http_errors() {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            status_code,
            2, // Fail twice, then succeed
            op.path_prefix(),
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url, verify, atomic).await;
        handle.abort();

        let failure_type = format!("HTTP {status_code} ({description})");
        verify_retries_occurred(&tracker, op, &failure_type);
        assert!(
            result.is_ok(),
            "{:?} (verify={}, atomic={}) should succeed after retrying {}: {:?}",
            op,
            verify,
            atomic,
            failure_type,
            result.err()
        );
    }
}

#[rstest]
#[case::scan(Operation::Scan, false, false)]
#[case::scan_verify(Operation::Scan, true, false)]
#[case::mirror(Operation::Mirror, false, false)]
#[case::mirror_verify(Operation::Mirror, true, false)]
#[case::mirror_atomic(Operation::Mirror, false, true)]
#[case::mirror_verify_atomic(Operation::Mirror, true, true)]
#[tokio::test]
async fn test_retries_on_connection_drops(
    #[case] op: Operation,
    #[case] verify: bool,
    #[case] atomic: bool,
) {
    let archive_path = test_archive_path();

    let config = FlakyServerConfig::archive_connection_drop(
        archive_path,
        2, // Fail twice, then succeed
        op.path_prefix(),
    );
    let (server_url, tracker, handle) = start_flaky_server(config).await;

    let result = op.run(&server_url, verify, atomic).await;
    handle.abort();

    verify_retries_occurred(&tracker, op, "connection drop");
    assert!(
        result.is_ok(),
        "{:?} (verify={}, atomic={}) should succeed after retrying connection drops: {:?}",
        op,
        verify,
        atomic,
        result.err()
    );
}

/// Tests that retry delays follow exponential backoff pattern.
#[rstest]
#[case::one_failure(1)]
#[case::two_failures(2)]
#[case::three_failures(3)]
#[tokio::test]
async fn test_exponential_backoff_timing(#[case] fail_count: usize) {
    use crate::storage::StorageConfig;
    use std::time::Duration;

    let archive_path = test_archive_path();

    // Use 503 Service Unavailable as a representative transient error
    let config = FlakyServerConfig::archive_status_error(archive_path, 503, fail_count, "bucket/");
    let (server_url, tracker, handle) = start_flaky_server(config).await;

    let storage_config = StorageConfig::new(
        3,
        Duration::from_secs(1), // Use large timeout values so jitter doesn't affect unit tests.
        Duration::from_secs(30),
        64,
        Duration::from_secs(30),
        Duration::from_secs(300),
        0,
        false,
    );

    let dest_dir = TempDir::new().unwrap();
    let result = run_mirror(
        MirrorConfig::new(&server_url, file_url_from_path(dest_dir.path()))
            .skip_optional()
            .concurrency(1)
            .high(63)
            .storage_config(storage_config),
    )
    .await;
    handle.abort();

    assert!(
        result.is_ok(),
        "Mirror should succeed after {} retries: {:?}",
        fail_count,
        result.err()
    );

    let counts = tracker.get_counts();
    let retried_paths: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with("bucket/") && **count == fail_count + 1)
        .collect();

    assert!(
        !retried_paths.is_empty(),
        "Expected at least one bucket file to be retried {} times, got counts: {:?}",
        fail_count,
        counts
            .iter()
            .filter(|(p, _)| p.starts_with("bucket/"))
            .collect::<Vec<_>>()
    );

    for (path, _) in &retried_paths {
        if let Err(e) = tracker.verify_backoff_timing(path, 1000, 200) {
            panic!("Exponential backoff failed for {path} with {fail_count} failures: {e}");
        }
    }
}

//=============================================================================
// Permanent Error Tests (no retries expected)
//=============================================================================

#[rstest]
#[case::scan(Operation::Scan, false, false)]
#[case::scan_verify(Operation::Scan, true, false)]
#[case::mirror(Operation::Mirror, false, false)]
#[case::mirror_verify(Operation::Mirror, true, false)]
#[case::mirror_atomic(Operation::Mirror, false, true)]
#[case::mirror_verify_atomic(Operation::Mirror, true, true)]
#[tokio::test]
async fn test_fails_on_permanent_http_errors(
    #[case] op: Operation,
    #[case] verify: bool,
    #[case] atomic: bool,
) {
    let archive_path = test_archive_path();

    for (status_code, description) in PERMANENT_HTTP_ERRORS {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            *status_code,
            1000, // Always fail
            op.path_prefix(),
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url, verify, atomic).await;
        handle.abort();

        assert!(
            result.is_err(),
            "{op:?} (verify={verify}, atomic={atomic}) should fail on HTTP {status_code} ({description})"
        );

        // Verify no retries occurred - each path should only be requested once
        let counts = tracker.get_counts();
        let retried: Vec<_> = counts
            .iter()
            .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
            .collect();
        assert!(
            retried.is_empty(),
            "{op:?} (verify={verify}, atomic={atomic}): HTTP {status_code} ({description}) should not trigger retries, but these paths were retried: {retried:?}"
        );
    }
}

//=============================================================================
// Helper Functions
//=============================================================================

fn verify_retries_occurred(tracker: &RequestTracker, op: Operation, failure_type: &str) {
    let counts = tracker.get_counts();
    let retried: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
        .collect();

    assert!(
        !retried.is_empty(),
        "{:?}: {} should trigger retries, got counts: {:?}",
        op,
        failure_type,
        counts
            .iter()
            .filter(|(p, _)| p.starts_with(op.path_prefix()))
            .collect::<Vec<_>>()
    );
}

//=============================================================================
// HEAD Request Edge Cases
//=============================================================================

/// HEAD response behavior for testing edge cases
#[derive(Clone, Copy)]
enum HeadResponse {
    /// Return 200 OK without Content-Length header
    NoContentLength,
    /// Return 200 OK with Content-Length: 0
    ZeroContentLength,
    /// Return 405 Method Not Allowed
    MethodNotAllowed,
    /// Normal response
    Normal,
}

fn make_head_response(behavior: HeadResponse) -> Response<Body> {
    match behavior {
        HeadResponse::NoContentLength => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap(),
        HeadResponse::ZeroContentLength => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Length", "0")
            .body(Body::empty())
            .unwrap(),
        HeadResponse::MethodNotAllowed => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("Allow", "GET")
            .body(Body::empty())
            .unwrap(),
        HeadResponse::Normal => unreachable!("Should not be called for Normal behavior"),
    }
}

#[rstest]
#[case::no_content_length(HeadResponse::NoContentLength, true, "no Content-Length")]
#[case::zero_content_length(HeadResponse::ZeroContentLength, true, "Content-Length: 0")]
#[case::method_not_allowed(HeadResponse::MethodNotAllowed, true, "405 Method Not Allowed")]
#[case::proper_response(HeadResponse::Normal, false, "proper Content-Length")]
#[tokio::test]
async fn test_scan_head_response_handling(
    #[case] behavior: HeadResponse,
    #[case] expect_failure: bool,
    #[case] description: &str,
) {
    let archive_path = test_archive_path();

    let app = if matches!(behavior, HeadResponse::Normal) {
        Router::new().fallback(get_service(ServeDir::new(&archive_path)))
    } else {
        let serve_dir = ServeDir::new(&archive_path);
        Router::new().fallback(any(move |req: Request<Body>| {
            let serve_dir = serve_dir.clone();
            async move {
                if req.method() == Method::HEAD {
                    make_head_response(behavior)
                } else {
                    match serve_dir.oneshot(req).await {
                        Ok(resp) => resp.into_response(),
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                    }
                }
            }
        }))
    };

    let (server_url, server_handle) = start_http_server_with_app(app).await;
    let result = run_scan(ScanConfig::new(&server_url).skip_optional().high(63)).await;
    server_handle.abort();

    if expect_failure {
        assert!(
            result.is_err(),
            "Scan should fail when HEAD returns {description}"
        );
    } else {
        assert!(
            result.is_ok(),
            "Scan should succeed when HEAD returns {description}"
        );
    }
}

//=============================================================================
// Mid-stream Failure Tests
//=============================================================================

/// Tests that retries properly handle mid-stream download failures
/// without causing file corruption.
///
/// The server sends 200 OK with Content-Length but drops the connection after
/// sending partial data (25%). The pipeline detects this and retries the entire
/// file download from scratch, ensuring the destination file is not corrupted.
/// When verify=true, also independently verifies each bucket file's SHA256 hash
/// by decompressing and hashing, comparing against the hash in the filename.
#[rstest]
#[case::atomic_no_verify(true, false)]
#[case::atomic_with_verify(true, true)]
#[case::non_atomic_no_verify(false, false)]
#[case::non_atomic_with_verify(false, true)]
#[tokio::test]
async fn test_partial_body_retry_succeeds_without_corruption(
    #[case] atomic_file_writes: bool,
    #[case] verify: bool,
) {
    use crate::storage::StorageConfig;
    use std::time::Duration;

    let archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Server sends 200 OK with Content-Length but only 25% of body, twice, then succeeds
    let fail_count = 2;
    let config = FlakyServerConfig::archive_partial_body(archive_path.clone(), fail_count);
    let (server_url, tracker, handle) = start_flaky_server(config).await;

    let storage_config = StorageConfig::new(
        3,                          // max_retries
        Duration::from_millis(100), // retry_min_delay
        Duration::from_secs(30),    // retry_max_delay
        64,                         // max_concurrent
        Duration::from_secs(30),    // timeout
        Duration::from_secs(300),   // io_timeout
        0,                          // bandwidth_limit
        atomic_file_writes,
    );

    let dest_dir = TempDir::new().unwrap();
    let mut mirror_config = MirrorConfig::new(&server_url, file_url_from_path(dest_dir.path()))
        .skip_optional()
        .concurrency(1)
        .high(63)
        .storage_config(storage_config);

    if verify {
        mirror_config = mirror_config.verify();
    }

    let result = run_mirror(mirror_config).await;
    handle.abort();

    // Mirror should succeed
    assert!(
        result.is_ok(),
        "Mirror (atomic_writes={}, verify={}) should complete successfully: {:?}",
        atomic_file_writes,
        verify,
        result.err()
    );

    // Verify retries occurred
    let counts = tracker.get_counts();
    let bucket_retries: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with("bucket/") && **count > 1)
        .collect();
    assert!(
        !bucket_retries.is_empty(),
        "Expected bucket files to be retried, got counts: {counts:?}"
    );

    // Verify file sizes match source
    let dest_bucket_dir = dest_dir.path().join("bucket");
    let mut corrupted_files = Vec::new();

    for entry in walkdir::WalkDir::new(&dest_bucket_dir)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        let rel_path = entry.path().strip_prefix(dest_dir.path()).unwrap();
        let src_file = archive_path.join(rel_path);

        let expected_size = std::fs::metadata(&src_file).unwrap().len() as usize;
        let actual_size = std::fs::metadata(entry.path()).unwrap().len() as usize;

        if actual_size != expected_size {
            corrupted_files.push((rel_path.display().to_string(), expected_size, actual_size));
        }
    }

    assert!(
        corrupted_files.is_empty(),
        "Expected no corrupted files (atomic_writes={}, verify={}), but found {} corrupted: {:?}",
        atomic_file_writes,
        verify,
        corrupted_files.len(),
        corrupted_files
    );

    // When verify is enabled, also independently verify bucket hashes
    if verify {
        use flate2::read::GzDecoder;
        use sha2::{Digest, Sha256};
        use std::io::Read;

        let mut verified_count = 0;
        let mut hash_errors = Vec::new();

        for entry in walkdir::WalkDir::new(&dest_bucket_dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let path = entry.path();
            let filename = path.file_name().unwrap().to_str().unwrap();

            // Skip non-bucket files
            if !filename.starts_with("bucket-") || !filename.ends_with(".xdr.gz") {
                continue;
            }

            // Extract expected hash from filename (bucket-{hash}.xdr.gz)
            let expected_hash = filename
                .strip_prefix("bucket-")
                .and_then(|s| s.strip_suffix(".xdr.gz"))
                .unwrap();

            // Read, decompress, and hash
            let compressed_data = std::fs::read(path).unwrap();
            let mut decoder = GzDecoder::new(&compressed_data[..]);
            let mut decompressed = Vec::new();

            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => {
                    let mut hasher = Sha256::new();
                    hasher.update(&decompressed);
                    let actual_hash = hex::encode(hasher.finalize());

                    if actual_hash == expected_hash {
                        verified_count += 1;
                    } else {
                        hash_errors.push(format!(
                            "{}: expected {}, got {}",
                            filename, expected_hash, actual_hash
                        ));
                    }
                }
                Err(e) => {
                    hash_errors.push(format!("{}: decompression failed: {}", filename, e));
                }
            }
        }

        assert!(
            hash_errors.is_empty(),
            "Bucket hash verification failed (atomic_writes={}): {:?}",
            atomic_file_writes,
            hash_errors
        );

        assert!(
            verified_count > 0,
            "Expected to verify at least one bucket file"
        );
    }
}
