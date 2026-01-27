//! HTTP error handling tests for scan and mirror operations
//!
//! This module tests retry behavior for HTTP errors across both operations:
//! - Transient errors (5xx, 408, 429) trigger retries with exponential backoff
//! - Permanent errors (4xx except 408, 429) do not trigger retries
//! - Connection drops trigger retries

use super::utils::{
    start_flaky_server, start_http_server_with_app, test_archive_path, transient_http_errors,
    FlakyServerConfig, RequestTracker,
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

    async fn run(&self, server_url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Operation::Scan => run_scan(ScanConfig::new(server_url).skip_optional().high(63))
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            Operation::Mirror => {
                let dest_dir = TempDir::new().unwrap();
                run_mirror(
                    MirrorConfig::new(server_url, format!("file://{}", dest_dir.path().display()))
                        .skip_optional()
                        .concurrency(1)
                        .high(63),
                )
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
#[case::scan(Operation::Scan)]
#[case::mirror(Operation::Mirror)]
#[tokio::test]
async fn test_retries_on_transient_well_known_errors(#[case] op: Operation) {
    let archive_path = test_archive_path();

    for (status_code, description) in transient_http_errors() {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            status_code,
            2,              // Fail twice, then succeed
            ".well-known/", // Target the .well-known file
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url).await;
        handle.abort();

        // Verify retries occurred on .well-known
        let counts = tracker.get_counts();
        let well_known_count = counts
            .get(".well-known/stellar-history.json")
            .copied()
            .unwrap_or(0);

        assert!(
            well_known_count > 1,
            "{op:?}: HTTP {status_code} ({description}) on .well-known should trigger retries, got count: {well_known_count}"
        );

        assert!(
            result.is_ok(),
            "{:?} should succeed after retrying .well-known HTTP {} ({}): {:?}",
            op,
            status_code,
            description,
            result.err()
        );
    }
}

#[rstest]
#[case::scan(Operation::Scan)]
#[case::mirror(Operation::Mirror)]
#[tokio::test]
async fn test_retries_on_transient_http_errors(#[case] op: Operation) {
    let archive_path = test_archive_path();

    for (status_code, description) in transient_http_errors() {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            status_code,
            2, // Fail twice, then succeed
            op.path_prefix(),
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url).await;
        handle.abort();

        let failure_type = format!("HTTP {status_code} ({description})");
        verify_retries_occurred(&tracker, op, &failure_type);
        assert!(
            result.is_ok(),
            "{:?} should succeed after retrying {}: {:?}",
            op,
            failure_type,
            result.err()
        );
    }
}

#[rstest]
#[case::scan(Operation::Scan)]
#[case::mirror(Operation::Mirror)]
#[tokio::test]
async fn test_retries_on_connection_drops(#[case] op: Operation) {
    let archive_path = test_archive_path();

    let config = FlakyServerConfig::archive_connection_drop(
        archive_path,
        2, // Fail twice, then succeed
        op.path_prefix(),
    );
    let (server_url, tracker, handle) = start_flaky_server(config).await;

    let result = op.run(&server_url).await;
    handle.abort();

    verify_retries_occurred(&tracker, op, "connection drop");
    assert!(
        result.is_ok(),
        "{:?} should succeed after retrying connection drops: {:?}",
        op,
        result.err()
    );
}

/// Tests that retry delays follow exponential backoff pattern.
/// Uses 1s initial backoff so 100ms system jitter is only ~10% tolerance.
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
        MirrorConfig::new(&server_url, format!("file://{}", dest_dir.path().display()))
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
        if let Err(e) = tracker.verify_backoff_timing(path, 1000, 100) {
            panic!("Exponential backoff failed for {path} with {fail_count} failures: {e}");
        }
    }
}

//=============================================================================
// Permanent Error Tests (no retries expected)
//=============================================================================

#[rstest]
#[case::scan(Operation::Scan)]
#[case::mirror(Operation::Mirror)]
#[tokio::test]
async fn test_fails_on_permanent_http_errors(#[case] op: Operation) {
    let archive_path = test_archive_path();

    for (status_code, description) in PERMANENT_HTTP_ERRORS {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            *status_code,
            1000, // Always fail
            op.path_prefix(),
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url).await;
        handle.abort();

        assert!(
            result.is_err(),
            "{op:?} should fail on HTTP {status_code} ({description})"
        );

        // Verify no retries occurred - each path should only be requested once
        let counts = tracker.get_counts();
        let retried: Vec<_> = counts
            .iter()
            .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
            .collect();
        assert!(
            retried.is_empty(),
            "{op:?}: HTTP {status_code} ({description}) should not trigger retries, but these paths were retried: {retried:?}"
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
/// Tests both with and without atomic file writes
#[rstest]
#[case::with_atomic_writes(true)]
#[case::without_atomic_writes(false)]
#[tokio::test]
async fn test_partial_body_retry_succeeds_without_corruption(#[case] atomic_file_writes: bool) {
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
    let mirror_config =
        MirrorConfig::new(&server_url, format!("file://{}", dest_dir.path().display()))
            .skip_optional()
            .concurrency(1)
            .high(63)
            .storage_config(storage_config);

    let result = run_mirror(mirror_config).await;
    handle.abort();

    // Mirror should succeed
    assert!(
        result.is_ok(),
        "Mirror (atomic_writes={}) should complete successfully: {:?}",
        atomic_file_writes,
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

    // Find any files with wrong sizes
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

    // Assert that no corruption occurred
    assert!(
        corrupted_files.is_empty(),
        "Expected no corrupted files (atomic_writes={}), but found {} corrupted files: {:?}",
        atomic_file_writes,
        corrupted_files.len(),
        corrupted_files
    );
}
