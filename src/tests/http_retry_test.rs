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
            "{:?}: HTTP {} ({}) on .well-known should trigger retries, got count: {}",
            op,
            status_code,
            description,
            well_known_count
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

        verify_retries_occurred(&tracker, op, status_code, description);
        assert!(
            result.is_ok(),
            "{:?} should succeed after retrying HTTP {} ({}): {:?}",
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

    let counts = tracker.get_counts();
    let retried: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
        .collect();

    assert!(
        !retried.is_empty(),
        "{:?}: Connection drops should trigger retries, got counts: {:?}",
        op,
        counts
            .iter()
            .filter(|(p, _)| p.starts_with(op.path_prefix()))
            .collect::<Vec<_>>()
    );
    assert!(
        result.is_ok(),
        "{:?} should succeed after retrying connection drops: {:?}",
        op,
        result.err()
    );
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
        let (server_url, _tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url).await;
        handle.abort();

        assert!(
            result.is_err(),
            "{:?} should fail on HTTP {} ({})",
            op,
            status_code,
            description
        );
    }
}

//=============================================================================
// Helper Functions
//=============================================================================

fn verify_retries_occurred(
    tracker: &RequestTracker,
    op: Operation,
    status_code: u16,
    description: &str,
) {
    let counts = tracker.get_counts();
    let retried: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
        .collect();

    assert!(
        !retried.is_empty(),
        "{:?}: HTTP {} ({}) should trigger retries, got counts: {:?}",
        op,
        status_code,
        description,
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
            "Scan should fail when HEAD returns {}",
            description
        );
    } else {
        assert!(
            result.is_ok(),
            "Scan should succeed when HEAD returns {}",
            description
        );
    }
}

//=============================================================================
// Mid-stream Failure Tests
//=============================================================================

/// Demonstrates a data corruption bug with mid-stream download failures.
///
/// When the server sends 200 OK with Content-Length but drops the connection after
/// sending partial data, OpenDAL's retry layer retries the HTTP request from byte 0.
/// However, our streaming write has already written the partial data to the destination.
/// The retry data gets APPENDED, resulting in a corrupted file.
///
/// Example with a 129-byte file and fail_count=2 (25% partial each time):
///   - Attempt 1: writes 32 bytes, connection drops
///   - Attempt 2: writes 32 bytes (from byte 0), connection drops
///   - Attempt 3: writes 129 bytes (from byte 0)
///   - Result: 32 + 32 + 129 = 193 bytes (CORRUPTED)
///
/// This test verifies the bug exists by checking that files are larger than expected.
/// Note: atomic_file_writes=true doesn't help because the temp file still receives
/// appended data before being renamed.
#[tokio::test]
async fn test_partial_body_retry_causes_corruption() {
    use crate::storage::StorageConfig;
    use std::time::Duration;

    let archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Server sends 200 OK with Content-Length but only 25% of body, twice, then succeeds
    let fail_count = 2;
    let config = FlakyServerConfig::archive_partial_body(archive_path.clone(), fail_count);
    let (server_url, tracker, handle) = start_flaky_server(config).await;

    // Enable atomic writes to verify they don't prevent the corruption
    let storage_config = StorageConfig::new(
        3,                          // max_retries
        Duration::from_millis(100), // retry_min_delay
        Duration::from_secs(30),    // retry_max_delay
        64,                         // max_concurrent
        Duration::from_secs(30),    // timeout
        Duration::from_secs(300),   // io_timeout
        0,                          // bandwidth_limit
        true,                       // atomic_file_writes
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

    // Mirror "succeeds" because OpenDAL retries are transparent
    assert!(
        result.is_ok(),
        "Mirror should complete (retries are transparent): {:?}",
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
        "Expected bucket files to be retried, got counts: {:?}",
        counts
    );

    // Find corrupted files and verify the corruption pattern
    let dest_bucket_dir = dest_dir.path().join("bucket");
    let mut corrupted_files = Vec::new();

    for entry in walkdir::WalkDir::new(&dest_bucket_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let rel_path = entry.path().strip_prefix(dest_dir.path()).unwrap();
        let src_file = archive_path.join(rel_path);

        let expected_size = std::fs::metadata(&src_file).unwrap().len() as usize;
        let actual_size = std::fs::metadata(entry.path()).unwrap().len() as usize;

        if actual_size != expected_size {
            // Verify corruption matches the pattern: N partial attempts + 1 full
            // Each partial attempt writes 25% of the file
            let partial_size = expected_size / 4;
            let expected_corrupted_size =
                (partial_size * fail_count) + expected_size;

            corrupted_files.push((
                rel_path.display().to_string(),
                expected_size,
                actual_size,
                expected_corrupted_size,
            ));
        }
    }

    // Assert that corruption occurred (this test documents the bug)
    assert!(
        !corrupted_files.is_empty(),
        "Expected to find corrupted files demonstrating the retry-append bug"
    );

    // Verify the corruption matches our expected pattern
    for (path, expected, actual, expected_corrupted) in &corrupted_files {
        assert!(
            *actual > *expected,
            "File {} should be larger than expected due to appended retry data \
             (expected {}, got {})",
            path,
            expected,
            actual
        );

        // Allow some tolerance for rounding in partial size calculation
        let tolerance = 10;
        assert!(
            (*actual as i64 - *expected_corrupted as i64).abs() <= tolerance,
            "File {} corruption should match pattern: {} partial bytes + {} full bytes = ~{}, \
             but got {} bytes",
            path,
            expected / 4 * fail_count,
            expected,
            expected_corrupted,
            actual
        );
    }

    println!(
        "Bug demonstrated: {} files corrupted by retry-append behavior",
        corrupted_files.len()
    );
    for (path, expected, actual, _) in &corrupted_files {
        println!("  {}: expected {} bytes, got {} bytes", path, expected, actual);
    }
}
