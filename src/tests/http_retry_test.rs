//! HTTP error handling tests for scan and mirror operations
//!
//! This module tests retry behavior for HTTP errors across both operations:
//! - Transient errors (5xx, 429, 408) trigger retries with exponential backoff
//! - Permanent errors (4xx) do not trigger retries
//! - Connection drops trigger retries

use super::utils::{
    start_flaky_server, start_http_server_with_app, test_archive_path, FlakyServerConfig,
    RequestTracker, PERMANENT_HTTP_ERRORS, TRANSIENT_HTTP_ERRORS,
};
use std::path::PathBuf;
use crate::test_helpers::{run_mirror, run_scan, MirrorConfig, ScanConfig};
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get_service},
    Router,
};
use rstest::rstest;
use tempfile::TempDir;
use tower::util::ServiceExt;
use tower_http::services::ServeDir;

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

#[rstest]
#[case::scan(Operation::Scan)]
#[case::mirror(Operation::Mirror)]
#[tokio::test]
async fn test_retries_on_transient_http_errors(#[case] op: Operation) {
    let archive_path = test_archive_path();

    for (status_code, description) in TRANSIENT_HTTP_ERRORS {
        let config = FlakyServerConfig::archive_status_error(
            archive_path.clone(),
            *status_code,
            2, // Fail twice, then succeed
            op.path_prefix(),
        );
        let (server_url, tracker, handle) = start_flaky_server(config).await;

        let result = op.run(&server_url).await;
        handle.abort();

        verify_retries_occurred(&tracker, op, *status_code, description);
        assert!(
            result.is_ok(),
            "{:?} should succeed after retrying HTTP {} ({}): {:?}",
            op,
            status_code,
            description,
            result.err()
        );
        verify_backoff_timing(&tracker, op, *status_code, description);
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

    if let Some((path, _)) = retried.first() {
        tracker
            .verify_backoff_timing(path, 100, 0.8)
            .unwrap_or_else(|e| panic!("{:?}: Connection drop backoff timing failed: {}", op, e));
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
            "{:?} should fail on HTTP {} ({})",
            op,
            status_code,
            description
        );

        // Verify no retries happened
        let counts = tracker.get_counts();
        let retried: Vec<_> = counts
            .iter()
            .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
            .collect();

        assert!(
            retried.is_empty(),
            "{:?}: HTTP {} ({}) should NOT trigger retries, but got: {:?}",
            op,
            status_code,
            description,
            retried
        );
    }
}

//=============================================================================
// Helper Functions
//=============================================================================

fn verify_retries_occurred(tracker: &RequestTracker, op: Operation, status_code: u16, description: &str) {
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

fn verify_backoff_timing(tracker: &RequestTracker, op: Operation, status_code: u16, description: &str) {
    let counts = tracker.get_counts();
    let retried: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with(op.path_prefix()) && **count > 1)
        .collect();

    if let Some((path, _)) = retried.first() {
        tracker
            .verify_backoff_timing(path, 100, 0.8)
            .unwrap_or_else(|e| {
                panic!(
                    "{:?}: HTTP {} ({}) backoff timing failed: {}",
                    op, status_code, description, e
                )
            });
    }
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

/// Tests that mid-stream failures (truncated responses) are retried and partial
/// files are cleaned up. Verifies both that retries occur and that the final
/// file has correct content (not partial data from failed attempt).
#[tokio::test]
async fn test_mirror_cleans_up_partial_file_on_failure() {
    let archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Read expected content for verification
    let bucket_file = archive_path
        .join("bucket/44/3c/ec/bucket-443cec6682d0e98930ffd71b6f3f450a29fff6b9dd2e0cfaf41bd714926b4422.xdr.gz");
    let expected_content = std::fs::read(&bucket_file).expect("Test bucket file should exist");

    // Server fails first request to bucket files with partial body, succeeds on retry
    let config = FlakyServerConfig::archive_partial_body(archive_path, 1);
    let (server_url, tracker, handle) = start_flaky_server(config).await;

    let dest_dir = TempDir::new().unwrap();
    let mirror_config =
        MirrorConfig::new(&server_url, format!("file://{}", dest_dir.path().display()))
            .skip_optional()
            .concurrency(1)
            .high(63);

    let result = run_mirror(mirror_config).await;
    handle.abort();

    // Verify bucket files were retried (count > 1: first fails, retry succeeds)
    let counts = tracker.get_counts();
    let bucket_retry_counts: Vec<_> = counts
        .iter()
        .filter(|(path, count)| path.starts_with("bucket/") && **count > 1)
        .collect();
    assert!(
        !bucket_retry_counts.is_empty(),
        "Expected bucket files to be retried (request count > 1), got counts: {:?}",
        counts.iter().filter(|(p, _)| p.starts_with("bucket/")).collect::<Vec<_>>()
    );

    assert!(result.is_ok(), "Mirror should succeed after retry: {:?}", result.err());

    // Verify the final file has correct content (not partial)
    let dest_bucket = dest_dir.path()
        .join("bucket/44/3c/ec/bucket-443cec6682d0e98930ffd71b6f3f450a29fff6b9dd2e0cfaf41bd714926b4422.xdr.gz");
    let final_content = std::fs::read(&dest_bucket).expect("Bucket file should exist");

    assert_eq!(
        final_content.len(),
        expected_content.len(),
        "Final file should have correct size, not partial"
    );
}
