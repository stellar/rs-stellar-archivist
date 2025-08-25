//! Tests for HTTP retry logic and error handling

use crate::storage::{HttpRetryConfig, HttpStore, Storage};
use tokio::io::AsyncReadExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Default retry config for tests
fn test_retry_config() -> HttpRetryConfig {
    HttpRetryConfig {
        max_retries: 3,
        initial_backoff_ms: 100,
    }
}

/// Test that transient errors (500, 502, 503, 504) are retried
#[tokio::test]
async fn test_retry_on_server_errors() {
    let mock_server = MockServer::start().await;

    // Set up sequence of responses - fail twice then succeed
    // We'll use multiple mocks that get consumed in order
    Mock::given(method("GET"))
        .and(path("/test-file"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/test-file"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/test-file"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"test content"))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    // Should succeed after retries
    let mut reader = store.open_reader("test-file").await.unwrap();
    let mut content = String::new();
    reader.read_to_string(&mut content).await.unwrap();

    assert_eq!(content, "test content");
}

/// Test that 429 (Too Many Requests) triggers retry with backoff
#[tokio::test]
async fn test_retry_on_rate_limit() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/rate-limited"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/rate-limited"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"success after rate limit"))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let start = std::time::Instant::now();
    let mut reader = store.open_reader("rate-limited").await.unwrap();
    let duration = start.elapsed();

    // Should have backed off (at least 300ms for two retries with exponential backoff)
    assert!(
        duration.as_millis() >= 300,
        "Expected backoff delay, got {}ms",
        duration.as_millis()
    );

    let mut content = String::new();
    reader.read_to_string(&mut content).await.unwrap();
    assert_eq!(content, "success after rate limit");
}

/// Test that 404 (Not Found) does not retry
#[tokio::test]
async fn test_no_retry_on_not_found() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/missing"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1) // Should be called exactly once
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    // Should fail immediately without retries
    let result = store.open_reader("missing").await;
    assert!(result.is_err());
    let error = result.err().unwrap();
    assert!(error.to_string().contains("HTTP 404"));
}

/// Test that 403 (Forbidden) does not retry
#[tokio::test]
async fn test_no_retry_on_forbidden() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/forbidden"))
        .respond_with(ResponseTemplate::new(403))
        .expect(1) // Should be called exactly once
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let result = store.open_reader("forbidden").await;
    assert!(result.is_err());
    let error = result.err().unwrap();
    assert!(error.to_string().contains("HTTP 403"));
}

/// Test max retries limit
#[tokio::test]
async fn test_max_retries_exceeded() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/always-fails"))
        .respond_with(ResponseTemplate::new(500))
        .expect(4) // Initial + 3 retries
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let result = store.open_reader("always-fails").await;
    assert!(result.is_err());
    let error_msg = result.err().unwrap().to_string();
    assert!(error_msg.contains("HTTP 500") || error_msg.contains("after 3 retries"));
}

/// Test exists() with successful HEAD request
#[tokio::test]
async fn test_exists_with_head_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/existing-file"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let exists = store.exists("existing-file").await.unwrap();
    assert!(exists);
}

/// Test exists() returns false for 404
#[tokio::test]
async fn test_exists_returns_false_for_not_found() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/not-found"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let exists = store.exists("not-found").await.unwrap();
    assert!(!exists);
}

/// Test exists() retries on server errors
#[tokio::test]
async fn test_exists_retries_on_server_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(path("/flaky-exists"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    Mock::given(method("HEAD"))
        .and(path("/flaky-exists"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let exists = store.exists("flaky-exists").await.unwrap();
    assert!(exists);
}

/// Test exists() retries and eventually errors
#[tokio::test]
async fn test_exists_error_on_unexpected_status() {
    let mock_server = MockServer::start().await;

    // Return 401 Unauthorized - this should retry then error after max retries
    Mock::given(method("HEAD"))
        .and(path("/unauthorized"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let result = store.exists("unauthorized").await;
    assert!(result.is_err());
    let error_msg = result.err().unwrap().to_string();
    assert!(error_msg.contains("Failed to check existence"));
    assert!(error_msg.contains("401"));
}

/// Test network/connection errors trigger retry
#[tokio::test]
async fn test_retry_on_connection_error() {
    // Use a non-existent port to trigger connection error
    let store = HttpStore::new("http://127.0.0.1:1".parse().unwrap(), test_retry_config());

    let result = store.open_reader("test").await;
    assert!(result.is_err());

    // Error message should indicate retries were attempted
    let error_msg = result.err().unwrap().to_string();
    assert!(error_msg.contains("after 3 retries") || error_msg.contains("Failed to fetch"));
}

/// Test 408 Request Timeout triggers retry
#[tokio::test]
async fn test_retry_on_request_timeout() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/timeout"))
        .respond_with(ResponseTemplate::new(408))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/timeout"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"success after timeout"))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let mut reader = store.open_reader("timeout").await.unwrap();
    let mut content = String::new();
    reader.read_to_string(&mut content).await.unwrap();

    assert_eq!(content, "success after timeout");
}

/// Test 502 Bad Gateway triggers retry
#[tokio::test]
async fn test_retry_on_bad_gateway() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/bad-gateway"))
        .respond_with(ResponseTemplate::new(502))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/bad-gateway"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"success after bad gateway"))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let mut reader = store.open_reader("bad-gateway").await.unwrap();
    let mut content = String::new();
    reader.read_to_string(&mut content).await.unwrap();

    assert_eq!(content, "success after bad gateway");
}

/// Test 504 Gateway Timeout triggers retry
#[tokio::test]
async fn test_retry_on_gateway_timeout() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/gateway-timeout"))
        .respond_with(ResponseTemplate::new(504))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/gateway-timeout"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"success after gateway timeout"))
        .mount(&mock_server)
        .await;

    let store = HttpStore::new(mock_server.uri().parse().unwrap(), test_retry_config());

    let mut reader = store.open_reader("gateway-timeout").await.unwrap();
    let mut content = String::new();
    reader.read_to_string(&mut content).await.unwrap();

    assert_eq!(content, "success after gateway timeout");
}
