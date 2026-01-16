//! Shared test support utilities
//!
//! This module provides common helpers used across multiple test files:
//! - Test archive path and copy helpers
//! - HTTP server helpers (axum-based and raw TCP)
//! - Flaky server for testing retry/failure behavior
//! - HTTP retry test helpers for parameterized testing

#![allow(dead_code)] // Test utilities may not all be used in every test run

use crate::utils::{NON_STANDARD_RETRYABLE_HTTP_ERRORS, STANDARD_RETRYABLE_HTTP_ERRORS};
use axum::{routing::get_service, Router};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use walkdir::WalkDir;

//=============================================================================
// Test Archive Helpers
//=============================================================================

/// Get the path to the test archive
pub fn test_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

/// Copy the test archive to a destination directory
pub fn copy_test_archive(dst: &Path) -> Result<(), std::io::Error> {
    let src = test_archive_path();

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
pub fn get_files_by_pattern(archive_path: &Path, pattern: &str) -> Vec<PathBuf> {
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

//=============================================================================
// Axum HTTP Server Helpers
//=============================================================================

/// Start an HTTP server serving the specified archive path using axum/ServeDir
pub async fn start_http_server(archive_path: &Path) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(get_service(ServeDir::new(archive_path.to_path_buf())));
    start_http_server_with_app(app).await
}

/// Start an HTTP server with a custom axum Router
pub async fn start_http_server_with_app(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind to address");

    let addr = listener.local_addr().expect("Failed to get local address");
    let url = format!("http://{}", addr);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("HTTP server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (url, handle)
}

//=============================================================================
// Flaky Server
//=============================================================================

#[derive(Clone, Default)]
pub struct FlakyServerConfig {
    /// Number of times to fail with partial body before succeeding (per path)
    pub partial_body_fail_count: usize,
    /// Path prefix filter for partial body failures (e.g., "bucket/")
    pub partial_body_path_prefix: Option<String>,
    /// Archive path for serving real files
    pub archive_path: Option<PathBuf>,
    /// HTTP status code to return for error simulation (e.g., 500, 429, 408)
    pub error_status_code: Option<u16>,
    /// Number of times to return the error status code before succeeding (per path)
    pub error_status_fail_count: usize,
    /// Path prefix filter for status code errors
    pub error_status_path_prefix: Option<String>,
    /// Number of times to drop connection without response (simulates network failure)
    pub connection_drop_fail_count: usize,
    /// Path prefix filter for connection drops
    pub connection_drop_path_prefix: Option<String>,
}

impl FlakyServerConfig {
    /// Create a config for archive-based mid-stream failure tests
    pub fn archive_partial_body(archive_path: PathBuf, fail_count: usize) -> Self {
        Self {
            partial_body_fail_count: fail_count,
            partial_body_path_prefix: Some("bucket/".to_string()),
            archive_path: Some(archive_path),
            ..Default::default()
        }
    }

    /// Create a config for arbitrary HTTP status code error tests
    pub fn archive_status_error(
        archive_path: PathBuf,
        status_code: u16,
        fail_count: usize,
        path_prefix: &str,
    ) -> Self {
        Self {
            error_status_code: Some(status_code),
            error_status_fail_count: fail_count,
            error_status_path_prefix: Some(path_prefix.to_string()),
            archive_path: Some(archive_path),
            ..Default::default()
        }
    }

    /// Create a config for connection drop tests (simulates network failures)
    pub fn archive_connection_drop(
        archive_path: PathBuf,
        fail_count: usize,
        path_prefix: &str,
    ) -> Self {
        Self {
            connection_drop_fail_count: fail_count,
            connection_drop_path_prefix: Some(path_prefix.to_string()),
            archive_path: Some(archive_path),
            ..Default::default()
        }
    }
}

/// State for tracking requests to the flaky server
#[derive(Clone)]
pub struct RequestTracker {
    counts: Arc<Mutex<HashMap<String, usize>>>,
    timestamps: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
    /// Tracks files that have been successfully fetched (200 OK with full body)
    successful_fetches: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
            timestamps: Arc::new(Mutex::new(HashMap::new())),
            successful_fetches: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    pub fn increment(&self, path: &str) -> usize {
        let now = Instant::now();

        let mut counts = self.counts.lock().unwrap();
        let count = counts.entry(path.to_string()).or_insert(0);
        *count += 1;
        let result = *count;
        drop(counts);

        let mut timestamps = self.timestamps.lock().unwrap();
        timestamps.entry(path.to_string()).or_default().push(now);

        result
    }

    /// Record a successful fetch. Panics if this file was already successfully fetched.
    /// This ensures each file is only downloaded once.
    pub fn record_success(&self, path: &str) {
        let mut fetches = self.successful_fetches.lock().unwrap();
        if !fetches.insert(path.to_string()) {
            panic!(
                "File '{}' was successfully fetched more than once! \
                 This indicates a bug in deduplication or caching logic.",
                path
            );
        }
    }

    pub fn get_counts(&self) -> HashMap<String, usize> {
        self.counts.lock().unwrap().clone()
    }

    pub fn get_timestamps(&self, path: &str) -> Vec<Instant> {
        self.timestamps
            .lock()
            .unwrap()
            .get(path)
            .cloned()
            .unwrap_or_default()
    }

    /// Verify that retry delays follow exponential backoff pattern.
    /// Returns Ok if delays are within tolerance, Err with details otherwise.
    ///
    /// Expected backoff sequence: 100ms, 200ms, 400ms, 800ms, 1600ms, 3200ms, 5000ms (capped)
    /// Tolerance specifies the allowed deviation (e.g., 0.2 means delays must be within ±20% of expected).
    pub fn verify_backoff_timing(
        &self,
        path: &str,
        initial_backoff_ms: u64,
        tolerance: f64,
    ) -> Result<(), String> {
        let timestamps = self.get_timestamps(path);
        if timestamps.len() < 2 {
            return Ok(()); // No retries, nothing to verify
        }

        let mut expected_backoff_ms = initial_backoff_ms;
        for i in 1..timestamps.len() {
            let actual_delay = timestamps[i].duration_since(timestamps[i - 1]);
            let actual_ms = actual_delay.as_millis() as u64;
            let min_expected = (expected_backoff_ms as f64 * (1.0 - tolerance)) as u64;
            let max_expected = (expected_backoff_ms as f64 * (1.0 + tolerance)) as u64;

            if actual_ms < min_expected {
                return Err(format!(
                    "Retry {} -> {}: delay {}ms is below minimum {}ms (expected ~{}ms ±{}%)",
                    i,
                    i + 1,
                    actual_ms,
                    min_expected,
                    expected_backoff_ms,
                    (tolerance * 100.0) as u32
                ));
            }

            if actual_ms > max_expected {
                return Err(format!(
                    "Retry {} -> {}: delay {}ms exceeds maximum {}ms (expected ~{}ms ±{}%)",
                    i,
                    i + 1,
                    actual_ms,
                    max_expected,
                    expected_backoff_ms,
                    (tolerance * 100.0) as u32
                ));
            }

            // Advance expected backoff (doubles, capped at 5000ms)
            expected_backoff_ms = (expected_backoff_ms * 2).min(5000);
        }

        Ok(())
    }
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Start a flaky HTTP server with configurable failure modes
pub async fn start_flaky_server(
    config: FlakyServerConfig,
) -> (String, RequestTracker, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tracker = RequestTracker::new();
    let tracker_clone = tracker.clone();

    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };

            let cfg = config.clone();
            let tracker = tracker_clone.clone();

            tokio::spawn(async move {
                // Read HTTP request
                let mut buf = [0u8; 4096];
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);

                // Parse the request method and path from "GET /path HTTP/1.1"
                let first_line = request.lines().next().unwrap_or("");
                let mut parts = first_line.split_whitespace();
                let method = parts.next().unwrap_or("");
                let path = parts
                    .next()
                    .map(|p| p.trim_start_matches('/'))
                    .unwrap_or("");
                let is_head = method == "HEAD";

                let count = tracker.increment(path);

                // Determine the content to serve
                let content = if let Some(ref archive_path) = cfg.archive_path {
                    let file_path = archive_path.join(path);
                    match std::fs::read(&file_path) {
                        Ok(c) => c,
                        Err(_) => {
                            // 404 Not Found
                            let response = "HTTP/1.1 404 Not Found\r\n\
                                           Content-Length: 0\r\n\
                                           Connection: close\r\n\r\n";
                            let _ = socket.write_all(response.as_bytes()).await;
                            return;
                        }
                    }
                } else {
                    Vec::new()
                };

                // Check if we should drop the connection (simulates network failure)
                let should_drop = cfg.connection_drop_fail_count > 0
                    && count <= cfg.connection_drop_fail_count
                    && cfg
                        .connection_drop_path_prefix
                        .as_ref()
                        .map_or(true, |prefix| path.starts_with(prefix));

                if should_drop {
                    // Just drop the socket without sending any response
                    drop(socket);
                    return;
                }

                // Check if we should return a custom error status code
                let should_status_error = cfg.error_status_code.is_some()
                    && count <= cfg.error_status_fail_count
                    && cfg
                        .error_status_path_prefix
                        .as_ref()
                        .map_or(true, |prefix| path.starts_with(prefix));

                if should_status_error {
                    let status = cfg.error_status_code.unwrap();
                    let reason = match status {
                        400 => "Bad Request",
                        403 => "Forbidden",
                        404 => "Not Found",
                        408 => "Request Timeout",
                        429 => "Too Many Requests",
                        500 => "Internal Server Error",
                        502 => "Bad Gateway",
                        503 => "Service Unavailable",
                        504 => "Gateway Timeout",
                        _ => "Error",
                    };
                    let response = format!(
                        "HTTP/1.1 {} {}\r\n\
                         Content-Length: 0\r\n\
                         Connection: close\r\n\r\n",
                        status, reason
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    return;
                }

                // Check if we should fail mid-stream
                let should_partial_fail = cfg.partial_body_fail_count > 0
                    && count <= cfg.partial_body_fail_count
                    && cfg
                        .partial_body_path_prefix
                        .as_ref()
                        .map_or(true, |prefix| path.starts_with(prefix));

                if should_partial_fail {
                    // Send 200 OK with full Content-Length, but only partial body
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n\
                         Content-Type: application/octet-stream\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\r\n",
                        content.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;

                    // Send only partial data (25%), then close abruptly
                    let partial_len = content.len() / 4;
                    let partial_len = partial_len.max(1).min(content.len());
                    let _ = socket.write_all(&content[..partial_len]).await;
                    drop(socket);
                    return;
                }

                // Normal success response
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/octet-stream\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\r\n",
                    content.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                // HEAD requests get headers only, no body
                if !is_head {
                    let _ = socket.write_all(&content).await;
                    // Record successful GET fetch - panics if same file fetched twice
                    tracker.record_success(path);
                }
            });
        }
    });

    // Give the server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    (format!("http://{}", addr), tracker, handle)
}

/// Returns all transient HTTP errors that should trigger retry behavior.
///
/// This combines:
/// - Standard HTTP errors (500, 502, 503, 504) - handled by OpenDAL natively
/// - Non-standard errors (429, Cloudflare, proxy, etc.) - handled by our RetryableErrorLayer
pub fn transient_http_errors() -> Vec<(u16, &'static str)> {
    STANDARD_RETRYABLE_HTTP_ERRORS
        .iter()
        .chain(NON_STANDARD_RETRYABLE_HTTP_ERRORS.iter())
        .copied()
        .collect()
}
