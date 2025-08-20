//! Test HTTP via local server

use axum::{routing::get_service, Router};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use stellar_archivist::test_helpers::{run_mirror, run_scan, MirrorConfig, ScanConfig};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

/// Start an HTTP server serving the specified archive path
async fn start_test_http_server(
    archive_path: &std::path::Path,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(get_service(ServeDir::new(archive_path.to_path_buf())));
    start_http_server_with_app(app).await
}

async fn start_http_server_with_app(app: Router) -> (String, tokio::task::JoinHandle<()>) {
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

// Helper function to copy only partial archive (missing some ledger files)
fn copy_partial_archive(src: &std::path::Path, dst: &std::path::Path) {
    use std::fs;
    use walkdir::WalkDir;

    let mut skip_count = 0;

    for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dst_path).ok();
        } else if entry.file_type().is_file() {
            // Skip every 3rd ledger file to create gaps
            if src_path.to_string_lossy().contains("/ledger/") && skip_count % 3 == 0 {
                skip_count += 1;
                continue; // Skip this file
            }
            skip_count += 1;

            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).ok();
            }
            fs::copy(src_path, dst_path).ok();
        }
    }
}

// Helper function to copy archive but omit some files to cause download failures
fn copy_archive_with_unreadable_files(src: &std::path::Path, dst: &std::path::Path) {
    use std::fs;
    use walkdir::WalkDir;

    let mut file_count = 0;

    for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dst_path).ok();
        } else if entry.file_type().is_file() {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).ok();
            }

            // Skip specific files to cause 404 errors
            // IMPORTANT: Never skip the highest history file (000000ff) as it's needed for HAS generation
            let path_str = src_path.to_string_lossy();
            if !path_str.contains("history-000000ff.json") // Don't skip the highest checkpoint's history
                && !path_str.contains("stellar-history.json")
                && (path_str.contains("transactions-000000bf.xdr.gz")
                    || (path_str.contains("bucket-") && file_count % 15 == 0))
            {
                // Don't copy these files - they'll 404 when mirror tries to download
                file_count += 1;
                continue;
            }

            fs::copy(src_path, &dst_path).ok();
            file_count += 1;
        }
    }
}

#[tokio::test]
async fn test_scan_http_archive() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    let (server_url, server_handle) = start_test_http_server(&test_archive_path).await;

    // Scan the archive via HTTP
    let scan_config = ScanConfig {
        archive: server_url.clone(),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => (),
        Err(e) => {
            server_handle.abort();
            panic!("HTTP scan failed: {}", e);
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_mirror_http_to_filesystem() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");
    let (server_url, server_handle) = start_test_http_server(&test_archive_path).await;

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
    };

    match run_mirror(mirror_config).await {
        Ok(_) => (),
        Err(e) => {
            server_handle.abort();
            panic!("HTTP mirror failed: {}", e);
        }
    }

    // Verify the mirrored archive
    let scan_config = ScanConfig {
        archive: format!("file://{}", mirror_dest),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => (),
        Err(e) => {
            server_handle.abort();
            panic!("Scan of mirrored archive failed: {}", e);
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_http_server_scan_with_missing_files() {
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();
    copy_partial_archive(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("testnet-archive-small"),
        archive_path,
    );

    let (server_url, server_handle) = start_test_http_server(archive_path).await;

    // Try to scan the partial archive via HTTP
    let scan_config = ScanConfig {
        archive: server_url.clone(),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: None,
    };

    match run_scan(scan_config).await {
        Ok(_) => {
            server_handle.abort();
            panic!("Scan should have failed due to missing files");
        }
        Err(_e) => {}
    }

    server_handle.abort();
}

// Test that HAS file is written even when some files fail to download
#[tokio::test]
async fn test_mirror_writes_has_despite_failures() {
    // Create a temp archive with some files that will fail to serve
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();
    copy_archive_with_unreadable_files(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("testnet-archive-small"),
        archive_path,
    );

    let (server_url, server_handle) = start_test_http_server(archive_path).await;

    let temp_dest = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dest.path().to_str().unwrap();

    // Mirror from HTTP to filesystem (should have some failures)
    let mirror_config = MirrorConfig {
        src: server_url.clone(),
        dst: format!("file://{}", mirror_dest),
        concurrency: 4,
        high: Some(255),
        low: None,
        skip_optional: true,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    // Mirror should fail due to unreadable files
    match run_mirror(mirror_config).await {
        Ok(_) => {
            server_handle.abort();
            panic!("Mirror should have reported failures");
        }
        Err(_e) => {}
    }

    // .well-known file should still exist despite failures
    let well_known_path =
        std::path::Path::new(mirror_dest).join(".well-known/stellar-history.json");
    assert!(well_known_path.exists());
    let well_known_content = std::fs::read_to_string(&well_known_path).unwrap();
    let well_known: serde_json::Value = serde_json::from_str(&well_known_content).unwrap();
    assert_eq!(well_known["currentLedger"], serde_json::json!(255));

    server_handle.abort();
}

#[tokio::test]
async fn test_mirror_race_condition_with_advancing_archive() {
    let source_archive = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Create a temp archive that simulates a live archive that advances during mirror
    let temp_archive = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_archive.path();

    use walkdir::WalkDir;
    for entry in WalkDir::new(&source_archive)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(&source_archive).unwrap();
        let dst_path = archive_path.join(relative);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path).ok();
        } else if entry.file_type().is_file() {
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::copy(src_path, &dst_path).ok();
        }
    }

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

    // Start HTTP server with special handler that advances .well-known after initial reads
    let should_advance = Arc::new(AtomicBool::new(false));
    let app = Router::new()
        .route(
            "/.well-known/stellar-history.json",
            axum::routing::get(move || {
                let should_advance = should_advance.clone();
                let well_known_content = if should_advance.load(Ordering::Relaxed) {
                    // Return advanced .well-known
                    advanced_well_known.to_string()
                } else {
                    // First few reads get initial .well-known, then we advance
                    should_advance.store(true, Ordering::Relaxed);
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
    };

    match run_mirror(mirror_config).await {
        Ok(_) => {}
        Err(e) => {
            server_handle.abort();
            panic!("Mirror failed: {}", e);
        }
    }

    // Check what .well-known was written with the original destination .well-known file (127)
    let dest_well_known_path =
        std::path::Path::new(mirror_dest).join(".well-known/stellar-history.json");
    let dest_well_known_content = std::fs::read_to_string(&dest_well_known_path)
        .expect("Failed to read destination .well-known");
    let dest_well_known: serde_json::Value = serde_json::from_str(&dest_well_known_content)
        .expect("Failed to parse destination .well-known");

    let dest_well_known_ledger = dest_well_known["currentLedger"].as_u64().unwrap() as u32;
    assert_eq!(dest_well_known_ledger, 127);

    // Verify that none of the checkpoint 191 files were downloaded
    let checkpoint_bf_history =
        std::path::Path::new(mirror_dest).join("history/00/00/00/history-000000bf.json");
    let checkpoint_bf_ledger =
        std::path::Path::new(mirror_dest).join("ledger/00/00/00/ledger-000000bf.xdr.gz");
    let checkpoint_bf_tx = std::path::Path::new(mirror_dest)
        .join("transactions/00/00/00/transactions-000000bf.xdr.gz");
    let checkpoint_bf_results =
        std::path::Path::new(mirror_dest).join("results/00/00/00/results-000000bf.xdr.gz");
    let checkpoint_bf_scp =
        std::path::Path::new(mirror_dest).join("scp/00/00/00/scp-000000bf.xdr.gz");

    assert!(
        !checkpoint_bf_history.exists(),
        "history-000000bf.json should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_ledger.exists(),
        "ledger-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_tx.exists(),
        "transactions-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_results.exists(),
        "results-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );
    assert!(
        !checkpoint_bf_scp.exists(),
        "scp-000000bf.xdr.gz should not exist since we only mirrored up to 127"
    );

    // Verify that checkpoint 127 files were mirrored
    let checkpoint_7f_history =
        std::path::Path::new(mirror_dest).join("history/00/00/00/history-0000007f.json");
    let checkpoint_7f_ledger =
        std::path::Path::new(mirror_dest).join("ledger/00/00/00/ledger-0000007f.xdr.gz");

    assert!(
        checkpoint_7f_history.exists(),
        "history-0000007f.json should exist as it was within our mirror range"
    );
    assert!(
        checkpoint_7f_ledger.exists(),
        "ledger-0000007f.xdr.gz should exist as it was within our mirror range"
    );

    server_handle.abort();
}

// Helper function to test HTTP/HTTPS client connections
async fn test_client_connection(url_str: &str, description: &str) {
    use stellar_archivist::storage::{HttpRetryConfig, HttpStore, Storage};

    let retry_config = HttpRetryConfig {
        max_retries: 3,
        initial_backoff_ms: 100,
    };

    let url = reqwest::Url::parse(url_str).unwrap();
    let store = HttpStore::new(url, retry_config);

    match store.open_reader(".well-known/stellar-history.json").await {
        Ok(mut reader) => {
            // Read some data to ensure the connection works
            let mut buffer = vec![0u8; 100];
            match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                Ok(n) if n > 0 => {}
                Ok(_) => {
                    panic!("{} connected but no data received", description);
                }
                Err(e) => {
                    panic!("{} failed to read data: {}", description, e);
                }
            }
        }
        Err(e) => {
            panic!("{} failed to connect: {}", description, e);
        }
    }
}

// Smoke test against actual HTTP/HTTPS URLs
#[tokio::test]
async fn test_http_and_https_client_creation() {
    // Test with HTTP URL without trailing slash
    test_client_connection(
        "http://history.stellar.org/prd/core-live/core_live_001",
        "HTTP client (no trailing slash)",
    )
    .await;

    // Test with HTTPS URL with trailing slash
    test_client_connection(
        "https://history.stellar.org/prd/core-live/core_live_001/",
        "HTTPS client (with trailing slash)",
    )
    .await;
}
