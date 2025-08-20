//! Tests for command-line interface validation and error handling

use stellar_archivist::test_helpers::{run_mirror as cmd_mirror_run, MirrorConfig};

#[tokio::test]
async fn test_mirror_rejects_http_destination() {
    // HTTP destinations are read-only
    let config = MirrorConfig {
        src: "file:///tmp/test-source".to_string(),
        dst: "http://example.com/archive".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    let result = cmd_mirror_run(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mirror_rejects_https_destination() {
    // HTTPS destinations are also read-only
    let config = MirrorConfig {
        src: "file:///tmp/test-source".to_string(),
        dst: "https://example.com/archive".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    let result = cmd_mirror_run(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mirror_rejects_s3_destination() {
    // S3 destinations are not currently supported for writing
    let config = MirrorConfig {
        src: "file:///tmp/test-source".to_string(),
        dst: "s3://my-bucket/archive".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    let result = cmd_mirror_run(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mirror_rejects_malformed_source_url() {
    // Test various malformed source URLs
    let test_cases = vec![
        "not-a-url",
        "://missing-scheme",
        "file:/missing-slash",
        "http//missing-colon",
        "",
    ];

    for bad_src in test_cases {
        let config = MirrorConfig {
            src: bad_src.to_string(),
            dst: "file:///tmp/test-dest".to_string(),
            concurrency: 4,
            skip_optional: false,
            high: None,
            low: None,
            overwrite: false,
            allow_mirror_gaps: false,
        };

        let result = cmd_mirror_run(config).await;
        assert!(
            result.is_err(),
            "Should reject malformed source URL: '{}'",
            bad_src
        );
    }
}

#[tokio::test]
async fn test_mirror_rejects_malformed_destination_url() {
    // Test various malformed destination URLs
    let test_cases = vec!["not-a-url", "://missing-scheme", "file:/missing-slash", ""];

    for bad_dst in test_cases {
        let config = MirrorConfig {
            src: "file:///tmp/test-source".to_string(),
            dst: bad_dst.to_string(),
            concurrency: 4,
            skip_optional: false,
            high: None,
            low: None,
            overwrite: false,
            allow_mirror_gaps: false,
        };

        let result = cmd_mirror_run(config).await;
        assert!(
            result.is_err(),
            "Should reject malformed destination URL: '{}'",
            bad_dst
        );
    }
}

#[tokio::test]
async fn test_mirror_rejects_nonexistent_source() {
    // Source that doesn't exist should fail gracefully
    let config = MirrorConfig {
        src: "file:///this/path/does/not/exist/at/all".to_string(),
        dst: "file:///tmp/test-dest".to_string(),
        concurrency: 4,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    let result = cmd_mirror_run(config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_scan_rejects_nonexistent_source() {
    use stellar_archivist::test_helpers::{run_scan as cmd_scan_run, ScanConfig};

    // Source that doesn't exist should fail with a clear error
    let config = ScanConfig {
        archive: "file:///this/path/does/not/exist/at/all".to_string(),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
    };

    let result = cmd_scan_run(config).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Source path does not exist"),
        "Error should clearly indicate the source doesn't exist, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_mirror_creates_destination_if_not_exists() {
    use std::path::PathBuf;
    use tempfile::TempDir;

    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Create a temp directory, then use a subdirectory that doesn't exist yet
    let temp_base = TempDir::new().expect("Failed to create temp base");
    let dest_path = temp_base.path().join("new-dest-directory");

    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: true,
        high: Some(63),
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    let result = cmd_mirror_run(config).await;
    assert!(
        result.is_ok(),
        "Should create destination directory if it doesn't exist, got error: {:?}",
        result
    );

    // Verify the destination was created
    assert!(
        dest_path.exists(),
        "Destination directory should have been created"
    );
    assert!(
        dest_path.join(".well-known/stellar-history.json").exists(),
        "HAS file should exist in destination"
    );
}

#[tokio::test]
async fn test_scan_rejects_low_greater_than_high() {
    use std::path::PathBuf;
    use stellar_archivist::test_helpers::{run_scan as cmd_scan_run, ScanConfig};

    // Create a valid test archive path
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    let config = ScanConfig {
        archive: format!("file://{}", test_archive_path.display()),
        concurrency: 4,
        skip_optional: false,
        low: Some(2000),
        high: Some(1000), // Lower value - should be rejected
    };

    let result = cmd_scan_run(config).await;
    assert!(
        result.is_err(),
        "Should reject when --low is greater than --high"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("low checkpoint") && err_msg.contains("is greater than high checkpoint"),
        "Error should indicate that low is greater than high, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_mirror_rejects_low_greater_than_high() {
    use std::path::PathBuf;
    use stellar_archivist::test_helpers::{run_mirror as cmd_mirror_run, MirrorConfig};
    use tempfile::TempDir;

    // Create a valid test archive path
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    // Create a temporary destination
    let temp_dir = TempDir::new().unwrap();
    let dest_path = temp_dir.path().join("test_dest");

    let config = MirrorConfig {
        src: format!("file://{}", test_archive_path.display()),
        dst: format!("file://{}", dest_path.display()),
        concurrency: 4,
        skip_optional: false,
        low: Some(2000),
        high: Some(1000), // Lower value - should be rejected
        overwrite: false,
        allow_mirror_gaps: false,
    };

    let result = cmd_mirror_run(config).await;
    assert!(
        result.is_err(),
        "Should reject when --low is greater than --high"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("low checkpoint") && err_msg.contains("is greater than high checkpoint"),
        "Error should indicate that low is greater than high, got: {}",
        err_msg
    );
}
