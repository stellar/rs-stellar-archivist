//! Tests for command-line interface validation and error handling

use crate::test_helpers::{run_mirror as cmd_mirror_run, MirrorConfig};
use rstest::rstest;

#[rstest]
#[case::http("http://example.com/archive", "HTTP destinations are read-only")]
#[case::https("https://example.com/archive", "HTTPS destinations are read-only")]
#[case::s3("s3://my-bucket/archive", "S3 destinations are not currently supported for writing")]
#[tokio::test]
async fn test_mirror_rejects_readonly_destination(#[case] dst: &str, #[case] _reason: &str) {
    let config = MirrorConfig::new("file:///tmp/test-source", dst);
    let result = cmd_mirror_run(config).await;
    assert!(result.is_err());
}

#[rstest]
#[case::not_a_url("not-a-url")]
#[case::missing_scheme("://missing-scheme")]
#[case::missing_slash("file:/missing-slash")]
#[case::missing_colon("http//missing-colon")]
#[case::empty("")]
#[tokio::test]
async fn test_mirror_rejects_malformed_url(
    #[case] bad_url: &str,
    #[values("source", "destination")] position: &str,
) {
    let config = match position {
        "source" => MirrorConfig::new(bad_url, "file:///tmp/test-dest"),
        "destination" => MirrorConfig::new("file:///tmp/test-source", bad_url),
        _ => unreachable!(),
    };
    let result = cmd_mirror_run(config).await;
    assert!(
        result.is_err(),
        "Should reject malformed {} URL: '{}'",
        position,
        bad_url
    );
}

#[rstest]
#[case::scan("scan")]
#[case::mirror("mirror")]
#[tokio::test]
async fn test_rejects_nonexistent_source(#[case] operation: &str) {
    use crate::test_helpers::{run_scan, ScanConfig};

    let src = "file:///this/path/does/not/exist/at/all";

    let result = match operation {
        "scan" => run_scan(ScanConfig::new(src)).await,
        "mirror" => cmd_mirror_run(MirrorConfig::new(src, "file:///tmp/test-dest")).await,
        _ => unreachable!(),
    };

    assert!(result.is_err(), "{} should reject nonexistent source", operation);
}

#[tokio::test]
async fn test_mirror_creates_destination_if_not_exists() {
    use super::utils::test_archive_path;
    use tempfile::TempDir;

    // Create a temp directory, then use a subdirectory that doesn't exist yet
    let temp_base = TempDir::new().expect("Failed to create temp base");
    let dest_path = temp_base.path().join("new-dest-directory");

    let src = format!("file://{}", test_archive_path().display());
    let dst = format!("file://{}", dest_path.display());
    let config = MirrorConfig::new(&src, &dst).skip_optional().high(63);

    let result = cmd_mirror_run(config).await;
    assert!(
        result.is_ok(),
        "Should create destination directory if it doesn't exist, got error: {:?}",
        result
    );

    assert!(dest_path.exists(), "Destination directory should exist");
    assert!(
        dest_path.join(".well-known/stellar-history.json").exists(),
        "HAS file should exist in destination"
    );
}

#[rstest]
#[case::scan("scan")]
#[case::mirror("mirror")]
#[tokio::test]
async fn test_rejects_low_greater_than_high(#[case] operation: &str) {
    use super::utils::test_archive_path;
    use crate::test_helpers::{run_scan, ScanConfig};
    use tempfile::TempDir;

    let src = format!("file://{}", test_archive_path().display());

    let result = match operation {
        "scan" => run_scan(ScanConfig::new(&src).low(2000).high(1000)).await,
        "mirror" => {
            let temp_dir = TempDir::new().unwrap();
            let dst = format!("file://{}", temp_dir.path().join("dest").display());
            cmd_mirror_run(MirrorConfig::new(&src, &dst).low(2000).high(1000)).await
        }
        _ => unreachable!(),
    };

    assert!(result.is_err(), "{} should reject when low > high", operation);

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("low checkpoint") && err_msg.contains("is greater than high checkpoint"),
        "Error should indicate low > high, got: {}",
        err_msg
    );
}
