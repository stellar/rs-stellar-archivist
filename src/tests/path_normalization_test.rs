//! Tests for path normalization and file URL handling

use super::utils::file_url_from_path;
use crate::storage::{from_url_with_config, OpendalStore, Storage};
use crate::test_helpers::test_storage_config;
use normalize_path::NormalizePath;
use tempfile::TempDir;

#[test]
fn test_filesystem_root_path_is_normalized() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = temp_dir.path().join("subdir").join("..");

    let store = OpendalStore::filesystem(&root, &test_storage_config())
        .expect("Failed to create filesystem store");
    let base = store
        .get_base_path()
        .expect("Expected filesystem store to expose base path");

    assert_eq!(base, root.normalize());
}

#[tokio::test]
async fn test_file_url_round_trip_base_path() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let url = file_url_from_path(temp_dir.path());

    let store = from_url_with_config(&url, &test_storage_config())
        .await
        .expect("Failed to create store from file URL");
    let base = store
        .get_base_path()
        .expect("Expected filesystem store to expose base path");

    assert_eq!(base, temp_dir.path());
}

#[cfg(windows)]
#[tokio::test]
async fn test_file_url_backslashes_are_accepted() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let raw = format!("file://{}", temp_dir.path().display());

    let store = from_url_with_config(&raw, &test_storage_config())
        .await
        .expect("Failed to create store from backslash file URL");
    let base = store
        .get_base_path()
        .expect("Expected filesystem store to expose base path");

    assert_eq!(base, temp_dir.path());
}
