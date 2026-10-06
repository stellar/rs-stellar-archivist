//! Tests for writable object-store (S3/GCS/Azure/B2) destinations.
//!
//! Most tests use `mock_object_store` (see `tests::utils`) — a local-fs-backed
//! store that presents exactly what a cloud object store presents to the rest
//! of the code: writable, atomic commit-on-close, and NO filesystem base path.
//! The backing directory doubles as an inspection window: after operations
//! complete, the "bucket" contents can be checked as plain files.

use super::utils::{
    delete_first_file, file_url_from_path, mock_object_store, testnet_small_archive_path,
};
use crate::history_format::ROOT_WELL_KNOWN_PATH;
use crate::mirror_operation::MirrorOperation;
use crate::pipeline::{Pipeline, PipelineConfig};
use crate::repair_operation::RepairOperation;
use crate::storage::{OpendalStore, Storage, StorageRef};
use crate::test_helpers::{run_mirror, run_scan, test_storage_config, MirrorConfig, ScanConfig};
use crate::utils::{stamp_network_passphrase, update_well_known_from_history};
use crate::verify::verify_and_write_bucket;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use walkdir::WalkDir;

#[tokio::test]
async fn mock_object_store_is_writable_with_no_base_path() {
    let dir = TempDir::new().unwrap();
    let store = mock_object_store(dir.path());
    assert!(store.supports_writes());
    assert!(store.get_base_path().is_none());
}

/// Gzip `content` into a valid bucket file: the returned archive path embeds
/// the sha256 of the *decompressed* content, as real bucket files do.
fn make_bucket(content: &[u8]) -> (String, Vec<u8>) {
    let hash = hex::encode(Sha256::digest(content));
    let path = crate::history_format::bucket_path(&hash).unwrap();
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(content).unwrap();
    (path, enc.finish().unwrap())
}

/// Put `bytes` at `path` inside `dir` so a filesystem source store can serve it.
fn plant_file(dir: &Path, path: &str, bytes: &[u8]) {
    let full = dir.join(path);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(&full, bytes).unwrap();
}

fn fs_store(root: &Path) -> StorageRef {
    Arc::new(OpendalStore::filesystem(root, &test_storage_config()).expect("create fs store"))
}

#[tokio::test]
async fn verified_write_commits_good_bucket_at_final_path_on_object_store() {
    let src_dir = TempDir::new().unwrap();
    let dst_dir = TempDir::new().unwrap();
    let (path, gz) = make_bucket(b"verified bucket content");
    plant_file(src_dir.path(), &path, &gz);

    let src = fs_store(src_dir.path());
    let dst = mock_object_store(dst_dir.path());
    let reader = src.open_reader(&path).await.unwrap();

    verify_and_write_bucket(&path, reader, &dst)
        .await
        .expect("verified write should succeed");

    assert!(dst.exists(&path).await.unwrap(), "object at final path");
    assert!(
        !dst_dir.path().join(format!("{path}.tmp")).exists(),
        "no stranded sibling .tmp object"
    );
}

#[tokio::test]
async fn verified_write_of_corrupt_bucket_leaves_nothing_at_final_path() {
    let src_dir = TempDir::new().unwrap();
    let dst_dir = TempDir::new().unwrap();

    // Path claims the hash of one content, bytes decompress to another:
    // valid gzip, wrong hash — must fail verification.
    let (path, _) = make_bucket(b"content the path claims");
    let (_, wrong_gz) = make_bucket(b"content actually delivered");
    plant_file(src_dir.path(), &path, &wrong_gz);

    let src = fs_store(src_dir.path());
    let dst = mock_object_store(dst_dir.path());
    let reader = src.open_reader(&path).await.unwrap();

    let err = verify_and_write_bucket(&path, reader, &dst)
        .await
        .expect_err("hash mismatch must fail");
    assert!(err.to_string().contains("Hash mismatch"), "got: {err}");

    assert!(
        !dst.exists(&path).await.unwrap(),
        "corrupt data must not be visible at the final path"
    );
    assert!(
        !dst_dir.path().join(format!("{path}.tmp")).exists(),
        "no stranded sibling .tmp object"
    );
}

#[cfg(feature = "opendal-s3")]
#[tokio::test]
async fn s3_store_is_writable_with_no_base_path() {
    let store = OpendalStore::s3(
        "test-bucket",
        Some("us-east-1"),
        Some("http://127.0.0.1:9"), // never contacted; construction only
        Some("test-access-key"),
        Some("test-secret-key"),
        "some/prefix",
        &test_storage_config(),
    )
    .expect("construct S3 store");
    assert!(store.supports_writes());
    assert!(store.get_base_path().is_none());
}

#[test]
fn stamp_passphrase_inserts_when_missing() {
    let out = stamp_network_passphrase(
        br#"{"currentLedger": 1023}"#.to_vec(),
        Some("Test SDF Network ; September 2015"),
    )
    .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(
        json["networkPassphrase"],
        "Test SDF Network ; September 2015"
    );
    assert_eq!(json["currentLedger"], 1023);
}

#[test]
fn stamp_passphrase_is_identity_when_already_present_or_absent() {
    let with = br#"{"currentLedger": 1023, "networkPassphrase": "P"}"#.to_vec();
    assert_eq!(
        stamp_network_passphrase(with.clone(), Some("P")).unwrap(),
        with
    );
    let none = br#"{"currentLedger": 1023}"#.to_vec();
    assert_eq!(stamp_network_passphrase(none.clone(), None).unwrap(), none);
}

#[test]
fn stamp_passphrase_rejects_non_object_json() {
    assert!(stamp_network_passphrase(b"[1,2,3]".to_vec(), Some("P")).is_err());
}

#[tokio::test]
async fn update_well_known_writes_through_the_storage_trait() {
    let dir = TempDir::new().unwrap();
    let store = mock_object_store(dir.path());
    let history_path = crate::history_format::checkpoint_path("history", 1023);

    // Plant the history file *through the store* (no fs paths).
    store
        .write(&history_path, br#"{"currentLedger": 1023}"#.to_vec().into())
        .await
        .unwrap();

    update_well_known_from_history(&store, &history_path, Some("P"), &test_storage_config())
        .await
        .expect("update .well-known via store");

    let got = crate::storage::download_buffer(&store, ROOT_WELL_KNOWN_PATH)
        .await
        .unwrap()
        .to_vec();
    let json: serde_json::Value = serde_json::from_slice(&got).unwrap();
    assert_eq!(json["networkPassphrase"], "P");
    assert_eq!(json["currentLedger"], 1023);
}

/// Assert no `*.tmp` file exists anywhere under `root`. Valid only after
/// operations that completed successfully (a failed verified write may
/// legitimately leave OpenDAL's own abandoned staging file behind, the analog
/// of an S3 incomplete multipart upload).
fn assert_no_tmp_files(root: &Path) {
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            assert!(
                entry.path().extension().is_none_or(|ext| ext != "tmp"),
                "stranded temp file: {}",
                entry.path().display()
            );
        }
    }
}

/// Mirror `src_url` into a pre-built destination store. The mock object store
/// has no URL form, so this drives `MirrorOperation` directly — the same steps
/// `test_helpers::run_mirror` performs for URL destinations.
async fn mirror_url_to_store(src_url: &str, dst: &StorageRef) {
    let config = test_storage_config();
    let src_store =
        crate::storage::from_url_with_config(src_url, &config).expect("create source backend");
    let pipeline_config = PipelineConfig::new(4, false, false, false, config, &src_store).await;
    let operation = MirrorOperation::new(
        src_store,
        dst.clone(),
        /*overwrite=*/ false,
        None,
        None,
        /*allow_mirror_gaps=*/ false,
        pipeline_config.clone(),
        /*update_well_known=*/ true,
    );
    Pipeline::new(operation, pipeline_config, None)
        .run()
        .await
        .expect("mirror to object store should succeed");
}

#[tokio::test]
async fn mirror_to_object_store_end_to_end() {
    let src_url = file_url_from_path(&testnet_small_archive_path());
    let dst_dir = TempDir::new().unwrap();
    let dst = mock_object_store(dst_dir.path());

    mirror_url_to_store(&src_url, &dst).await;

    // .well-known was created through the Storage trait
    assert!(dst.exists(ROOT_WELL_KNOWN_PATH).await.unwrap());

    // Everything landed at final paths — no stranded temp objects
    assert_no_tmp_files(dst_dir.path());

    // The backing dir now holds the complete archive; verify it wholesale
    run_scan(ScanConfig::new(file_url_from_path(dst_dir.path())).verify())
        .await
        .expect("mirrored object-store archive should pass a verify scan");
}

#[tokio::test]
async fn repair_object_store_restores_deleted_bucket_and_well_known() {
    // Seed the "bucket" by mirroring the fixture into it
    let src_url = file_url_from_path(&testnet_small_archive_path());
    let dst_dir = TempDir::new().unwrap();
    mirror_url_to_store(&src_url, &mock_object_store(dst_dir.path())).await;

    // Break the archive behind the store's back: drop a bucket file and .well-known
    let deleted_bucket = delete_first_file(dst_dir.path(), "/bucket-");
    std::fs::remove_file(dst_dir.path().join(ROOT_WELL_KNOWN_PATH)).unwrap();

    // Repair through a fresh mock store, driving RepairOperation directly —
    // the same steps `test_helpers::run_repair` performs for URL destinations.
    let dst = mock_object_store(dst_dir.path());
    let config = test_storage_config();
    let src_store =
        crate::storage::from_url_with_config(&src_url, &config).expect("create source backend");
    let pipeline_config = PipelineConfig::new(4, false, false, false, config, &src_store).await;
    let operation = RepairOperation::new(
        src_store,
        dst.clone(),
        None,
        None,
        /*dry_run=*/ false,
        pipeline_config.clone(),
    );
    Pipeline::new(operation, pipeline_config, None)
        .run()
        .await
        .expect("repair to object store should succeed");

    assert!(
        dst.exists(&deleted_bucket).await.unwrap(),
        "deleted bucket file restored"
    );
    assert!(
        dst.exists(ROOT_WELL_KNOWN_PATH).await.unwrap(),
        ".well-known restored through the Storage trait"
    );
    assert_no_tmp_files(dst_dir.path());

    run_scan(ScanConfig::new(file_url_from_path(dst_dir.path())).verify())
        .await
        .expect("repaired archive should pass a verify scan");
}

#[tokio::test]
async fn http_destination_is_still_rejected() {
    let src_url = file_url_from_path(&testnet_small_archive_path());
    let err = run_mirror(MirrorConfig::new(&src_url, "https://example.org/archive"))
        .await
        .expect_err("HTTP destinations must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("does not support writes"), "got: {msg}");
}
