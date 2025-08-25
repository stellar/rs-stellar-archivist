//! Correctness tests for mirror command using filesystem operations

use crate::{
    history_format,
    test_helpers::{run_mirror, run_scan, MirrorConfig, ScanConfig},
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use walkdir::WalkDir;

// Checks if all files (except .well-known/stellar-history.json) are mirrored properly
fn verify_mirror_correctness(source_path: &Path, dest_path: &Path, max_checkpoint: Option<u32>) {
    let mut expected_bucket_hashes = HashSet::new();

    // Walk source and verify all files are mirrored to dest. Also collect all referenced bucket hashes
    for entry in WalkDir::new(source_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let src_file = entry.path();
        let relative_path = src_file
            .strip_prefix(source_path)
            .expect("Failed to get relative path");
        let path_str = relative_path.to_string_lossy();

        // Skip .well-known file as it's handled specially
        if path_str == ".well-known/stellar-history.json" {
            continue;
        }

        // For history files within bounds, parse and collect bucket hashes
        if path_str.contains("history-") && path_str.ends_with(".json") {
            let filename = src_file.file_name().unwrap().to_string_lossy();
            let checkpoint = history_format::checkpoint_from_filename(&filename).expect(&format!(
                "Failed to extract checkpoint from history filename: {}",
                filename
            ));

            if let Some(max_cp) = max_checkpoint {
                if checkpoint > max_cp {
                    // Skip history files beyond the bound
                    continue;
                }
            }

            // Parse the history file to get referenced bucket hashes
            let content = std::fs::read_to_string(&src_file)
                .expect(&format!("Failed to read history file: {:?}", src_file));
            let has: history_format::HistoryFileState = serde_json::from_str(&content)
                .expect(&format!("Failed to parse history file: {:?}", src_file));
            for hash in has.buckets() {
                expected_bucket_hashes.insert(hash);
            }
        }

        let dst_file = dest_path.join(&relative_path);

        // Check non-bucket files for existence in destination
        if !path_str.starts_with("bucket/") {
            if let Some(max_cp) = max_checkpoint {
                // For bounded mirrors, check if this file should be included
                if path_str.contains("history-")
                    || path_str.contains("ledger-")
                    || path_str.contains("results-")
                    || path_str.contains("transactions-")
                    || path_str.contains("scp-")
                {
                    let filename = src_file.file_name().unwrap().to_string_lossy();
                    let checkpoint =
                        history_format::checkpoint_from_filename(&filename).expect(&format!(
                            "Failed to extract checkpoint from history filename: {}",
                            filename
                        ));
                    if checkpoint > max_cp {
                        assert!(
                            !dst_file.exists(),
                            "File beyond bound should not exist: {} (checkpoint 0x{:08x})",
                            path_str,
                            checkpoint
                        );
                        continue;
                    }

                    // File is within bounds, should exist
                    assert!(
                        dst_file.exists(),
                        "Missing required file within bound: {} (checkpoint 0x{:08x})",
                        path_str,
                        checkpoint
                    );
                } else {
                    // Other non-bucket files should exist
                    assert!(
                        dst_file.exists(),
                        "Missing file in destination: {}",
                        path_str
                    );
                }
            } else {
                // For unbounded mirrors, all files should be in destination
                assert!(
                    dst_file.exists(),
                    "Missing file in destination: {}",
                    path_str
                );
            }
        }
    }

    // Second pass: Walk destination files to verify correctness and no extra files
    for entry in WalkDir::new(dest_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let dst_file = entry.path();
        let relative_path = dst_file
            .strip_prefix(dest_path)
            .expect("Failed to get relative path");
        let path_str = relative_path.to_string_lossy();

        // Skip .well-known file as it's handled specially
        if path_str == ".well-known/stellar-history.json" {
            continue;
        }

        // Check that the bucket file is actually reference by something in bounds
        if path_str.starts_with("bucket/") {
            let filename = dst_file.file_name().unwrap().to_string_lossy();
            let hash = history_format::bucket_hash_from_filename(&filename).expect(&format!(
                "Failed to extract bucket hash from filename: {}",
                filename
            ));
            assert!(
                expected_bucket_hashes.remove(&hash),
                "Unexpected bucket file in destination: {} (hash: {})",
                path_str,
                hash
            );
        }

        // For checkpoint files, verify they're within bounds if max_checkpoint is set
        if let Some(max_cp) = max_checkpoint {
            if !path_str.starts_with("bucket/") {
                let filename = dst_file.file_name().unwrap().to_string_lossy();
                if let Some(checkpoint) = history_format::checkpoint_from_filename(&filename) {
                    assert!(
                        checkpoint <= max_cp,
                        "Found file beyond bound: {} (checkpoint 0x{:08x} > 0x{:08x})",
                        path_str,
                        checkpoint,
                        max_cp
                    );
                }
            }
        }

        // All files in destination must exist in source and match content
        let src_file = source_path.join(&relative_path);
        assert!(
            src_file.exists(),
            "Destination file has no source equivalent: {}",
            path_str
        );

        let src_content =
            std::fs::read(&src_file).expect(&format!("Failed to read source: {:?}", src_file));
        let dst_content =
            std::fs::read(&dst_file).expect(&format!("Failed to read dest: {:?}", dst_file));

        assert_eq!(
            src_content,
            dst_content,
            "Content mismatch for {}",
            relative_path.display()
        );
    }

    // Verify all expected buckets were found
    assert!(
        expected_bucket_hashes.is_empty(),
        "Missing {} expected bucket files",
        expected_bucket_hashes.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_mirror_and_scan_roundtrip() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    let temp_dir1 = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest1 = temp_dir1.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest1),
        concurrency: 4,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(mirror_config).await.expect("Full mirror failed");

    // Scan the fully mirrored archive
    let scan_config = ScanConfig {
        archive: format!("file://{}", mirror_dest1),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
    };

    run_scan(scan_config)
        .await
        .expect("Scan of full archive failed");

    let temp_dir2 = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest2 = temp_dir2.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest2),
        concurrency: 4,
        skip_optional: false,
        high: Some(4991),
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(mirror_config)
        .await
        .expect("Bounded mirror failed");

    let scan_config = ScanConfig {
        archive: format!("file://{}", mirror_dest2),
        concurrency: 4,
        skip_optional: false,
        low: None,
        high: None,
    };

    run_scan(scan_config).await.unwrap();

    let temp_dir3 = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest3 = temp_dir3.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest3),
        concurrency: 4,
        skip_optional: true,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(mirror_config)
        .await
        .expect("Mirror with skip_optional failed");

    // Verify that the scp directory doesn't exist
    let scp_dir = temp_dir3.path().join("scp");
    assert!(!scp_dir.exists());

    let scan_config = ScanConfig {
        archive: format!("file://{}", mirror_dest3),
        concurrency: 4,
        skip_optional: true,
        low: None,
        high: None,
    };

    run_scan(scan_config)
        .await
        .expect("Scan of archive without optional files failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_mirror() {
    let test_archive_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest = temp_dir.path().to_str().unwrap();

    // Mirror with high concurrency
    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest),
        concurrency: 20,
        skip_optional: false,
        high: None,
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(mirror_config)
        .await
        .expect("Mirror with high concurrency failed");

    verify_mirror_correctness(
        &test_archive_path,
        temp_dir.path(),
        None, // No upper bound
    );

    // Verify .well-known file, which should be the same in src and dst since we don't have an upper bound
    let src_has = test_archive_path.join(".well-known/stellar-history.json");
    let dst_has = temp_dir.path().join(".well-known/stellar-history.json");

    let src_content = std::fs::read(&src_has).expect("Failed to read source HAS");
    let dst_content = std::fs::read(&dst_has).expect("Failed to read dest HAS");

    let src_json: serde_json::Value =
        serde_json::from_slice(&src_content).expect("Failed to parse source HAS");
    let dst_json: serde_json::Value =
        serde_json::from_slice(&dst_content).expect("Failed to parse dest HAS");

    assert_eq!(src_json, dst_json);

    let temp_dir2 = TempDir::new().expect("Failed to create temp dir");
    let mirror_dest2 = temp_dir2.path().to_str().unwrap();

    let mirror_config = MirrorConfig {
        src: format!("file://{}", test_archive_path.to_str().unwrap()),
        dst: format!("file://{}", mirror_dest2),
        concurrency: 20,
        skip_optional: false,
        high: Some(4991),
        low: None,
        overwrite: false,
        allow_mirror_gaps: false,
    };

    run_mirror(mirror_config)
        .await
        .expect("Bounded mirror failed");

    // Verify the .well-known file is the same as the history file for checkpoint 4991
    let has_path = temp_dir2.path().join(".well-known/stellar-history.json");
    let has_content = std::fs::read(&has_path).expect("Failed to read destination HAS");

    let history_path = test_archive_path.join("history/00/00/13/history-0000137f.json");
    let history_content = std::fs::read(&history_path).expect("Failed to read source history file");

    assert_eq!(has_content, history_content);

    // Use helper to verify bounded mirror
    verify_mirror_correctness(
        &test_archive_path,
        temp_dir2.path(),
        Some(0x137f), // Max checkpoint 4991
    );
}
