//! End-to-end tests for XDR verification (--verify flag)
//!
//! These tests verify that the --verify flag properly validates XDR file contents
//! including ledger hash chains, result hash verification, and cross-file consistency.
//!
//! Uses pubnet-archive-old-txset which contains V0 TransactionSet format.

use super::utils::get_files_by_pattern;
use crate::test_helpers::{run_mirror, run_scan, test_storage_config, MirrorConfig, ScanConfig};
use flate2::write::GzEncoder;
use flate2::Compression;
use rstest::rstest;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use stellar_xdr::curr::{
    Frame, Hash, LedgerHeaderHistoryEntry, Limited, Limits, ReadXdr, WriteXdr,
};
use tempfile::TempDir;

fn pubnet_old_txset_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("pubnet-archive-old-txset")
}

fn testnet_small_archive_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join("testnet-archive-small")
}

fn copy_archive(src: &Path, dst: &Path) -> Result<(), std::io::Error> {
    use walkdir::WalkDir;
    for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
        let src_path = entry.path();
        let relative = src_path.strip_prefix(src).unwrap();
        let dst_path = dst.join(relative);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dst_path)?;
        } else {
            std::fs::copy(src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn get_files_in_checkpoint_range(
    archive_path: &Path,
    archive_type: ArchiveType,
    pattern: &str,
) -> Vec<PathBuf> {
    let (low, high) = archive_type.checkpoint_bounds();
    let mut files: Vec<_> = get_files_by_pattern(archive_path, pattern)
        .into_iter()
        .filter(|path| {
            let relative = path.strip_prefix(archive_path).ok();
            let checkpoint = relative
                .and_then(|p| p.to_str())
                .and_then(crate::history_format::checkpoint_from_path);

            match (checkpoint, low, high) {
                (Some(cp), Some(low), Some(high)) => cp >= low && cp <= high,
                (Some(cp), Some(low), None) => cp >= low,
                (Some(cp), None, Some(high)) => cp <= high,
                (Some(_), None, None) => true,
                (None, _, _) => false,
            }
        })
        .collect();
    files.sort();
    files
}

fn get_first_file_in_range(
    archive_path: &Path,
    archive_type: ArchiveType,
    pattern: &str,
) -> PathBuf {
    let files = get_files_in_checkpoint_range(archive_path, archive_type, pattern);
    assert!(
        !files.is_empty(),
        "No files matching pattern '{}' in archive range for {}",
        pattern,
        archive_type.name()
    );
    files.into_iter().next().unwrap()
}

fn decompressed_len(path: &Path) -> usize {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let data = std::fs::read(path).expect("Failed to read file");
    let mut decoder = GzDecoder::new(&data[..]);
    let mut buf = Vec::new();
    decoder
        .read_to_end(&mut buf)
        .expect("Failed to decompress file");
    buf.len()
}

fn get_file_to_corrupt(
    archive_path: &Path,
    archive_type: ArchiveType,
    pattern: &str,
    corruption: CorruptionMethod,
) -> PathBuf {
    let files = get_files_in_checkpoint_range(archive_path, archive_type, pattern);
    assert!(
        !files.is_empty(),
        "No files matching pattern '{}' in archive range for {}",
        pattern,
        archive_type.name()
    );

    match corruption {
        CorruptionMethod::FlippedBytes => files
            .into_iter()
            .find(|path| decompressed_len(path) > 0)
            .expect("No non-empty gzip files available for flipped-bytes corruption"),
        CorruptionMethod::InvalidGzip | CorruptionMethod::WrongContent => {
            files.into_iter().next().unwrap()
        }
    }
}

fn corrupt_file_invalid_gzip(path: &Path) {
    let garbage = b"this is not valid gzip data at all, just garbage bytes!";
    std::fs::write(path, garbage).expect("Failed to write garbage to file");
}

fn corrupt_file_wrong_content(path: &Path) {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(b"This is valid compressed content but completely wrong XDR data!")
        .expect("Failed to write to encoder");
    let compressed = encoder.finish().expect("Failed to finish compression");
    std::fs::write(path, compressed).expect("Failed to write wrong content to file");
}

fn corrupt_xdr_by_flipping_bytes(path: &Path) {
    let data = std::fs::read(path).expect("Failed to read file");
    let decompressed = {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(&data[..]);
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).expect("Failed to decompress");
        buf
    };

    let mut corrupted = decompressed;
    if !corrupted.is_empty() {
        let start = (corrupted.len() / 3).min(corrupted.len() - 1);
        let end = (start + 32).min(corrupted.len());
        for byte in &mut corrupted[start..end] {
            *byte ^= 0xff;
        }
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&corrupted)
        .expect("Failed to write corrupted data");
    let compressed = encoder.finish().expect("Failed to finish compression");
    std::fs::write(path, compressed).expect("Failed to write corrupted file");
}

fn corrupt_json_file(path: &Path) {
    std::fs::write(path, "{ invalid json content }}}").expect("Failed to write corrupt JSON");
}

#[derive(Clone, Copy, Debug)]
enum ArchiveType {
    PubnetOldTxset,
    TestnetSmall,
}

impl ArchiveType {
    fn source_path(&self) -> PathBuf {
        match self {
            ArchiveType::PubnetOldTxset => pubnet_old_txset_archive_path(),
            ArchiveType::TestnetSmall => testnet_small_archive_path(),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ArchiveType::PubnetOldTxset => "pubnet-old-txset",
            ArchiveType::TestnetSmall => "testnet-small",
        }
    }

    fn checkpoint_bounds(&self) -> (Option<u32>, Option<u32>) {
        match self {
            ArchiveType::PubnetOldTxset => (Some(11999999), Some(12001023)),
            ArchiveType::TestnetSmall => (Some(63), Some(255)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum XdrFileType {
    Ledger,
    Transactions,
    Results,
    Scp,
}

impl XdrFileType {
    fn pattern(&self) -> &'static str {
        match self {
            XdrFileType::Ledger => "/ledger-",
            XdrFileType::Transactions => "/transactions-",
            XdrFileType::Results => "/results-",
            XdrFileType::Scp => "/scp-",
        }
    }

    fn name(&self) -> &'static str {
        match self {
            XdrFileType::Ledger => "ledger",
            XdrFileType::Transactions => "transactions",
            XdrFileType::Results => "results",
            XdrFileType::Scp => "scp",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum VerifyOperation {
    Scan,
    Mirror,
}

impl VerifyOperation {
    async fn run_with_verify(
        &self,
        archive_url: &str,
        archive_type: ArchiveType,
        skip_optional: bool,
    ) -> Result<(), crate::Error> {
        match self {
            VerifyOperation::Scan => {
                run_scan(configure_scan(
                    archive_url,
                    archive_type,
                    skip_optional,
                    true,
                ))
                .await
            }
            VerifyOperation::Mirror => {
                let dest_dir = TempDir::new().expect("Failed to create temp dir");
                let dest_url = format!("file://{}", dest_dir.path().display());
                run_mirror(configure_mirror(
                    archive_url,
                    &dest_url,
                    archive_type,
                    skip_optional,
                    true,
                ))
                .await
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CorruptionMethod {
    InvalidGzip,
    WrongContent,
    FlippedBytes,
}

impl CorruptionMethod {
    fn apply(&self, path: &Path) {
        match self {
            CorruptionMethod::InvalidGzip => corrupt_file_invalid_gzip(path),
            CorruptionMethod::WrongContent => corrupt_file_wrong_content(path),
            CorruptionMethod::FlippedBytes => corrupt_xdr_by_flipping_bytes(path),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            CorruptionMethod::InvalidGzip => "invalid-gzip",
            CorruptionMethod::WrongContent => "wrong-content",
            CorruptionMethod::FlippedBytes => "flipped-bytes",
        }
    }
}

fn setup_archive(archive_type: ArchiveType) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let archive_path = temp_dir.path().to_path_buf();
    copy_archive(&archive_type.source_path(), &archive_path).expect("Failed to copy archive");
    (temp_dir, archive_path)
}

fn setup_corrupted_xdr_archive(
    archive_type: ArchiveType,
    file_type: XdrFileType,
    corruption: CorruptionMethod,
) -> (TempDir, PathBuf) {
    let (temp_dir, archive_path) = setup_archive(archive_type);
    let file_to_corrupt =
        get_file_to_corrupt(&archive_path, archive_type, file_type.pattern(), corruption);
    corruption.apply(&file_to_corrupt);
    (temp_dir, archive_path)
}

fn setup_corrupted_history_archive(archive_type: ArchiveType) -> (TempDir, PathBuf) {
    let (temp_dir, archive_path) = setup_archive(archive_type);
    let history_file = get_first_file_in_range(&archive_path, archive_type, "/history-");
    corrupt_json_file(&history_file);
    (temp_dir, archive_path)
}

fn configure_scan(
    archive_url: &str,
    archive_type: ArchiveType,
    skip_optional: bool,
    verify: bool,
) -> ScanConfig {
    let (low, high) = archive_type.checkpoint_bounds();
    let mut config = ScanConfig::new(archive_url);
    if skip_optional {
        config = config.skip_optional();
    }
    if verify {
        config = config.verify();
    }
    if let Some(l) = low {
        config = config.low(l);
    }
    if let Some(h) = high {
        config = config.high(h);
    }
    config
}

fn configure_mirror(
    src_url: &str,
    dst_url: &str,
    archive_type: ArchiveType,
    skip_optional: bool,
    verify: bool,
) -> MirrorConfig {
    let (low, high) = archive_type.checkpoint_bounds();
    let mut config = MirrorConfig::new(src_url, dst_url);
    if skip_optional {
        config = config.skip_optional();
    }
    if verify {
        config = config.verify();
    }
    if let Some(l) = low {
        config = config.low(l);
    }
    if let Some(h) = high {
        config = config.high(h);
    }
    config
}

//=============================================================================
// Valid Archive Tests - Scan with --verify should succeed
//
// These baseline tests verify that --verify passes on known-good archives.
// Two archives are tested to cover both transaction set formats:
//   - testnet-small: Uses V1 GeneralizedTransactionSet (protocol 20+)
//   - pubnet-old-txset: Uses V0 TransactionSet (pre-protocol 20)
//
// If these fail, either the archive test data is corrupted or the verification
// implementation has a bug that rejects valid data.
//=============================================================================

// Verifies --verify flag accepts valid archives with V0 transaction format.
#[rstest]
#[case::pubnet_old_txset(ArchiveType::PubnetOldTxset)]
#[case::testnet_small(ArchiveType::TestnetSmall)]
#[tokio::test]
async fn test_scan_verify_valid_archive(#[case] archive_type: ArchiveType) {
    let archive_url = format!("file://{}", archive_type.source_path().display());

    let result = run_scan(configure_scan(&archive_url, archive_type, true, true)).await;

    result.unwrap_or_else(|e| {
        panic!(
            "Scan --verify should succeed on valid {} archive: {}",
            archive_type.name(),
            e
        )
    });
}

#[rstest]
#[case::pubnet_old_txset(ArchiveType::PubnetOldTxset)]
#[case::testnet_small(ArchiveType::TestnetSmall)]
#[tokio::test]
async fn test_mirror_verify_valid_archive(#[case] archive_type: ArchiveType) {
    let archive_url = format!("file://{}", archive_type.source_path().display());
    let dest_dir = TempDir::new().expect("Failed to create temp dir");
    let dest_url = format!("file://{}", dest_dir.path().display());

    run_mirror(configure_mirror(
        &archive_url,
        &dest_url,
        archive_type,
        true,
        true,
    ))
    .await
    .unwrap_or_else(|e| {
        panic!(
            "Mirror --verify should succeed on valid {} archive: {}",
            archive_type.name(),
            e
        )
    });

    run_scan(configure_scan(&dest_url, archive_type, true, true))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Scan --verify should succeed on mirrored {} archive: {}",
                archive_type.name(),
                e
            )
        });
}

//=============================================================================
// XDR File Corruption Tests (Scan)
//
// Tests that --verify detects corruption across all XDR file types (ledger,
// transactions, results) and all corruption methods (InvalidGzip, WrongContent,
// FlippedBytes) for both archive formats.
//=============================================================================

#[rstest]
#[case::pubnet_ledger_invalid_gzip(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Ledger,
    CorruptionMethod::InvalidGzip
)]
#[case::pubnet_ledger_wrong_content(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Ledger,
    CorruptionMethod::WrongContent
)]
#[case::pubnet_ledger_flipped_bytes(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Ledger,
    CorruptionMethod::FlippedBytes
)]
#[case::pubnet_transactions_invalid_gzip(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Transactions,
    CorruptionMethod::InvalidGzip
)]
#[case::pubnet_transactions_wrong_content(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Transactions,
    CorruptionMethod::WrongContent
)]
#[case::pubnet_transactions_flipped_bytes(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Transactions,
    CorruptionMethod::FlippedBytes
)]
#[case::pubnet_results_invalid_gzip(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Results,
    CorruptionMethod::InvalidGzip
)]
#[case::pubnet_results_wrong_content(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Results,
    CorruptionMethod::WrongContent
)]
#[case::pubnet_results_flipped_bytes(
    ArchiveType::PubnetOldTxset,
    XdrFileType::Results,
    CorruptionMethod::FlippedBytes
)]
#[case::testnet_ledger_invalid_gzip(
    ArchiveType::TestnetSmall,
    XdrFileType::Ledger,
    CorruptionMethod::InvalidGzip
)]
#[case::testnet_ledger_wrong_content(
    ArchiveType::TestnetSmall,
    XdrFileType::Ledger,
    CorruptionMethod::WrongContent
)]
#[case::testnet_ledger_flipped_bytes(
    ArchiveType::TestnetSmall,
    XdrFileType::Ledger,
    CorruptionMethod::FlippedBytes
)]
#[case::testnet_transactions_invalid_gzip(
    ArchiveType::TestnetSmall,
    XdrFileType::Transactions,
    CorruptionMethod::InvalidGzip
)]
#[case::testnet_transactions_wrong_content(
    ArchiveType::TestnetSmall,
    XdrFileType::Transactions,
    CorruptionMethod::WrongContent
)]
#[case::testnet_transactions_flipped_bytes(
    ArchiveType::TestnetSmall,
    XdrFileType::Transactions,
    CorruptionMethod::FlippedBytes
)]
#[case::testnet_results_invalid_gzip(
    ArchiveType::TestnetSmall,
    XdrFileType::Results,
    CorruptionMethod::InvalidGzip
)]
#[case::testnet_results_wrong_content(
    ArchiveType::TestnetSmall,
    XdrFileType::Results,
    CorruptionMethod::WrongContent
)]
#[case::testnet_results_flipped_bytes(
    ArchiveType::TestnetSmall,
    XdrFileType::Results,
    CorruptionMethod::FlippedBytes
)]
#[tokio::test]
async fn test_scan_verify_detects_corrupt_xdr(
    #[case] archive_type: ArchiveType,
    #[case] file_type: XdrFileType,
    #[case] corruption: CorruptionMethod,
) {
    let (_temp_dir, archive_path) =
        setup_corrupted_xdr_archive(archive_type, file_type, corruption);
    let archive_url = format!("file://{}", archive_path.display());

    let result = run_scan(configure_scan(&archive_url, archive_type, true, true)).await;

    assert!(
        result.is_err(),
        "Scan --verify should fail on {} {} corruption ({}) but succeeded",
        archive_type.name(),
        file_type.name(),
        corruption.name()
    );
}

//=============================================================================
// History JSON File Corruption Tests
//
// History JSON files (history-*.json) are the entry point for each checkpoint.
// They contain:
//   - Current ledger state hash (for bucket verification)
//   - List of bucket hashes to verify
//   - Checkpoint metadata
//
// If history JSON is corrupted, the entire checkpoint cannot be processed.
// This is different from XDR corruption - it fails at the discovery phase
// rather than the verification phase.
//=============================================================================

// Verifies --verify fails when history JSON is corrupted (unparseable).
#[rstest]
#[case::pubnet_old_txset(ArchiveType::PubnetOldTxset)]
#[case::testnet_small(ArchiveType::TestnetSmall)]
#[tokio::test]
async fn test_scan_verify_detects_corrupt_history_json(#[case] archive_type: ArchiveType) {
    let (_temp_dir, archive_path) = setup_corrupted_history_archive(archive_type);
    let archive_url = format!("file://{}", archive_path.display());

    let result = run_scan(configure_scan(&archive_url, archive_type, true, true)).await;

    assert!(
        result.is_err(),
        "Scan --verify should fail on {} history JSON corruption but succeeded",
        archive_type.name()
    );
}

//=============================================================================
// Cross-file Verification Tests - Result Hash Mismatch
//
// These tests verify cross-file consistency: the ledger header contains expected
// hashes (tx_set_hash, tx_set_result_hash) that must match computed hashes from
// the transaction and result files.
//
// corrupt_result_hash_in_ledger modifies bytes in the ledger file to change the
// expected hash, creating a mismatch when compared against the actual result file.
// This simulates scenarios like:
//   - Ledger file from one checkpoint, result file from another
//   - Partial archive corruption affecting only some files
//=============================================================================

fn corrupt_result_hash_in_ledger(archive_path: &Path, archive_type: ArchiveType) {
    let ledger_file = get_first_file_in_range(archive_path, archive_type, "/ledger-");

    let data = std::fs::read(&ledger_file).expect("Failed to read file");
    let decompressed = {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(&data[..]);
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).expect("Failed to decompress");
        buf
    };

    let mut corrupted = decompressed;
    if corrupted.len() > 200 {
        for byte in corrupted.iter_mut().take(182).skip(150) {
            *byte = 0xff;
        }
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&corrupted)
        .expect("Failed to write corrupted data");
    let compressed = encoder.finish().expect("Failed to finish compression");
    std::fs::write(&ledger_file, compressed).expect("Failed to write corrupted file");
}

//=============================================================================
// Hash Chain Verification Tests
//
// The ledger hash chain is the core integrity guarantee: each ledger header
// contains previous_ledger_hash = SHA256(previous_header_xdr). Breaking this
// chain means the ledger history has been tampered with or corrupted.
//
// corrupt_prev_ledger_hash modifies the second ledger file's prev_hash field,
// breaking the chain from the first checkpoint to the second. This simulates:
//   - Deliberate history rewriting
//   - File corruption affecting the prev_hash bytes
//   - Wrong file served for a checkpoint
//=============================================================================

fn corrupt_prev_ledger_hash(archive_path: &Path, archive_type: ArchiveType) {
    let ledger_files = get_files_in_checkpoint_range(archive_path, archive_type, "/ledger-");

    assert!(
        ledger_files.len() >= 2,
        "Need at least 2 ledger files for hash chain test"
    );

    let second_file = &ledger_files[1];

    let data = std::fs::read(second_file).expect("Failed to read file");
    let decompressed = {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(&data[..]);
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).expect("Failed to decompress");
        buf
    };

    let mut corrupted = decompressed;
    if corrupted.len() > 100 {
        for byte in corrupted.iter_mut().take(82).skip(50) {
            *byte = 0xaa;
        }
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&corrupted)
        .expect("Failed to write corrupted data");
    let compressed = encoder.finish().expect("Failed to finish compression");
    std::fs::write(second_file, compressed).expect("Failed to write corrupted file");
}

//=============================================================================
// Without --verify flag, corruption should be ignored
//
// The default scan mode (without --verify) only checks file existence, not content.
// This is intentional for performance: full verification requires decompressing
// and parsing every file, which is expensive for large archives.
//
// These tests confirm that corrupted files don't cause failures when --verify
// is not specified - the scan completes successfully because it only checks
// that the files exist, not that their contents are valid.
//=============================================================================

// Verifies scan without --verify passes even when XDR files are corrupted.
// Only file existence is checked, not content validity.
#[rstest]
#[case::ledger_wrong(XdrFileType::Ledger, CorruptionMethod::WrongContent)]
#[case::ledger_flipped(XdrFileType::Ledger, CorruptionMethod::FlippedBytes)]
#[case::ledger_gzip(XdrFileType::Ledger, CorruptionMethod::InvalidGzip)]
#[case::transactions_wrong(XdrFileType::Transactions, CorruptionMethod::WrongContent)]
#[case::transactions_flipped(XdrFileType::Transactions, CorruptionMethod::FlippedBytes)]
#[case::transactions_gzip(XdrFileType::Transactions, CorruptionMethod::InvalidGzip)]
#[case::results_wrong(XdrFileType::Results, CorruptionMethod::WrongContent)]
#[case::results_flipped(XdrFileType::Results, CorruptionMethod::FlippedBytes)]
#[case::results_gzip(XdrFileType::Results, CorruptionMethod::InvalidGzip)]
#[tokio::test]
async fn test_scan_no_verify_ignores_xdr_corruption(
    #[case] file_type: XdrFileType,
    #[case] corruption: CorruptionMethod,
) {
    let (low, high) = ArchiveType::PubnetOldTxset.checkpoint_bounds();
    let (_temp_dir, archive_path) =
        setup_corrupted_xdr_archive(ArchiveType::PubnetOldTxset, file_type, corruption);
    let archive_url = format!("file://{}", archive_path.display());

    let mut config = ScanConfig::new(&archive_url).skip_optional();
    if let Some(l) = low {
        config = config.low(l);
    }
    if let Some(h) = high {
        config = config.high(h);
    }
    let result = run_scan(config).await;

    result.unwrap_or_else(|e| {
        panic!(
            "Scan without --verify should pass even with corrupt {} file: {}",
            file_type.name(),
            e
        )
    });
}

#[rstest]
#[case::scan_v0(VerifyOperation::Scan, ArchiveType::PubnetOldTxset)]
#[case::scan_v1(VerifyOperation::Scan, ArchiveType::TestnetSmall)]
#[case::mirror_v0(VerifyOperation::Mirror, ArchiveType::PubnetOldTxset)]
#[case::mirror_v1(VerifyOperation::Mirror, ArchiveType::TestnetSmall)]
#[tokio::test]
async fn test_verify_detects_corrupt_scp_when_optional_enabled(
    #[case] op: VerifyOperation,
    #[case] archive_type: ArchiveType,
) {
    let (_temp_dir, archive_path) = setup_corrupted_xdr_archive(
        archive_type,
        XdrFileType::Scp,
        CorruptionMethod::WrongContent,
    );
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_with_verify(&archive_url, archive_type, false).await;

    assert!(
        result.is_err(),
        "{:?} --verify should fail on {} scp corruption when optional files are included",
        op,
        archive_type.name()
    );
}

#[rstest]
#[case::ledger_scan(
    VerifyOperation::Scan,
    ArchiveType::PubnetOldTxset,
    XdrFileType::Ledger,
    CorruptionMethod::FlippedBytes
)]
#[case::ledger_mirror(
    VerifyOperation::Mirror,
    ArchiveType::PubnetOldTxset,
    XdrFileType::Ledger,
    CorruptionMethod::FlippedBytes
)]
#[case::transactions_scan(
    VerifyOperation::Scan,
    ArchiveType::TestnetSmall,
    XdrFileType::Transactions,
    CorruptionMethod::WrongContent
)]
#[case::transactions_mirror(
    VerifyOperation::Mirror,
    ArchiveType::TestnetSmall,
    XdrFileType::Transactions,
    CorruptionMethod::WrongContent
)]
#[case::results_scan(
    VerifyOperation::Scan,
    ArchiveType::PubnetOldTxset,
    XdrFileType::Results,
    CorruptionMethod::InvalidGzip
)]
#[case::results_mirror(
    VerifyOperation::Mirror,
    ArchiveType::TestnetSmall,
    XdrFileType::Results,
    CorruptionMethod::InvalidGzip
)]
#[tokio::test]
async fn test_verify_detects_corrupt_xdr_in_scan_and_mirror(
    #[case] op: VerifyOperation,
    #[case] archive_type: ArchiveType,
    #[case] file_type: XdrFileType,
    #[case] corruption: CorruptionMethod,
) {
    let (_temp_dir, archive_path) =
        setup_corrupted_xdr_archive(archive_type, file_type, corruption);
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_with_verify(&archive_url, archive_type, true).await;

    assert!(
        result.is_err(),
        "{:?} --verify should fail on {} {} corruption ({})",
        op,
        archive_type.name(),
        file_type.name(),
        corruption.name()
    );
}

#[rstest]
#[case::scan_v0(VerifyOperation::Scan, ArchiveType::PubnetOldTxset)]
#[case::scan_v1(VerifyOperation::Scan, ArchiveType::TestnetSmall)]
#[case::mirror_v0(VerifyOperation::Mirror, ArchiveType::PubnetOldTxset)]
#[case::mirror_v1(VerifyOperation::Mirror, ArchiveType::TestnetSmall)]
#[tokio::test]
async fn test_verify_detects_hash_chain_break_in_scan_and_mirror(
    #[case] op: VerifyOperation,
    #[case] archive_type: ArchiveType,
) {
    let (_temp_dir, archive_path) = setup_archive(archive_type);
    corrupt_prev_ledger_hash(&archive_path, archive_type);
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_with_verify(&archive_url, archive_type, true).await;
    assert!(
        result.is_err(),
        "{:?} --verify should fail on {} hash chain corruption",
        op,
        archive_type.name()
    );
}

#[rstest]
#[case::scan_v0(VerifyOperation::Scan, ArchiveType::PubnetOldTxset)]
#[case::scan_v1(VerifyOperation::Scan, ArchiveType::TestnetSmall)]
#[case::mirror_v0(VerifyOperation::Mirror, ArchiveType::PubnetOldTxset)]
#[case::mirror_v1(VerifyOperation::Mirror, ArchiveType::TestnetSmall)]
#[tokio::test]
async fn test_verify_detects_result_hash_mismatch_in_scan_and_mirror(
    #[case] op: VerifyOperation,
    #[case] archive_type: ArchiveType,
) {
    let (_temp_dir, archive_path) = setup_archive(archive_type);
    corrupt_result_hash_in_ledger(&archive_path, archive_type);
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_with_verify(&archive_url, archive_type, true).await;
    assert!(
        result.is_err(),
        "{:?} --verify should fail on {} result hash mismatch",
        op,
        archive_type.name()
    );
}

#[tokio::test]
async fn test_scan_verify_fails_with_multiple_corrupt_files_same_checkpoint() {
    let (_temp_dir, archive_path) = setup_archive(ArchiveType::PubnetOldTxset);
    let ledger_file = get_first_file_in_range(
        &archive_path,
        ArchiveType::PubnetOldTxset,
        XdrFileType::Ledger.pattern(),
    );
    let transactions_file = get_first_file_in_range(
        &archive_path,
        ArchiveType::PubnetOldTxset,
        XdrFileType::Transactions.pattern(),
    );
    corrupt_xdr_by_flipping_bytes(&ledger_file);
    corrupt_file_wrong_content(&transactions_file);

    let archive_url = format!("file://{}", archive_path.display());
    let result = run_scan(configure_scan(
        &archive_url,
        ArchiveType::PubnetOldTxset,
        true,
        true,
    ))
    .await;

    assert!(
        result.is_err(),
        "Scan --verify should fail when multiple files in a checkpoint are corrupt"
    );
}

//=============================================================================
// XDR-level ledger manipulation helpers
//
// These helpers parse, modify, and rewrite ledger files at the XDR type level
// (rather than raw bytes), preserving per-entry hash consistency so that
// failures surface at cross-file or cross-checkpoint verification.
//=============================================================================

fn read_and_parse_ledger_file(path: &Path) -> Vec<LedgerHeaderHistoryEntry> {
    use flate2::read::GzDecoder;
    use std::io::Read as _;

    let data = std::fs::read(path).expect("Failed to read ledger file");
    let mut decoder = GzDecoder::new(&data[..]);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .expect("Failed to decompress");

    let cursor = std::io::Cursor::new(&decompressed);
    let mut limited = Limited::new(cursor, Limits::none());

    Frame::<LedgerHeaderHistoryEntry>::read_xdr_iter(&mut limited)
        .map(|r| r.expect("Failed to parse ledger entry").0)
        .collect()
}

fn recompute_entry_hash(entry: &mut LedgerHeaderHistoryEntry) {
    let header_xdr = entry
        .header
        .to_xdr(Limits::none())
        .expect("Failed to serialize header");
    entry.hash = Hash(Sha256::digest(&header_xdr).into());
}

fn write_ledger_entries_to_file(path: &Path, entries: &[LedgerHeaderHistoryEntry]) {
    let mut data = Vec::new();
    for entry in entries {
        let entry_xdr = entry
            .to_xdr(Limits::none())
            .expect("Failed to serialize entry");
        let frame_len = entry_xdr.len() as u32 | 0x8000_0000;
        data.extend_from_slice(&frame_len.to_be_bytes());
        data.extend_from_slice(&entry_xdr);
    }

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&data)
        .expect("Failed to write compressed data");
    let compressed = encoder.finish().expect("Failed to finish compression");
    std::fs::write(path, compressed).expect("Failed to write file");
}

/// Modify all entries in a ledger file: set the specified hash field to a wrong
/// value, recompute each entry's hash, and fix the internal prev-hash chain so
/// the file passes per-entry and intra-checkpoint validation. The mismatch
/// surfaces only during cross-file verification.
fn corrupt_ledger_hash_field_preserving_entry_hash(
    archive_path: &Path,
    archive_type: ArchiveType,
    field: &str,
) {
    let ledger_file = get_first_file_in_range(archive_path, archive_type, "/ledger-");
    let mut entries = read_and_parse_ledger_file(&ledger_file);

    for i in 0..entries.len() {
        match field {
            "tx_set" => entries[i].header.scp_value.tx_set_hash = Hash([0xDE; 32]),
            "result" => entries[i].header.tx_set_result_hash = Hash([0xDE; 32]),
            _ => unreachable!(),
        }
        if i > 0 {
            entries[i].header.previous_ledger_hash = entries[i - 1].hash.clone();
        }
        recompute_entry_hash(&mut entries[i]);
    }

    write_ledger_entries_to_file(&ledger_file, &entries);
}

/// Corrupt the first ledger of the second checkpoint file by changing its
/// previous_ledger_hash. Recompute all subsequent entry hashes and fix the
/// internal chain so the ONLY failure is at the cross-checkpoint boundary.
fn corrupt_cross_checkpoint_boundary(archive_path: &Path, archive_type: ArchiveType) {
    let ledger_files = get_files_in_checkpoint_range(archive_path, archive_type, "/ledger-");
    assert!(
        ledger_files.len() >= 2,
        "Need at least 2 ledger files for cross-checkpoint chain test"
    );

    let second_file = &ledger_files[1];
    let mut entries = read_and_parse_ledger_file(second_file);

    // Break the cross-checkpoint boundary: set first entry's prev_hash to garbage
    entries[0].header.previous_ledger_hash = Hash([0xBA; 32]);
    recompute_entry_hash(&mut entries[0]);

    // Fix the internal chain so the ONLY failure is at the boundary
    for i in 1..entries.len() {
        entries[i].header.previous_ledger_hash = entries[i - 1].hash.clone();
        recompute_entry_hash(&mut entries[i]);
    }

    write_ledger_entries_to_file(second_file, &entries);
}

//=============================================================================
// True cross-file hash mismatch tests
//
// Unlike the raw-byte corruption tests above, these modify the ledger header
// at the XDR level and recompute entry.hash so the ledger file is internally
// consistent. The failure surfaces only during cross-file verification when
// the ledger header's expected hash doesn't match the actual file hash.
//=============================================================================

#[rstest]
#[case::tx_set_scan_v0(VerifyOperation::Scan, ArchiveType::PubnetOldTxset, "tx_set")]
#[case::tx_set_scan_v1(VerifyOperation::Scan, ArchiveType::TestnetSmall, "tx_set")]
#[case::tx_set_mirror_v0(VerifyOperation::Mirror, ArchiveType::PubnetOldTxset, "tx_set")]
#[case::tx_set_mirror_v1(VerifyOperation::Mirror, ArchiveType::TestnetSmall, "tx_set")]
#[case::result_scan_v0(VerifyOperation::Scan, ArchiveType::PubnetOldTxset, "result")]
#[case::result_scan_v1(VerifyOperation::Scan, ArchiveType::TestnetSmall, "result")]
#[case::result_mirror_v0(VerifyOperation::Mirror, ArchiveType::PubnetOldTxset, "result")]
#[case::result_mirror_v1(VerifyOperation::Mirror, ArchiveType::TestnetSmall, "result")]
#[case::chain_scan_v0(VerifyOperation::Scan, ArchiveType::PubnetOldTxset, "chain")]
#[case::chain_scan_v1(VerifyOperation::Scan, ArchiveType::TestnetSmall, "chain")]
#[case::chain_mirror_v0(VerifyOperation::Mirror, ArchiveType::PubnetOldTxset, "chain")]
#[case::chain_mirror_v1(VerifyOperation::Mirror, ArchiveType::TestnetSmall, "chain")]
#[tokio::test]
async fn test_verify_detects_true_cross_file_mismatch(
    #[case] op: VerifyOperation,
    #[case] archive_type: ArchiveType,
    #[case] corruption_type: &str,
) {
    let (_temp_dir, archive_path) = setup_archive(archive_type);
    match corruption_type {
        "tx_set" | "result" => corrupt_ledger_hash_field_preserving_entry_hash(
            &archive_path,
            archive_type,
            corruption_type,
        ),
        "chain" => corrupt_cross_checkpoint_boundary(&archive_path, archive_type),
        _ => unreachable!(),
    }
    let archive_url = format!("file://{}", archive_path.display());

    let result = op.run_with_verify(&archive_url, archive_type, true).await;
    assert!(
        result.is_err(),
        "{:?} --verify should fail on {} true {corruption_type} mismatch",
        op,
        archive_type.name()
    );
}

//=============================================================================
// Atomic Write Safety Tests
//
// Verify that corrupt XDR files are never committed to the destination,
// for both atomic and non-atomic write backends.
//=============================================================================

#[rstest]
#[case::non_atomic(false)]
#[case::atomic(true)]
#[tokio::test]
async fn test_mirror_verify_no_corrupt_file_written(#[case] atomic: bool) {
    let (_temp_src, archive_path) = setup_corrupted_xdr_archive(
        ArchiveType::PubnetOldTxset,
        XdrFileType::Transactions,
        CorruptionMethod::WrongContent,
    );
    let corrupt_file = get_first_file_in_range(
        &archive_path,
        ArchiveType::PubnetOldTxset,
        XdrFileType::Transactions.pattern(),
    );
    let relative = corrupt_file.strip_prefix(&archive_path).unwrap();

    let temp_dest = TempDir::new().expect("Failed to create temp dir");
    let src_url = format!("file://{}", archive_path.display());
    let dest_url = format!("file://{}", temp_dest.path().display());

    let mut storage_cfg = test_storage_config();
    storage_cfg.atomic_file_writes = atomic;

    let (low, high) = ArchiveType::PubnetOldTxset.checkpoint_bounds();
    let mut config = MirrorConfig::new(&src_url, &dest_url)
        .skip_optional()
        .verify()
        .storage_config(storage_cfg);
    if let Some(l) = low {
        config = config.low(l);
    }
    if let Some(h) = high {
        config = config.high(h);
    }

    let result = run_mirror(config).await;
    assert!(result.is_err(), "mirror should fail on corrupt XDR");

    assert!(
        !temp_dest.path().join(relative).exists(),
        "corrupt xdr file should not exist at destination (atomic={})",
        atomic,
    );
}
