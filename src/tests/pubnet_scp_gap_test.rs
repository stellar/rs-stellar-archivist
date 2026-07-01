//! Tests for the pubnet early-SCP-gap tolerance against a **real** pubnet slice.
//!
//! `testdata/pubnet-archive-scp-boundary` is a mirror of
//! `history.stellar.org/prd/core-live/core_live_001` over checkpoints
//! [1_213_823, 1_214_271], straddling `FIRST_SCP_CHECKPOINT` (1_214_079). On
//! pubnet the four checkpoints below the boundary genuinely have **no** `scp-*`
//! file (SCP was not archived that early and can never be backfilled); the four
//! at/above it do. The fixture's root `.well-known` carries the pubnet
//! networkPassphrase, so `PipelineConfig::new` resolves `is_pubnet()` true and
//! the gap tolerance activates.
//!
//! Unlike the synthetic tests (which fake pubnet by rewriting a testnet
//! passphrase and deleting an scp), these exercise the real absence pattern.
//!
//! Note on repair retry: the repair checkpoint-retry sub-pipeline inherits
//! `is_pubnet` via the `PipelineConfig` clone it is built from, so it gates SCP
//! scheduling identically to the main pass — the gap SCP never becomes a repair
//! action in either. That path is covered structurally by construction rather
//! than by forcing a (non-deterministic) mid-run retry here.

use super::utils::{
    copy_pubnet_scp_boundary_archive, file_url_from_path, get_files_by_pattern,
    pubnet_scp_boundary_archive_path, set_network_passphrase, PUBNET_SCP_BOUNDARY_AT_CP,
    PUBNET_SCP_BOUNDARY_BELOW_CP, PUBNET_SCP_BOUNDARY_HIGH, PUBNET_SCP_BOUNDARY_LOW,
};
use crate::test_helpers::{
    run_mirror, run_repair, run_scan, MirrorConfig, RepairConfig, ScanConfig,
};
use tempfile::TempDir;

/// A clearly non-pubnet passphrase, to flip the fixture out of pubnet mode.
const TESTNET_PASSPHRASE: &str = "Test SDF Network ; September 2015";

fn fixture_url() -> String {
    file_url_from_path(&pubnet_scp_boundary_archive_path())
}

/// Delete the single file matching `category-{cp:08x}` from `dir`; panics if it
/// isn't there (so a test can't silently assert on a no-op).
fn delete_checkpoint_file(dir: &std::path::Path, category: &str, cp: u32) {
    let pat = format!("/{category}-{cp:08x}");
    let files = get_files_by_pattern(dir, &pat);
    assert_eq!(files.len(), 1, "expected exactly one file matching {pat}");
    std::fs::remove_file(&files[0]).expect("delete checkpoint file");
}

//=============================================================================
// Scan
//=============================================================================

/// The core case: a real pubnet slice whose early checkpoints have no scp files
/// scans clean — the gap is tolerated, nothing is reported missing.
#[tokio::test]
async fn test_scan_pubnet_fixture_tolerates_missing_early_scp() {
    run_scan(
        ScanConfig::new(fixture_url())
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect("pubnet: missing early-gap scp must be tolerated");
}

/// Same real data, but presented as testnet: the gap tolerance is pubnet-gated,
/// so the genuinely-absent early scp files are now flagged as missing. This is
/// the direct proof that the gating (not some other quirk of the data) is what
/// makes the pubnet scan pass.
#[tokio::test]
async fn test_scan_non_pubnet_fixture_flags_missing_early_scp() {
    let dir = TempDir::new().unwrap();
    copy_pubnet_scp_boundary_archive(dir.path()).unwrap();
    set_network_passphrase(dir.path(), TESTNET_PASSPHRASE);

    run_scan(
        ScanConfig::new(file_url_from_path(dir.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect_err("non-pubnet: the absent early scp files must be flagged as missing");
}

/// Hash verification over the real slice also tolerates the gap and confirms the
/// bucket/xdr content is intact.
#[tokio::test]
async fn test_scan_verify_pubnet_fixture_tolerates_missing_early_scp() {
    run_scan(
        ScanConfig::new(fixture_url())
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH)
            .verify(),
    )
    .await
    .expect("pubnet --verify: gap tolerated and content verifies");
}

/// Tolerance is bounded to the gap: an scp missing at `FIRST_SCP_CHECKPOINT`
/// itself — the first checkpoint pubnet archives an scp for — is still a real
/// error.
#[tokio::test]
async fn test_scan_pubnet_fixture_flags_missing_scp_at_boundary() {
    let dir = TempDir::new().unwrap();
    copy_pubnet_scp_boundary_archive(dir.path()).unwrap();
    delete_checkpoint_file(dir.path(), "scp", PUBNET_SCP_BOUNDARY_AT_CP);

    run_scan(
        ScanConfig::new(file_url_from_path(dir.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect_err("pubnet: a missing scp at the gap boundary must still be flagged");
}

/// The gap only excuses SCP: a missing *required* file (here a ledger) at a
/// below-boundary checkpoint is still flagged.
#[tokio::test]
async fn test_scan_pubnet_fixture_flags_missing_required_file_in_gap() {
    let dir = TempDir::new().unwrap();
    copy_pubnet_scp_boundary_archive(dir.path()).unwrap();
    delete_checkpoint_file(dir.path(), "ledger", PUBNET_SCP_BOUNDARY_BELOW_CP);

    run_scan(
        ScanConfig::new(file_url_from_path(dir.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect_err("pubnet: a missing required file in the gap range must still be flagged");
}

//=============================================================================
// Mirror
//=============================================================================

/// Mirroring the real slice succeeds, and the destination faithfully reproduces
/// the gap: no scp below the boundary (never scheduled), scp present above it.
#[tokio::test]
async fn test_mirror_pubnet_fixture_tolerates_and_reproduces_gap() {
    let dst = TempDir::new().unwrap();

    run_mirror(
        MirrorConfig::new(fixture_url(), file_url_from_path(dst.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect("pubnet mirror must tolerate the early-scp gap");

    // Only the four at/above-boundary scp files should exist in the mirror.
    let scp_files = get_files_by_pattern(dst.path(), "/scp-");
    assert_eq!(
        scp_files.len(),
        4,
        "mirror should contain only the 4 at/above-boundary scp files, got {scp_files:?}"
    );
    assert!(
        get_files_by_pattern(
            dst.path(),
            &format!("/scp-{PUBNET_SCP_BOUNDARY_BELOW_CP:08x}")
        )
        .is_empty(),
        "mirror must not contain a below-boundary scp"
    );
    assert_eq!(
        get_files_by_pattern(dst.path(), &format!("/scp-{PUBNET_SCP_BOUNDARY_AT_CP:08x}")).len(),
        1,
        "mirror must contain the boundary scp"
    );
}

/// Roundtrip: mirroring stamps the source's network passphrase into the
/// destination `.well-known` (per-checkpoint history files omit it), so the
/// *mirror itself* is still recognised as pubnet and re-scans clean. Without
/// that stamping the mirror's `.well-known` would lack the passphrase, the scan
/// would not detect pubnet, and the genuinely-absent early scp would be flagged.
#[tokio::test]
async fn test_mirror_preserves_pubnet_identity_for_rescan() {
    let dst = TempDir::new().unwrap();
    run_mirror(
        MirrorConfig::new(fixture_url(), file_url_from_path(dst.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect("pubnet mirror should succeed");

    // The mirror's root .well-known carries the pubnet passphrase.
    let wk = std::fs::read_to_string(dst.path().join(crate::history_format::ROOT_WELL_KNOWN_PATH))
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&wk).unwrap();
    assert_eq!(
        parsed["networkPassphrase"].as_str(),
        Some(crate::history_format::PUBLIC_NETWORK_PASSPHRASE),
        "mirror must stamp the pubnet passphrase into the destination .well-known"
    );

    // ...so scanning the mirror still tolerates the early-scp gap.
    run_scan(
        ScanConfig::new(file_url_from_path(dst.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect("re-scanning the mirror must still tolerate the gap (pubnet identity preserved)");
}

//=============================================================================
// Repair
//=============================================================================

/// Repairing a pubnet destination restores a genuinely-missing required file
/// while leaving the early-gap scp alone (it is never a repair action, and the
/// source has none to fetch). A follow-up scan then passes.
#[tokio::test]
async fn test_repair_pubnet_fixture_restores_required_file_and_skips_gap_scp() {
    let dst = TempDir::new().unwrap();
    copy_pubnet_scp_boundary_archive(dst.path()).unwrap();
    // Break a required file in the gap range; the gap scp stays absent (as on pubnet).
    delete_checkpoint_file(dst.path(), "ledger", PUBNET_SCP_BOUNDARY_BELOW_CP);

    run_repair(
        RepairConfig::new(fixture_url(), file_url_from_path(dst.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect("pubnet repair should restore the required file and tolerate the gap");

    // The required file is back; no below-boundary scp was fabricated.
    assert_eq!(
        get_files_by_pattern(
            dst.path(),
            &format!("/ledger-{PUBNET_SCP_BOUNDARY_BELOW_CP:08x}")
        )
        .len(),
        1,
        "repair must restore the deleted ledger"
    );
    assert!(
        get_files_by_pattern(
            dst.path(),
            &format!("/scp-{PUBNET_SCP_BOUNDARY_BELOW_CP:08x}")
        )
        .is_empty(),
        "repair must not fetch/create a below-boundary scp"
    );

    // And the repaired archive scans clean under pubnet tolerance.
    run_scan(
        ScanConfig::new(file_url_from_path(dst.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH),
    )
    .await
    .expect("repaired pubnet archive should scan clean");
}

/// Dry-run repair: the plan flags the genuinely-missing required file but never
/// lists an scp for a below-boundary checkpoint.
#[tokio::test]
async fn test_repair_dry_run_pubnet_fixture_plan_omits_gap_scp() {
    let dst = TempDir::new().unwrap();
    copy_pubnet_scp_boundary_archive(dst.path()).unwrap();
    delete_checkpoint_file(dst.path(), "ledger", PUBNET_SCP_BOUNDARY_BELOW_CP);

    let plan_path = dst.path().join("plan.json");
    run_repair(
        RepairConfig::new(fixture_url(), file_url_from_path(dst.path()))
            .low(PUBNET_SCP_BOUNDARY_LOW)
            .high(PUBNET_SCP_BOUNDARY_HIGH)
            .dry_run()
            .report(&plan_path),
    )
    .await
    .expect("dry-run repair should succeed");

    let plan = crate::report::read_from_path(&plan_path).unwrap();
    let below = PUBNET_SCP_BOUNDARY_BELOW_CP.to_string();

    // The deleted ledger is flagged...
    assert!(
        plan.section
            .files
            .get(&below)
            .is_some_and(|types| types.iter().any(|t| t == "ledger")),
        "dry-run plan must flag the deleted required ledger: {:?}",
        plan.section.files
    );
    // ...but no scp is ever listed for any below-boundary checkpoint.
    assert!(
        plan.section
            .files
            .values()
            .all(|types| !types.iter().any(|t| t == "scp")),
        "dry-run plan must not list any gap scp: {:?}",
        plan.section.files
    );
}
