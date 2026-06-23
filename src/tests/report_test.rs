//! Tests for the JSON plan/report schema (`crate::report`).

use crate::report::{ArchiveReport, Summary, REPORT_VERSION};
use crate::utils::{FailureTracker, FileFlags};
use stellar_xdr::curr::Hash;

fn sample_tracker() -> FailureTracker {
    let mut t = FailureTracker::default();
    t.record_file(127, FileFlags::LEDGER);
    t.record_file(127, FileFlags::TRANSACTIONS);
    t.record_file(255, FileFlags::HISTORY);
    t.record_bucket(Hash([0xab; 32]));
    t.record_checkpoint(191);
    t.record_well_known(255);
    t
}

#[test]
fn test_from_failures_projects_all_kinds() {
    let report = ArchiveReport::from_failures_and_summary(&sample_tracker(), Summary::default());
    assert_eq!(report.version, REPORT_VERSION);
    assert_eq!(report.section.well_known, Some(255));
    assert_eq!(
        report.section.files.get("127").unwrap(),
        &["ledger", "transactions"]
    );
    assert_eq!(report.section.files.get("255").unwrap(), &["history"]);
    assert_eq!(report.section.buckets, vec!["ab".repeat(32)]);
    assert_eq!(report.section.checkpoints, vec![191]);
}

#[test]
fn test_summary_always_serialized() {
    let report = ArchiveReport::from_failures_and_summary(
        &FailureTracker::default(),
        Summary {
            succeeded: 5,
            skipped: 1,
            failed: 0,
            retries: 2,
        },
    );
    let json = serde_json::to_string(&report).unwrap();
    assert!(
        json.contains("\"summary\""),
        "summary should always serialize: {json}"
    );
}

#[test]
fn test_round_trip_identity() {
    let t = sample_tracker();
    let back = ArchiveReport::from_failures_and_summary(&t, Summary::default())
        .into_failures()
        .unwrap();
    assert_eq!(back.well_known, t.well_known);
    assert_eq!(back.files, t.files);
    assert_eq!(back.buckets, t.buckets);
    assert_eq!(back.checkpoints, t.checkpoints);
}

#[test]
fn test_into_failures_rejects_bad_version() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.version = 999;
    assert!(r.into_failures().is_err());
}

#[test]
fn test_into_failures_rejects_non_boundary_checkpoint_key() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.section
        .files
        .insert("100".to_string(), vec!["ledger".to_string()]);
    let err = r.into_failures().unwrap_err().to_string();
    assert!(err.contains("checkpoint"), "got: {err}");
}

#[test]
fn test_into_failures_rejects_unknown_file_type() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.section
        .files
        .insert("127".to_string(), vec!["bogus".to_string()]);
    let err = r.into_failures().unwrap_err().to_string();
    assert!(
        err.contains("bogus") || err.contains("file type"),
        "got: {err}"
    );
}

#[test]
fn test_into_failures_rejects_bad_bucket_hash() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.section.buckets.push("not-hex".to_string());
    assert!(r.into_failures().is_err());
}

#[test]
fn test_into_failures_rejects_non_boundary_checkpoint_entry() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.section.checkpoints.push(100);
    assert!(r.into_failures().is_err());
}

#[test]
fn test_into_failures_ignores_summary() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.section.summary = Summary {
        succeeded: 9,
        skipped: 9,
        failed: 9,
        retries: 9,
    };
    assert!(r.into_failures().unwrap().is_empty());
}

#[test]
fn test_write_then_read_round_trips_via_disk() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("nested").join("plan.json");
    let report = ArchiveReport::from_failures_and_summary(&sample_tracker(), Summary::default());
    crate::report::write_to_path(&path, &report).unwrap();
    assert!(path.exists(), "parent dirs should be created");
    let read = crate::report::read_from_path(&path).unwrap();
    assert_eq!(read, report);
}

#[test]
fn test_section_computes_failed_and_summary() {
    let section = crate::report::section(&sample_tracker(), 100, 3, 7);
    let s = section.summary;
    assert_eq!(s.succeeded, 100);
    assert_eq!(s.skipped, 3);
    assert_eq!(s.retries, 7);
    // sample_tracker has 3 file failures + 1 bucket = 4 (checkpoints excluded)
    assert_eq!(s.failed, 4);
}

#[test]
fn test_record_well_known_stores_checkpoint() {
    let mut t = FailureTracker::default();
    assert!(t.is_empty());
    t.record_well_known(127);
    assert_eq!(t.well_known, Some(127));
    assert!(!t.is_empty());
    t.unrecord_well_known();
    assert_eq!(t.well_known, None);
    assert!(t.is_empty());
}

#[tokio::test]
async fn test_record_failure_does_not_touch_well_known() {
    // The root .well-known is no longer routed through record_failure; it is
    // recorded explicitly via FailureTracker::record_well_known with the
    // archive's high checkpoint. record_failure on that path is a no-op.
    let stats = crate::utils::ArchiveStats::new();
    stats
        .record_failure(255, crate::history_format::ROOT_WELL_KNOWN_PATH)
        .await;
    assert_eq!(stats.failures.lock().await.well_known, None);
}

#[test]
fn test_well_known_checkpoint_round_trips() {
    for wk in [Some(191u32), None] {
        let mut t = FailureTracker::default();
        if let Some(cp) = wk {
            t.record_well_known(cp);
        }
        let back = ArchiveReport::from_failures_and_summary(&t, Summary::default())
            .into_failures()
            .unwrap();
        assert_eq!(back.well_known, wk);
    }
}

#[test]
fn test_into_failures_rejects_non_boundary_well_known() {
    let mut r =
        ArchiveReport::from_failures_and_summary(&FailureTracker::default(), Summary::default());
    r.section.well_known = Some(100); // 100 is not a checkpoint boundary
    let err = r.into_failures().unwrap_err().to_string();
    assert!(err.contains("checkpoint"), "got: {err}");
}

#[test]
fn test_well_known_serializes_as_number_or_null() {
    let mut t = FailureTracker::default();
    t.record_well_known(127);
    let broken = serde_json::to_string(&ArchiveReport::from_failures_and_summary(
        &t,
        Summary::default(),
    ))
    .unwrap();
    assert!(broken.contains("\"well_known\":127"), "got: {broken}");

    let healthy = serde_json::to_string(&ArchiveReport::from_failures_and_summary(
        &FailureTracker::default(),
        Summary::default(),
    ))
    .unwrap();
    assert!(healthy.contains("\"well_known\":null"), "got: {healthy}");
}

/// A scan/mirror-shaped report (well_known + files + buckets + checkpoints)
/// is a valid repair plan: into_failures accepts it and reconstructs the
/// tracker. Guards cross-operation compatibility.
#[test]
fn test_scan_shaped_report_is_valid_plan() {
    let report = ArchiveReport::from_failures_and_summary(&sample_tracker(), Summary::default());
    let json = serde_json::to_string(&report).unwrap();
    let parsed: ArchiveReport = serde_json::from_str(&json).unwrap();
    let tracker = parsed
        .into_failures()
        .expect("scan/mirror report must be a valid plan");
    // FailureTracker is not PartialEq; compare fields (as test_round_trip_identity does).
    let expected = sample_tracker();
    assert_eq!(tracker.well_known, expected.well_known);
    assert_eq!(tracker.files, expected.files);
    assert_eq!(tracker.buckets, expected.buckets);
    assert_eq!(tracker.checkpoints, expected.checkpoints);
}
