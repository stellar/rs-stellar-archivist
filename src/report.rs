//! JSON plan/report schema for repair and status output.
//!
//! `ArchiveReport` is a versioned, deterministic serialization of an
//! operation's `FailureTracker` plus an outcome `Summary`. It is the single
//! shared schema for scan/mirror/repair-dry-run output and repair `--plan`
//! input, so any `--report` can be replayed as a plan. On apply, every listed
//! item is re-fetched from src; the CLI `--verify` works exactly as in regular
//! repair (it governs validation of the src content and the dst probing of
//! discovered bucket files). It serves three roles: repair dry-run output (a
//! "plan"), repair input (`--plan`), and status output for any operation
//! (`--report`). It round-trips: `into_failures` is the inverse of the
//! `from_failures_and_summary` projection (modulo `summary`/`version`).
//!
//! Multi-stage runs (non-dry-run repair) use `MultiSectionReport` — one labeled
//! `ReportSection` per stage (`main_pass` / `file_retry` / `checkpoint_retry`).

use crate::history_format::{is_checkpoint, is_valid_bucket_hash};
use crate::utils::{hex_to_hash, FailureTracker, FileFlags};
use crate::xdr_verify::HashExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use stellar_xdr::curr::Hash;
use thiserror::Error;

/// Current schema version.
pub const REPORT_VERSION: u32 = 1;

/// Upper bound on total plan entries (files + buckets + checkpoints) accepted
/// from an external `--plan`. Guards against accidental/malicious oversized input.
pub const MAX_PLAN_ENTRIES: usize = 10_000_000;

/// Canonical (flag, name) table — also fixes the serialization order of a
/// checkpoint's file-type list.
const FLAG_NAMES: &[(u8, &str)] = &[
    (FileFlags::HISTORY, "history"),
    (FileFlags::LEDGER, "ledger"),
    (FileFlags::TRANSACTIONS, "transactions"),
    (FileFlags::RESULTS, "results"),
    (FileFlags::SCP, "scp"),
];

#[derive(Debug, Error)]
pub enum ReportError {
    #[error("invalid plan: {0}")]
    Invalid(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub succeeded: u64,
    pub skipped: u64,
    pub failed: u64,
    pub retries: u64,
}

/// The body of a report: a `FailureTracker` projection plus outcome counters.
/// Used both as the flattened body of a single-section [`ArchiveReport`] and as
/// one named section of a [`MultiSectionReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportSection {
    /// `Some(cp)` if the root `.well-known` is broken, carrying the checkpoint
    /// to restore it from (the archive's highest checkpoint); `null` if healthy.
    pub well_known: Option<u32>,
    /// checkpoint (decimal string) -> per-cp file type names
    pub files: BTreeMap<String, Vec<String>>,
    /// 64-hex bucket content hashes
    pub buckets: Vec<String>,
    /// checkpoints with cross-file / chain failures
    pub checkpoints: Vec<u32>,
    /// Outcome counters. Always serialized; `#[serde(default)]` lets a
    /// hand-authored plan omit it (`into_failures` ignores it anyway).
    #[serde(default)]
    pub summary: Summary,
}

/// A single-section report (scan/mirror status, or a repair plan). The body is
/// `#[serde(flatten)]`ed so the JSON stays flat: `{version, well_known, …}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveReport {
    pub version: u32,
    #[serde(flatten)]
    pub section: ReportSection,
}

fn flags_to_names(flags: FileFlags) -> Vec<String> {
    FLAG_NAMES
        .iter()
        .filter(|(bit, _)| flags.has(*bit))
        .map(|(_, name)| (*name).to_string())
        .collect()
}

fn name_to_flag(name: &str) -> Option<u8> {
    FLAG_NAMES
        .iter()
        .find(|(_, n)| *n == name)
        .map(|(bit, _)| *bit)
}

fn validate_checkpoint(cp: u32) -> Result<(), ReportError> {
    if is_checkpoint(cp) {
        Ok(())
    } else {
        Err(ReportError::Invalid(format!(
            "{cp} is not a valid checkpoint boundary"
        )))
    }
}

fn parse_bucket_hash(s: &str) -> Result<Hash, ReportError> {
    if !is_valid_bucket_hash(s) {
        return Err(ReportError::Invalid(format!(
            "bucket hash '{s}' must be 64 hex characters"
        )));
    }
    hex_to_hash(s)
        .ok_or_else(|| ReportError::Invalid(format!("bucket hash '{s}' is not valid hex")))
}

/// Project a `FailureTracker` plus an outcome `Summary` into a report body.
/// Ordering is deterministic: BTree order for `files`/`checkpoints`/`buckets`,
/// fixed flag order within each file list. The single home for the projection,
/// shared by [`ArchiveReport::from_failures_and_summary`] and [`section`].
fn project(failures: &FailureTracker, summary: Summary) -> ReportSection {
    ReportSection {
        well_known: failures.well_known,
        files: failures
            .files
            .iter()
            .map(|(cp, flags)| (cp.to_string(), flags_to_names(*flags)))
            .collect(),
        buckets: failures.buckets.iter().map(HashExt::to_hex).collect(),
        checkpoints: failures.checkpoints.iter().copied().collect(),
        summary,
    }
}

impl ArchiveReport {
    /// Build a single-section report from a `FailureTracker` plus a `Summary`.
    #[must_use]
    pub fn from_failures_and_summary(failures: &FailureTracker, summary: Summary) -> Self {
        Self {
            version: REPORT_VERSION,
            section: project(failures, summary),
        }
    }

    /// Validate and convert into a `FailureTracker`. Inverse of the `project`
    /// projection (the summary is ignored). Rejects unsupported versions,
    /// non-boundary checkpoints, unknown file types, malformed bucket hashes,
    /// and oversized plans.
    pub fn into_failures(self) -> Result<FailureTracker, ReportError> {
        if self.version != REPORT_VERSION {
            return Err(ReportError::Invalid(format!(
                "unsupported report version {} (expected {REPORT_VERSION})",
                self.version
            )));
        }
        let body = self.section;
        let total = body.files.values().map(Vec::len).sum::<usize>()
            + body.buckets.len()
            + body.checkpoints.len();
        if total > MAX_PLAN_ENTRIES {
            return Err(ReportError::Invalid(format!(
                "plan has {total} entries, exceeds maximum {MAX_PLAN_ENTRIES}"
            )));
        }

        if let Some(cp) = body.well_known {
            validate_checkpoint(cp)?;
        }
        let mut tracker = FailureTracker {
            well_known: body.well_known,
            ..FailureTracker::default()
        };
        for (cp_str, names) in &body.files {
            let cp: u32 = cp_str
                .parse()
                .map_err(|_| ReportError::Invalid(format!("invalid checkpoint key '{cp_str}'")))?;
            validate_checkpoint(cp)?;
            for name in names {
                let flag = name_to_flag(name)
                    .ok_or_else(|| ReportError::Invalid(format!("unknown file type '{name}'")))?;
                tracker.record_file(cp, flag);
            }
        }
        for &cp in &body.checkpoints {
            validate_checkpoint(cp)?;
            tracker.record_checkpoint(cp);
        }
        for bucket in &body.buckets {
            tracker.record_bucket(parse_bucket_hash(bucket)?);
        }
        Ok(tracker)
    }
}

/// Outcome counters derived from a stats' failures + the given totals. `failed`
/// = file failures + bucket failures (checkpoint failures are reported via the
/// `checkpoints` body, not folded into `failed`).
fn summary_of(failures: &FailureTracker, succeeded: u64, skipped: u64, retries: u64) -> Summary {
    Summary {
        succeeded,
        skipped,
        retries,
        failed: u64::from(failures.total_file_failures()) + failures.buckets.len() as u64,
    }
}

/// A multi-section report: named sections (e.g. repair's `main_pass` /
/// `file_retry` / `checkpoint_retry`) in one file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiSectionReport {
    pub version: u32,
    pub sections: BTreeMap<String, ReportSection>,
}

/// Build one section from a stats' failures + counters (same projection as a
/// single-section report's body).
#[must_use]
pub fn section(
    failures: &FailureTracker,
    succeeded: u64,
    skipped: u64,
    retries: u64,
) -> ReportSection {
    project(failures, summary_of(failures, succeeded, skipped, retries))
}

/// Read and deserialize a single-section report from a local path.
pub fn read_from_path(path: &Path) -> Result<ArchiveReport, ReportError> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

/// Write a report value to a local path as pretty JSON, creating parent
/// directories as needed. Generic so it serves both the single-section
/// [`ArchiveReport`] and the multi-section [`MultiSectionReport`].
pub fn write_to_path<T: Serialize>(path: &Path, report: &T) -> Result<(), ReportError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let json = serde_json::to_string_pretty(report)?;
    std::fs::write(path, json)?;
    Ok(())
}
