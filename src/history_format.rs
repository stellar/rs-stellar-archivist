//! Stellar history file format handling.
//!
//! This module provides types and functions for working with Stellar
//! Archive history files.

use serde::{Deserialize, Serialize};
use thiserror::Error;

// Both Bucket List types have exactly 11 levels
const NUM_BUCKETLIST_LEVELS: usize = 11;
// Checkpoints occur every 64 ledgers
pub const CHECKPOINT_FREQUENCY: u32 = 64;
// The first checkpoint ledger
pub const GENESIS_CHECKPOINT_LEDGER: u32 = CHECKPOINT_FREQUENCY - 1;

// Standard location for .well-known file in archives
pub const ROOT_WELL_KNOWN_PATH: &str = ".well-known/stellar-history.json";

/// Errors that can occur when working with history files
#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid version: expected 1 or 2, got {version}")]
    InvalidVersion { version: i32 },

    #[error("Version {version} .well-known must {requirement} hotArchiveBuckets field")]
    MissingHotBuckets { version: i32, requirement: String },

    #[error("Invalid currentLedger: {reason}")]
    InvalidCurrentLedger { reason: String },

    #[error("Malformed bucket hash: {reason}")]
    MalformedBucketHash { reason: String },

    #[error("{prefix} level {level} next state is {state} but {issue}")]
    InvalidNextState {
        prefix: String,
        level: usize,
        state: u32,
        issue: String,
    },

    #[error("Invalid next state value at level {level}: {state}")]
    InvalidNextStateValue { level: usize, state: u32 },

    #[error("Invalid JSON in {path}: {error}")]
    InvalidJson { path: String, error: String },
}

// Root .well-known structure describing archive state at a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryFileState {
    pub version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    pub current_ledger: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_passphrase: Option<String>,
    pub current_buckets: [BucketLevel; NUM_BUCKETLIST_LEVELS],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_archive_buckets: Option<[BucketLevel; NUM_BUCKETLIST_LEVELS]>,
}

// Bucket state for a single bucketlist level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketLevel {
    pub curr: String,
    pub snap: String,
    pub next: NextState,
}

// Merge state for bucket level
// state: 0=idle, 1=running (output), 2=merging (curr/snap/shadow)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextState {
    pub state: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub curr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snap: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shadow: Option<Vec<String>>,
}

impl HistoryFileState {
    // Helper function to validate a single bucket level
    fn validate_bucket_level(level: &BucketLevel, index: usize, prefix: &str) -> Result<(), Error> {
        if !is_valid_bucket_hash(&level.curr) {
            return Err(Error::MalformedBucketHash {
                reason: format!("invalid hash: {}", level.curr),
            });
        }

        if !is_valid_bucket_hash(&level.snap) {
            return Err(Error::MalformedBucketHash {
                reason: format!("invalid hash: {}", level.snap),
            });
        }

        // Validate next state
        match level.next.state {
            0 => {
                if level.next.output.is_some() {
                    return Err(Error::InvalidNextState {
                        prefix: prefix.to_string(),
                        level: index,
                        state: 0,
                        issue: "has output field".to_string(),
                    });
                }
                if level.next.curr.is_some()
                    || level.next.snap.is_some()
                    || level.next.shadow.is_some()
                {
                    return Err(Error::InvalidNextState {
                        prefix: prefix.to_string(),
                        level: index,
                        state: 0,
                        issue: "has merge fields".to_string(),
                    });
                }
            }
            1 => {
                if level.next.output.is_none() {
                    return Err(Error::InvalidNextState {
                        prefix: prefix.to_string(),
                        level: index,
                        state: 1,
                        issue: "missing output field".to_string(),
                    });
                } else if let Some(ref output) = level.next.output {
                    if !is_valid_bucket_hash(output) {
                        return Err(Error::MalformedBucketHash {
                            reason: format!("invalid hash: {}", output),
                        });
                    }
                }
                if level.next.curr.is_some()
                    || level.next.snap.is_some()
                    || level.next.shadow.is_some()
                {
                    return Err(Error::InvalidNextState {
                        prefix: prefix.to_string(),
                        level: index,
                        state: 1,
                        issue: "has merge fields".to_string(),
                    });
                }
            }
            2 => {
                if level.next.curr.is_none() {
                    return Err(Error::InvalidNextState {
                        prefix: prefix.to_string(),
                        level: index,
                        state: 2,
                        issue: "missing curr field".to_string(),
                    });
                } else if let Some(ref curr) = level.next.curr {
                    if !is_valid_bucket_hash(curr) {
                        return Err(Error::MalformedBucketHash {
                            reason: format!("invalid hash: {}", curr),
                        });
                    }
                }

                if level.next.snap.is_none() {
                    return Err(Error::InvalidNextState {
                        prefix: prefix.to_string(),
                        level: index,
                        state: 2,
                        issue: "missing snap field".to_string(),
                    });
                } else if let Some(ref snap) = level.next.snap {
                    if !is_valid_bucket_hash(snap) {
                        return Err(Error::MalformedBucketHash {
                            reason: format!("invalid hash: {}", snap),
                        });
                    }
                }

                if let Some(ref shadow) = level.next.shadow {
                    for hash in shadow.iter() {
                        if !is_valid_bucket_hash(hash) {
                            return Err(Error::MalformedBucketHash {
                                reason: format!("invalid hash: {}", hash),
                            });
                        }
                    }
                }
            }
            _ => {
                return Err(Error::InvalidNextStateValue {
                    level: index,
                    state: level.next.state,
                });
            }
        }

        Ok(())
    }

    // Validate .well-known format for basic structural/string correctness
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != 1 && self.version != 2 {
            return Err(Error::InvalidVersion {
                version: self.version,
            });
        }

        // Version 2 must have hotArchiveBuckets, version 1 must not
        if self.version == 2 && self.hot_archive_buckets.is_none() {
            return Err(Error::MissingHotBuckets {
                version: 2,
                requirement: "have".to_string(),
            });
        }
        if self.version == 1 && self.hot_archive_buckets.is_some() {
            return Err(Error::MissingHotBuckets {
                version: 1,
                requirement: "not have".to_string(),
            });
        }

        if self.current_ledger == 0 {
            return Err(Error::InvalidCurrentLedger {
                reason: "cannot be 0".to_string(),
            });
        } else if !is_checkpoint(self.current_ledger) {
            return Err(Error::InvalidCurrentLedger {
                reason: format!(
                    "{} is not on a 64-ledger checkpoint boundary",
                    self.current_ledger
                ),
            });
        }

        // Validate each bucket level
        for (i, level) in self.current_buckets.iter().enumerate() {
            Self::validate_bucket_level(level, i, "Live")?;
        }

        // If version 2, also validate hotArchiveBuckets
        if let Some(ref hot_buckets) = self.hot_archive_buckets {
            for (i, level) in hot_buckets.iter().enumerate() {
                Self::validate_bucket_level(level, i, "HotArchive")?;
            }
        }

        Ok(())
    }

    // Extract all unique bucket hashes from current state, except for empty buckets with 0 hash
    pub fn buckets(&self) -> Vec<String> {
        let mut result = Vec::new();

        // Collect from currentBuckets
        for level in &self.current_buckets {
            if !level.curr.is_empty() && !is_zero_hash(&level.curr) {
                result.push(level.curr.clone());
            }
            if !level.snap.is_empty() && !is_zero_hash(&level.snap) {
                result.push(level.snap.clone());
            }
            if let Some(output) = &level.next.output {
                if !output.is_empty() && !is_zero_hash(output) {
                    result.push(output.clone());
                }
            }
        }

        // Collect from hotArchiveBuckets if present (version 2)
        if let Some(ref hot_buckets) = self.hot_archive_buckets {
            for level in hot_buckets {
                if !level.curr.is_empty() && !is_zero_hash(&level.curr) {
                    result.push(level.curr.clone());
                }
                if !level.snap.is_empty() && !is_zero_hash(&level.snap) {
                    result.push(level.snap.clone());
                }
                if let Some(output) = &level.next.output {
                    if !output.is_empty() && !is_zero_hash(output) {
                        result.push(output.clone());
                    }
                }
            }
        }

        result.sort();
        result.dedup();
        result
    }

    // Get all checkpoint ledger numbers up to current_ledger
    pub fn get_checkpoint_range(&self) -> Vec<u32> {
        // For very large ranges, warn about memory usage
        let num_checkpoints = (self.current_ledger / CHECKPOINT_FREQUENCY) as usize;
        if num_checkpoints > 10000 {
            tracing::warn!(
                "Generating {} checkpoints up to ledger {} - this may use significant memory",
                num_checkpoints,
                self.current_ledger
            );
        }

        let mut checkpoints = Vec::with_capacity(num_checkpoints);
        let mut checkpoint = GENESIS_CHECKPOINT_LEDGER; // Start at first checkpoint (63)

        while checkpoint <= self.current_ledger {
            checkpoints.push(checkpoint);
            checkpoint += CHECKPOINT_FREQUENCY;
        }

        checkpoints
    }
}

// Check if ledger is a checkpoint (63, 127, 191, ...)
pub fn is_checkpoint(ledger: u32) -> bool {
    ledger > 0 && (ledger + 1) % CHECKPOINT_FREQUENCY == 0
}

// Validate bucket hash format (64 hex chars)
pub fn is_valid_bucket_hash(hash: &str) -> bool {
    if hash.len() != 64 {
        return false;
    }

    hash.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn is_zero_hash(hash: &str) -> bool {
    hash == "0000000000000000000000000000000000000000000000000000000000000000"
}

/// Helper function to round to checkpoint boundary
/// If round_up is true, rounds up; otherwise rounds down
/// If the ledger is already a checkpoint, returns it unchanged
/// The minimum returned value is GENESIS_CHECKPOINT_LEDGER (63)
fn round_to_checkpoint(ledger: u32, round_up: bool) -> u32 {
    if ledger < GENESIS_CHECKPOINT_LEDGER {
        GENESIS_CHECKPOINT_LEDGER
    } else if is_checkpoint(ledger) {
        // Already a checkpoint
        ledger
    } else {
        // Calculate the checkpoint number (0-based from genesis)
        let checkpoint_num = (ledger + 1) / CHECKPOINT_FREQUENCY;
        // Round up or down as requested
        let final_num = if round_up {
            checkpoint_num + 1
        } else {
            checkpoint_num
        };
        final_num * CHECKPOINT_FREQUENCY - 1
    }
}

pub fn round_to_lower_checkpoint(ledger: u32) -> u32 {
    round_to_checkpoint(ledger, false)
}

pub fn round_to_upper_checkpoint(ledger: u32) -> u32 {
    round_to_checkpoint(ledger, true)
}

/// Count the number of checkpoints in the inclusive range [low_checkpoint, high_checkpoint]
/// Both parameters should already be valid checkpoint values
pub fn count_checkpoints_in_range(low_checkpoint: u32, high_checkpoint: u32) -> usize {
    assert!(
        is_checkpoint(low_checkpoint),
        "low_checkpoint {} is not a valid checkpoint",
        low_checkpoint
    );
    assert!(
        is_checkpoint(high_checkpoint),
        "high_checkpoint {} is not a valid checkpoint",
        high_checkpoint
    );

    if high_checkpoint < low_checkpoint {
        0
    } else {
        ((high_checkpoint - low_checkpoint) / CHECKPOINT_FREQUENCY + 1) as usize
    }
}

// Convert checkpoint to hex path prefix (e.g., "00/00/3f")
pub fn checkpoint_prefix(checkpoint: u32) -> String {
    format!(
        "{:02x}/{:02x}/{:02x}",
        (checkpoint >> 24) & 0xff,
        (checkpoint >> 16) & 0xff,
        (checkpoint >> 8) & 0xff
    )
}

// Convert first 6 hex chars of hash to directory path (e.g., "abcdef..." -> "ab/cd/ef")
pub fn hash_prefix(hash: &str) -> Result<String, Error> {
    if hash.len() < 6 {
        return Err(Error::MalformedBucketHash {
            reason: format!("hash too short: '{}'", hash),
        });
    }
    Ok(format!("{}/{}/{}", &hash[0..2], &hash[2..4], &hash[4..6]))
}

// Generate archive path for given bucket hash
pub fn bucket_path(hash: &str) -> Result<String, Error> {
    let prefix = hash_prefix(hash)?;
    Ok(format!("bucket/{}/bucket-{}.xdr.gz", prefix, hash))
}

/// Extract checkpoint number from a checkpoint filename like "history-0000137f.json"
pub fn checkpoint_from_filename(filename: &str) -> Option<u32> {
    let hex_start = filename.rfind('-')?;
    let hex_part = &filename[hex_start + 1..];
    let hex_part = hex_part
        .trim_end_matches(".xdr.gz")
        .trim_end_matches(".json");
    u32::from_str_radix(hex_part, 16).ok()
}

/// Extract bucket hash from a bucket filename like "bucket-b5c7cd74192aa750429bfaa8cde27a41a729643a194fb474be765da982ece22a.xdr.gz"
pub fn bucket_hash_from_filename(filename: &str) -> Option<String> {
    if !filename.starts_with("bucket-") {
        return None;
    }
    let hash_start = filename.find('-')? + 1;
    let hash_end = filename.find(".xdr.gz")?;
    let hash = &filename[hash_start..hash_end];

    // Validate it's a proper bucket hash (64 hex chars)
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hash.to_string())
    } else {
        None
    }
}

// Generate archive path for checkpoint file using string category (for backwards compatibility)
pub fn checkpoint_path(category: &str, checkpoint: u32) -> String {
    // All files are compressed except history archive files.
    let ext = match category {
        "history" => "json",
        _ => "xdr.gz",
    };
    let prefix = checkpoint_prefix(checkpoint);
    format!(
        "{}/{}/{}-{:08x}.{}",
        category, prefix, category, checkpoint, ext
    )
}
