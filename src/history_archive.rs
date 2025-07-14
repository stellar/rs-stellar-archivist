//! Stellar History Archive State (HAS) file format handling.
//!
//! This module provides types and functions for working with Stellar's
//! History Archive State files.

use serde::{Deserialize, Serialize};

// Stellar bucketlist has exactly 11 levels
const NUM_BUCKETLIST_LEVELS: usize = 11;
// Checkpoints occur every 64 ledgers
const CHECKPOINT_FREQUENCY: u32 = 64;
// Standard location for HAS file in archives
pub const ROOT_HAS_PATH: &str = ".well-known/stellar-history.json";

// Root HAS structure describing archive state at a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryArchiveState {
    pub version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(rename = "currentLedger")]
    pub current_ledger: u32,
    #[serde(rename = "networkPassphrase", skip_serializing_if = "Option::is_none")]
    pub network_passphrase: Option<String>,
    #[serde(rename = "currentBuckets")]
    pub current_buckets: [BucketLevel; NUM_BUCKETLIST_LEVELS],
    #[serde(rename = "hotArchiveBuckets", skip_serializing_if = "Option::is_none")]
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

impl HistoryArchiveState {
    // Helper function to validate a single bucket level
    fn validate_bucket_level(level: &BucketLevel, index: usize, prefix: &str) -> Result<(), String> {
        if !is_valid_bucket_hash(&level.curr) {
            return Err(format!("invalid bucket hash {}", level.curr));
        }
        
        if !is_valid_bucket_hash(&level.snap) {
            return Err(format!("invalid bucket hash {}", level.snap));
        }
        
        // Validate next state
        match level.next.state {
            0 => {
                if level.next.output.is_some() {
                    return Err(format!(
                        "{} level {} next state is 0 but has output field",
                        prefix, index
                    ));
                }
                if level.next.curr.is_some()
                    || level.next.snap.is_some()
                    || level.next.shadow.is_some()
                {
                    return Err(format!(
                        "{} level {} next state is 0 but has merge fields",
                        prefix, index
                    ));
                }
            }
            1 => {
                if level.next.output.is_none() {
                    return Err(format!(
                        "{} level {} next state is 1 but missing output field",
                        prefix, index
                    ));
                } else if let Some(ref output) = level.next.output {
                    if !is_valid_bucket_hash(output) {
                        return Err(format!("invalid bucket hash {}", output));
                    }
                }
                if level.next.curr.is_some()
                    || level.next.snap.is_some()
                    || level.next.shadow.is_some()
                {
                    return Err(format!(
                        "{} level {} next state is 1 but has merge fields",
                        prefix, index
                    ));
                }
            }
            2 => {
                if level.next.curr.is_none() {
                    return Err(format!(
                        "{} level {} next state is 2 but missing curr field",
                        prefix, index
                    ));
                } else if let Some(ref curr) = level.next.curr {
                    if !is_valid_bucket_hash(curr) {
                        return Err(format!("invalid bucket hash {}", curr));
                    }
                }
                
                if level.next.snap.is_none() {
                    return Err(format!(
                        "{} level {} next state is 2 but missing snap field",
                        prefix, index
                    ));
                } else if let Some(ref snap) = level.next.snap {
                    if !is_valid_bucket_hash(snap) {
                        return Err(format!("invalid bucket hash {}", snap));
                    }
                }
                
                if let Some(ref shadow) = level.next.shadow {
                    for hash in shadow.iter() {
                        if !is_valid_bucket_hash(hash) {
                            return Err(format!("invalid bucket hash {}", hash));
                        }
                    }
                }
            }
            _ => {
                return Err(format!(
                    "Invalid next state value at level {}: {}",
                    index, level.next.state
                ));
            }
        }
        
        Ok(())
    }
    
    // Validate HAS format for basic structural/string correctness
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 && self.version != 2 {
            return Err(format!("Invalid version: expected 1 or 2, got {}", self.version));
        }
        
        // Version 2 must have hotArchiveBuckets, version 1 must not
        if self.version == 2 && self.hot_archive_buckets.is_none() {
            return Err("Version 2 HAS must have hotArchiveBuckets field".to_string());
        }
        if self.version == 1 && self.hot_archive_buckets.is_some() {
            return Err("Version 1 HAS must not have hotArchiveBuckets field".to_string());
        }

        if self.current_ledger == 0 {
            return Err("currentLedger cannot be 0".to_string());
        } else if !is_checkpoint(self.current_ledger) {
            return Err(format!(
                "currentLedger {} is not on a 64-ledger checkpoint boundary",
                self.current_ledger
            ));
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
        let mut checkpoints = Vec::new();
        let mut checkpoint = CHECKPOINT_FREQUENCY - 1; // Start at first checkpoint (63)

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

// Round down to nearest checkpoint
pub fn checkpoint_number(ledger: u32) -> u32 {
    if ledger < CHECKPOINT_FREQUENCY - 1 {
        0
    } else {
        (ledger + 1) / CHECKPOINT_FREQUENCY * CHECKPOINT_FREQUENCY - 1
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
pub fn hash_prefix(hash: &str) -> Result<String, String> {
    if hash.len() < 6 {
        return Err(format!("Hash too short: '{}'", hash));
    }
    Ok(format!("{}/{}/{}", &hash[0..2], &hash[2..4], &hash[4..6]))
}

// Generate archive path for given bucket hash
pub fn bucket_path(hash: &str) -> Result<String, String> {
    let prefix = hash_prefix(hash)?;
    Ok(format!("bucket/{}/bucket-{}.xdr.gz", prefix, hash))
}

// Generate archive path for checkpoint file
// Path structure: category/XX/YY/ZZ/category-CHECKPOINT.ext
// where XX/YY/ZZ are the upper 3 bytes of the checkpoint number in hex
// (e.g., "ledger", 0x01020300 -> "ledger/01/02/03/ledger-01020300.xdr.gz")
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

// List all required archive files (excluding optional scp)
pub fn enumerate_all_files(has: &HistoryArchiveState) -> Vec<String> {
    let mut files = Vec::new();

    // Add root HAS
    files.push(ROOT_HAS_PATH.to_string());

    // Add all buckets
    for bucket_hash in has.buckets() {
        if let Ok(path) = bucket_path(&bucket_hash) {
            files.push(path);
        }
    }

    // Add all checkpoint files
    let checkpoints = has.get_checkpoint_range();
    for checkpoint in checkpoints {
        for category in &["history", "ledger", "transactions", "results"] {
            files.push(checkpoint_path(category, checkpoint));
        }
    }

    files.sort();
    files
}

// List all archive files including optional scp
pub fn enumerate_all_files_with_optional(has: &HistoryArchiveState) -> Vec<String> {
    let mut files = enumerate_all_files(has);

    let checkpoints = has.get_checkpoint_range();
    for checkpoint in checkpoints {
        files.push(checkpoint_path("scp", checkpoint));
    }

    files.sort();
    files
}
