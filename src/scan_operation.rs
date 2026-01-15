use crate::history_format;
/// Scan operation - validates that files exist
use crate::pipeline::{async_trait, Operation};
use crate::storage::{Error as StorageError, StorageRef};
use crate::utils::{compute_checkpoint_bounds, fetch_well_known_history_file, ArchiveStats};
use opendal::{Buffer, Reader};
use thiserror::Error;

/// Scan operation errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("Archive scan failed")]
    ScanFailed,

    #[error(transparent)]
    Utils(#[from] crate::utils::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("History format error: {0}")]
    HistoryFormat(#[from] crate::history_format::Error),
}

pub struct ScanOperation {
    stats: ArchiveStats,

    // User-specified arguments from CLI
    low: Option<u32>,
    high: Option<u32>,
}

impl ScanOperation {
    pub async fn new(low: Option<u32>, high: Option<u32>) -> Result<Self, Error> {
        Ok(Self {
            stats: ArchiveStats::new(),
            low,
            high,
        })
    }
}

#[async_trait]
impl Operation for ScanOperation {
    async fn get_checkpoint_bounds(
        &self,
        source: &StorageRef,
    ) -> Result<(u32, u32), crate::pipeline::Error> {
        let source_state = fetch_well_known_history_file(source)
            .await
            .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))?;
        let source_checkpoint =
            history_format::round_to_lower_checkpoint(source_state.current_ledger);

        compute_checkpoint_bounds(source_checkpoint, self.low, self.high)
            .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))
    }

    async fn process_object(&self, _path: &str, _reader: Reader) -> Result<(), StorageError> {
        // For now, scan just checks existence.
        unreachable!(
            "ScanOperation uses existence_check_only(), process_object should not be called"
        )
    }

    async fn process_buffer(&self, _path: &str, _buffer: Buffer) -> Result<(), StorageError> {
        // For now, scan just checks existence.
        unreachable!(
            "ScanOperation uses existence_check_only(), process_buffer should not be called"
        )
    }

    fn record_success(&self, path: &str) {
        self.stats.record_success(path);
    }

    async fn record_failure(&self, path: &str) {
        self.stats.record_failure(path).await;
    }

    fn record_skipped(&self, _path: &str) {
        // Scan never skips files
    }

    async fn finalize(&self, _highest_checkpoint: u32) -> Result<(), crate::pipeline::Error> {
        self.stats.report("scan").await;

        // Fail if there were any failures
        if self.stats.has_failures() {
            return Err(Error::ScanFailed.into());
        }

        Ok(())
    }

    fn existence_check_only(&self) -> bool {
        // Scan only needs to check existence, not read content
        // TODO: Add full verification when --verify is set
        true
    }
}
