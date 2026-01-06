/// Scan operation - validates that files exist
use crate::pipeline::{async_trait, Operation};
use crate::storage::{ReaderResult, StorageRef};
use crate::utils::{compute_checkpoint_bounds, ArchiveStats};
use thiserror::Error;
use tracing::{debug, error};

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
        compute_checkpoint_bounds(source, self.low, self.high)
            .await
            .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))
    }

    async fn process_object(&self, path: &str, reader_result: ReaderResult) {
        // For scan, we only need to verify the file exists and is accessible
        // The pipeline already attempted to open the file, so if we got a reader, it exists
        // We don't read from it to avoid HTTP/2 stream resets that cause rate limiting
        match reader_result {
            ReaderResult::Ok(_reader) => {
                // File exists and is accessible (pipeline successfully opened it)
                debug!("Validated: {}", path);
                self.stats.record_success(path);
                // Drop the reader without reading to avoid stream resets
            }
            ReaderResult::Err(_e) => {
                // Source file couldn't be read
                error!("Invalid file: {} - File not found or inaccessible", path);
                self.stats.record_failure(path).await;
            }
        }
    }

    async fn finalize(&self, _highest_checkpoint: u32) -> Result<(), crate::pipeline::Error> {
        self.stats.report("scan").await;

        // Fail if there were any failures
        if self.stats.has_failures() {
            return Err(Error::ScanFailed.into());
        }

        Ok(())
    }

    fn use_head_requests(&self) -> bool {
        // Scan only needs to check existence, not read content
        // Using HEAD requests avoids HTTP/2 stream resets
        true
    }
}
