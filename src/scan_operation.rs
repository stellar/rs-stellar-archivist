use crate::history_format;
use crate::pipeline::{async_trait, Operation, PipelineConfig, ProcessOutcome};
use crate::storage::{from_opendal_error, Error as StorageError, StorageRef};
use crate::utils::{compute_checkpoint_bounds, fetch_well_known_history_file, ArchiveStats};
use crate::xdr_verify::XdrVerificationManager;
use opendal::Buffer;
use opendal::Reader;
use thiserror::Error;
use tracing::error;

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

    #[error("Report error: {0}")]
    Report(#[from] crate::report::ReportError),
}

pub struct ScanOperation {
    // User-specified arguments from CLI
    low: Option<u32>,
    high: Option<u32>,

    // Pipeline configuration (storage retry params + verify mode for the report)
    pipeline_config: PipelineConfig,
}

impl ScanOperation {
    pub fn new(low: Option<u32>, high: Option<u32>, pipeline_config: PipelineConfig) -> Self {
        Self {
            low,
            high,
            pipeline_config,
        }
    }

    async fn consume_stream(&self, path: &str, reader: Reader) -> Result<(), StorageError> {
        use futures_util::StreamExt;
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|e| from_opendal_error(e, &format!("failed to create stream for {}", path)))?;
        while let Some(result) = stream.next().await {
            result.map_err(|e| from_opendal_error(e, &format!("failed to read from {}", path)))?;
        }
        Ok(())
    }

    fn checkpoint_from_verified_path(path: &str) -> Result<u32, StorageError> {
        crate::history_format::checkpoint_from_path(path)
            .ok_or_else(|| StorageError::fatal(format!("invalid checkpoint path: {}", path)))
    }
}

#[async_trait]
impl Operation for ScanOperation {
    async fn get_checkpoint_bounds(
        &self,
        source: &StorageRef,
    ) -> Result<(u32, u32), crate::pipeline::Error> {
        let source_state = fetch_well_known_history_file(
            source,
            self.pipeline_config.storage_config.max_retries as u32,
            self.pipeline_config
                .storage_config
                .retry_min_delay
                .as_millis() as u64,
        )
        .await
        .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))?;
        let source_checkpoint =
            history_format::round_to_lower_checkpoint(source_state.current_ledger);

        compute_checkpoint_bounds(source_checkpoint, self.low, self.high)
            .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))
    }

    async fn process_object(
        &self,
        path: &str,
        src_store: &StorageRef,
        manager: Option<&XdrVerificationManager>,
    ) -> Result<ProcessOutcome, StorageError> {
        if let Some(manager) = manager {
            // Verify mode: stream content and validate
            let reader = src_store.open_reader(path).await?;
            if crate::history_format::is_bucket_file(path) {
                crate::verify::verify_bucket_stream(path, reader).await?;
            } else if crate::history_format::is_ledger_header_file(path) {
                let checkpoint = Self::checkpoint_from_verified_path(path)?;
                let data = crate::xdr_verify::parse_ledger_header_stream(path, reader).await?;
                manager.record_header_data(checkpoint, data);
            } else if crate::history_format::is_transactions_file(path) {
                let checkpoint = Self::checkpoint_from_verified_path(path)?;
                let hashes = crate::xdr_verify::parse_transactions_stream(path, reader).await?;
                manager.record_tx_set_hashes(checkpoint, hashes);
            } else if crate::history_format::is_results_file(path) {
                let checkpoint = Self::checkpoint_from_verified_path(path)?;
                let hashes = crate::xdr_verify::parse_results_stream(path, reader).await?;
                manager.record_result_hashes(checkpoint, hashes);
            } else if crate::history_format::is_scp_file(path) {
                crate::xdr_verify::parse_scp_stream(path, reader).await?;
            } else {
                self.consume_stream(path, reader).await?;
            }
        } else {
            // Existence-only mode: just check if file exists (no streaming)
            if !src_store.exists(path).await? {
                return Err(StorageError::not_found());
            }
        }
        Ok(ProcessOutcome::Processed)
    }

    async fn finalize(
        &self,
        _highest_checkpoint: u32,
        stats: &ArchiveStats,
        report_path: Option<&std::path::Path>,
    ) -> Result<(), crate::pipeline::Error> {
        stats.report("scan").await;

        if let Some(path) = report_path {
            let report = crate::report::ArchiveReport {
                version: crate::report::REPORT_VERSION,
                section: stats.report_section().await,
            };
            crate::report::write_to_path(path, &report)
                .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Report(e)))?;
        }

        // Cross-file/chain checks compare files that each passed their own
        // verification; a flagged checkpoint means they don't agree. Scan is
        // read-only, so this only reports the inconsistency.
        let inconsistent = stats.checkpoint_failure_count().await;
        if inconsistent > 0 {
            error!(
                "{inconsistent} checkpoint(s) failed cross-file/chain verification: the files are \
                 individually valid but mutually inconsistent (scan does not modify the archive)."
            );
        }

        if stats.has_failures().await {
            return Err(Error::ScanFailed.into());
        }

        Ok(())
    }

    /// Scan never writes — return `Processed` and let the pipeline record it.
    async fn process_buffer(
        &self,
        _path: &str,
        _buffer: Buffer,
    ) -> Result<ProcessOutcome, StorageError> {
        Ok(ProcessOutcome::Processed)
    }
}
