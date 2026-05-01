use crate::history_format;
use crate::pipeline::{async_trait, Operation};
use crate::storage::{from_opendal_error, Error as StorageError, StorageRef};
use crate::utils::{compute_checkpoint_bounds, fetch_well_known_history_file, ArchiveStats};
use crate::xdr_verify::XdrVerificationManager;
use opendal::Reader;
use std::sync::Arc;
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
}

pub struct ScanOperation {
    // User-specified arguments from CLI
    low: Option<u32>,
    high: Option<u32>,

    // Retry configuration for source fetches
    max_retries: u32,
    retry_min_delay_ms: u64,

    // Populated if we should verify files, otherwise None
    verification_manager: Option<Arc<XdrVerificationManager>>,
}

impl ScanOperation {
    pub fn new(
        low: Option<u32>,
        high: Option<u32>,
        max_retries: u32,
        retry_min_delay_ms: u64,
        verify: bool,
    ) -> Self {
        Self {
            low,
            high,
            max_retries,
            retry_min_delay_ms,
            verification_manager: if verify {
                Some(Arc::new(XdrVerificationManager::new()))
            } else {
                None
            },
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
        let source_state =
            fetch_well_known_history_file(source, self.max_retries, self.retry_min_delay_ms)
                .await
                .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))?;
        let source_checkpoint =
            history_format::round_to_lower_checkpoint(source_state.current_ledger);

        compute_checkpoint_bounds(source_checkpoint, self.low, self.high)
            .map_err(|e| crate::pipeline::Error::ScanOperation(Error::Utils(e)))
    }

    async fn process_object(&self, path: &str, src_store: &StorageRef) -> Result<(), StorageError> {
        if let Some(ref manager) = self.verification_manager {
            // Verify mode: stream content and validate
            let reader = src_store.open_reader(path).await?;
            if crate::history_format::is_bucket_file(path) {
                crate::verify::verify_bucket_stream(path, reader).await
            } else if crate::history_format::is_ledger_file(path) {
                let checkpoint = Self::checkpoint_from_verified_path(path)?;
                let data = crate::xdr_verify::parse_ledger_stream(path, reader).await?;
                manager.record_ledger_data(checkpoint, data);
                Ok(())
            } else if crate::history_format::is_transactions_file(path) {
                let checkpoint = Self::checkpoint_from_verified_path(path)?;
                let hashes = crate::xdr_verify::parse_transactions_stream(path, reader).await?;
                manager.record_tx_set_hashes(checkpoint, hashes);
                Ok(())
            } else if crate::history_format::is_results_file(path) {
                let checkpoint = Self::checkpoint_from_verified_path(path)?;
                let hashes = crate::xdr_verify::parse_results_stream(path, reader).await?;
                manager.record_result_hashes(checkpoint, hashes);
                Ok(())
            } else if crate::history_format::is_scp_file(path) {
                crate::xdr_verify::parse_scp_stream(path, reader).await
            } else {
                self.consume_stream(path, reader).await
            }
        } else {
            // Existence-only mode: just check if file exists (no streaming)
            if src_store.exists(path).await? {
                Ok(())
            } else {
                Err(StorageError::not_found())
            }
        }
    }

    async fn finalize(
        &self,
        _highest_checkpoint: u32,
        stats: &ArchiveStats,
    ) -> Result<(), crate::pipeline::Error> {
        if let Some(ref manager) = self.verification_manager {
            let chain_errors = manager.verify_checkpoint_chain();
            for err in &chain_errors {
                error!("Checkpoint {}: {}", err.checkpoint, err.message);
                stats.record_failure("xdr-chain-verification").await;
            }

            let accumulated_errors = manager.get_errors();
            for _ in &accumulated_errors {
                stats.record_failure("xdr-cross-verification").await;
            }

            let total_errors = chain_errors.len() + accumulated_errors.len();
            if total_errors > 0 {
                error!(
                    "XDR verification found {} cross-file hash mismatches",
                    total_errors
                );
            }
        }

        stats.report("scan").await;

        if stats.has_failures() {
            return Err(Error::ScanFailed.into());
        }

        Ok(())
    }

    fn finalize_checkpoint(&self, checkpoint: u32) {
        if let Some(ref manager) = self.verification_manager {
            manager.verify_and_release(checkpoint);
        }
    }
}
