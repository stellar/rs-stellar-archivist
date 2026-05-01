use crate::cli::{Error, GlobalArgs};
use crate::{
    pipeline::{Pipeline, PipelineConfig},
    scan_operation::ScanOperation,
    storage, utils,
};
use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
pub struct ScanCmd {
    /// Archive URL to scan (http://, https://, file://)
    pub archive: String,

    /// Scan starting from this ledger (will round to nearest checkpoint)
    #[arg(long)]
    pub low: Option<u32>,

    /// Scan up to this checkpoint only
    #[arg(long)]
    pub high: Option<u32>,
}

impl ScanCmd {
    pub async fn run(self, args: GlobalArgs) -> Result<(), Error> {
        info!("Starting scan of {}", self.archive);

        if let Some(low) = self.low {
            info!("Scanning from ledger {} onwards", low);
        }
        if let Some(high) = self.high {
            info!("Scanning up to checkpoint {}", high);
        }

        // Create the scan operation
        let operation = ScanOperation::new(
            self.low,
            self.high,
            args.storage_config.max_retries as u32,
            args.storage_config.retry_min_delay.as_millis() as u64,
            args.verify,
        );

        let src_store = storage::from_url_with_config(&self.archive, &args.storage_config)
            .map_err(|e| Error::Other(format!("Failed to create source backend: {e}")))?;

        let pipeline_config = PipelineConfig {
            concurrency: args.concurrency,
            skip_optional: args.skip_optional,
            storage_config: args.storage_config,
        };

        let pipeline = Pipeline::new(operation, pipeline_config, src_store, None);

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
