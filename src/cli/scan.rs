use crate::cli::{Error, GlobalArgs};
use crate::{
    pipeline::{Pipeline, PipelineConfig},
    scan_operation::ScanOperation,
    utils,
};
use clap::Parser;
use std::sync::Arc;
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
        let operation = ScanOperation::new(self.low, self.high).await?;

        // Configure the pipeline with low/high bounds and storage config
        let pipeline_config = PipelineConfig {
            source: self.archive.clone(),
            concurrency: args.concurrency,
            skip_optional: args.skip_optional,
            storage_config: args.storage_config,
        };

        // Create and run the pipeline
        let pipeline = Arc::new(
            Pipeline::new(operation, pipeline_config)
                .await
                .map_err(utils::map_pipeline_error)?,
        );

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
