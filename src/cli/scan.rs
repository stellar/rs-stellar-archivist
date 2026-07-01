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

    /// Scan starting from this ledger (rounds down to previous checkpoint unless already one)
    #[arg(long)]
    pub low: Option<u32>,

    /// Scan up to this ledger (rounds up to next checkpoint unless already one)
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

        let src_store = storage::from_url_with_config(&self.archive, &args.storage_config)
            .map_err(|e| Error::Other(format!("Failed to create source backend: {e}")))?;

        let pipeline_config = PipelineConfig::new(
            args.concurrency,
            args.skip_optional,
            false,
            args.verify,
            args.storage_config,
            &src_store,
        )
        .await;

        // Create the scan operation
        let operation = ScanOperation::new(src_store, self.low, self.high, pipeline_config.clone());

        let pipeline = Pipeline::new(operation, pipeline_config, args.report_path);

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
