use crate::cli::{Error, GlobalArgs};
use crate::{
    mirror_operation::MirrorOperation,
    pipeline::{Pipeline, PipelineConfig},
    storage, utils,
};
use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
pub struct MirrorCmd {
    /// Source archive URL (http://, https://, file://)
    pub src: String,

    /// Destination path (must be file://)
    pub dst: String,

    /// Mirror starting from this ledger (rounds down to previous checkpoint unless already one)
    #[arg(long)]
    pub low: Option<u32>,

    /// Mirror up to this ledger (rounds up to next checkpoint unless already one)
    #[arg(long)]
    pub high: Option<u32>,

    /// Re-fetch and replace existing destination files in the mirror range
    #[arg(long)]
    pub overwrite: bool,

    /// Allow --low to start after the destination's current checkpoint, leaving a gap
    #[arg(long, default_value_t = false)]
    pub allow_mirror_gaps: bool,
}

impl MirrorCmd {
    pub async fn run(self, args: GlobalArgs) -> Result<(), Error> {
        info!(
            "Starting mirror from {} to {} with {} workers",
            self.src, self.dst, args.concurrency
        );

        let src_store = storage::from_url_with_config(&self.src, &args.storage_config)
            .map_err(|e| Error::Other(format!("Failed to create source backend: {e}")))?;

        let dst_store = storage::from_url_with_config(&self.dst, &args.storage_config)
            .map_err(|e| Error::Other(format!("Failed to create destination backend: {e}")))?;

        if !dst_store.supports_writes() {
            return Err(Error::Other(format!(
                "Destination does not support writes: {}",
                self.dst
            )));
        }

        let pipeline_config = PipelineConfig {
            concurrency: args.concurrency,
            skip_optional: args.skip_optional,
            skip_history_and_buckets: false,
            verify: args.verify,
            storage_config: args.storage_config,
        };

        let operation = MirrorOperation::new(
            src_store,
            dst_store,
            self.overwrite,
            self.low,
            self.high,
            self.allow_mirror_gaps,
            pipeline_config.clone(),
            /*update_well_known=*/ true,
        );

        let pipeline = Pipeline::new(operation, pipeline_config, args.report_path);

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
