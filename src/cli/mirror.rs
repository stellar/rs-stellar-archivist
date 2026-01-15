use crate::cli::{Error, GlobalArgs};
use crate::{
    mirror_operation::MirrorOperation,
    pipeline::{Pipeline, PipelineConfig},
    utils,
};
use clap::Parser;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
pub struct MirrorCmd {
    /// Source archive URL (http://, https://, file://)
    pub src: String,

    /// Destination path (must be file://)
    pub dst: String,

    /// Mirror starting from this ledger (will round down to nearest checkpoint)
    #[arg(long)]
    pub low: Option<u32>,

    /// Mirror up to this ledger only (will round up to nearest checkpoint)
    #[arg(long)]
    pub high: Option<u32>,

    /// Overwrite existing files within the mirrored range (if not set, mirror will skip over existing files)
    #[arg(long)]
    pub overwrite: bool,

    /// Allow mirroring even when it would create gaps in the destination archive
    #[arg(long, default_value_t = false)]
    pub allow_mirror_gaps: bool,
}

impl MirrorCmd {
    pub async fn run(self, args: GlobalArgs) -> Result<(), Error> {
        info!(
            "Starting mirror from {} to {} with {} workers",
            self.src, self.dst, args.concurrency
        );

        let operation = MirrorOperation::new(
            &self.dst,
            self.overwrite,
            self.low,
            self.high,
            self.allow_mirror_gaps,
            &args.storage_config,
        )
        .await?;

        let pipeline_config = PipelineConfig {
            source: self.src.clone(),
            concurrency: args.concurrency,
            skip_optional: args.skip_optional,
            storage_config: args.storage_config,
        };

        let pipeline = Arc::new(
            Pipeline::new(operation, pipeline_config)
                .await
                .map_err(utils::map_pipeline_error)?,
        );

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
