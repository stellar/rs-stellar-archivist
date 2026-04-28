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

        let operation = MirrorOperation::new(
            dst_store.clone(),
            self.overwrite,
            self.low,
            self.high,
            self.allow_mirror_gaps,
            &args.storage_config,
            args.verify,
        );

        let pipeline_config = PipelineConfig {
            concurrency: args.concurrency,
            skip_optional: args.skip_optional,
            storage_config: args.storage_config,
        };

        let pipeline = Pipeline::new(operation, pipeline_config, src_store, Some(dst_store));

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
