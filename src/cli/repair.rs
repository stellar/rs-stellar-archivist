use crate::cli::{Error, GlobalArgs};
use crate::{
    pipeline::{Pipeline, PipelineConfig},
    repair_operation::RepairOperation,
    storage, utils,
};
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
pub struct RepairCmd {
    /// Source archive URL (known-good archive to fetch repairs from)
    pub src: String,

    /// Destination archive path to repair (must be file://)
    pub dst: String,

    /// Repair starting from this ledger (will round down to nearest checkpoint)
    #[arg(long)]
    pub low: Option<u32>,

    /// Repair up to this ledger (will round up to nearest checkpoint)
    #[arg(long)]
    pub high: Option<u32>,

    /// JSON file listing specific file paths to repair (manual mode)
    #[arg(long)]
    pub files: Option<PathBuf>,

    /// Show what would be repaired without downloading
    #[arg(long)]
    pub dry_run: bool,
}

impl RepairCmd {
    pub async fn run(self, args: GlobalArgs) -> Result<(), Error> {
        info!(
            "Starting repair from {} to {} with {} workers",
            self.src, self.dst, args.concurrency
        );

        // Parse manual file list if provided
        let file_list = if let Some(ref path) = self.files {
            let content = std::fs::read_to_string(path).map_err(|e| {
                Error::Other(format!(
                    "Failed to read file list from {}: {}",
                    path.display(),
                    e
                ))
            })?;
            let list = crate::repair_operation::parse_file_list_json(&content)?;
            info!(
                "Manual mode: {} files to repair from {}",
                list.len(),
                path.display()
            );
            Some(list)
        } else {
            None
        };

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

        let operation = RepairOperation::new(
            src_store.clone(),
            dst_store.clone(),
            self.low,
            self.high,
            args.verify,
            self.dry_run,
            args.concurrency,
            args.skip_optional,
            &args.storage_config,
        );

        if let Some(files) = file_list {
            operation.run_manual(&files).await?;
            return Ok(());
        }

        let pipeline_config = PipelineConfig {
            concurrency: args.concurrency,
            skip_optional: args.skip_optional,
            verify: args.verify,
            storage_config: args.storage_config,
        };

        let pipeline = Pipeline::new(operation, pipeline_config, src_store, Some(dst_store));

        pipeline.run().await.map_err(utils::map_pipeline_error)?;

        Ok(())
    }
}
