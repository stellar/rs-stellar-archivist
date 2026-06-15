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

    /// JSON plan (from a prior --dry-run) listing the broken files, buckets,
    /// and checkpoints to repair. Cannot be combined with --low/--high/--dry-run.
    #[arg(long)]
    pub plan: Option<PathBuf>,

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

        if self.plan.is_some() && (self.low.is_some() || self.high.is_some() || self.dry_run) {
            return Err(Error::Other(
                "--plan cannot be combined with --low/--high/--dry-run".to_string(),
            ));
        }

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

        // Read the plan up front (if any); the `Option` doubles as the mode flag
        // below. `--verify` works exactly as in regular repair: it governs
        // validation of downloaded content and the dst probing of discovered
        // files (listed items are re-fetched unconditionally either way).
        let plan = if let Some(ref plan_path) = self.plan {
            info!("Plan mode: applying {}", plan_path.display());
            let plan = crate::report::read_from_path(plan_path).map_err(|e| {
                Error::Other(format!(
                    "Failed to read plan from {}: {}",
                    plan_path.display(),
                    e
                ))
            })?;
            Some(plan)
        } else {
            None
        };

        // Build the operation once; the pipeline path reuses the stores/config,
        // so the operation takes clones.
        let operation = RepairOperation::new(
            src_store.clone(),
            dst_store.clone(),
            self.low,
            self.high,
            self.dry_run,
            pipeline_config.clone(),
        );

        if let Some(plan) = plan {
            operation
                .run_manual(plan, args.report_path.as_deref())
                .await?;
        } else {
            let pipeline = Pipeline::new(
                operation,
                pipeline_config,
                src_store,
                Some(dst_store),
                args.report_path,
            );
            pipeline.run().await.map_err(utils::map_pipeline_error)?;
        }

        Ok(())
    }
}
