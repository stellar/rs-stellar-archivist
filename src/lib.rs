pub mod history_file;
pub mod mirror_operation;
pub mod pipeline;
pub mod scan_operation;
pub mod storage;
pub mod utils;

// Test helper modules with config structures and run functions
// Available for integration tests
pub mod test_helpers {
    use crate::{
        mirror_operation::MirrorOperation,
        pipeline::{Pipeline, PipelineConfig},
        scan_operation::ScanOperation,
    };
    use anyhow::Result;
    use std::sync::Arc;

    #[derive(Debug)]
    pub struct ScanConfig {
        pub archive: String,
        pub concurrency: usize,
        pub skip_optional: bool,
        pub low: Option<u32>,
        pub high: Option<u32>,
    }

    pub struct MirrorConfig {
        pub src: String,
        pub dst: String,
        pub concurrency: usize,
        pub skip_optional: bool,
        pub low: Option<u32>,
        pub high: Option<u32>,
        pub overwrite: bool,
        pub allow_mirror_gaps: bool,
    }

    pub async fn run_scan(config: ScanConfig) -> Result<()> {
        // Validate filesystem sources exist before creating storage
        if config.archive.starts_with("file://") {
            let path = config.archive.trim_start_matches("file://");
            if !std::path::Path::new(path).exists() {
                anyhow::bail!("Source path does not exist: {}", path);
            }
        }

        let operation = ScanOperation::new(config.low, config.high).await?;

        let pipeline_config = PipelineConfig {
            source: config.archive,
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            max_retries: 3,
            initial_backoff_ms: 100,
        };

        let pipeline = Arc::new(Pipeline::new(operation, pipeline_config).await?);
        pipeline.run().await
    }

    pub async fn run_mirror(config: MirrorConfig) -> Result<()> {
        // Validate destination is file:// (only supported destination type)
        if !config.dst.starts_with("file://") {
            anyhow::bail!("Destination must be a filesystem path (file://)");
        }

        // Validate filesystem sources exist before creating storage
        if config.src.starts_with("file://") {
            let path = config.src.trim_start_matches("file://");
            if !std::path::Path::new(path).exists() {
                anyhow::bail!("Source path does not exist: {}", path);
            }
        }

        let operation = MirrorOperation::new(
            &config.dst,
            config.overwrite,
            config.low,
            config.high,
            config.allow_mirror_gaps,
        )
        .await?;

        let pipeline_config = PipelineConfig {
            source: config.src,
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            max_retries: 3,
            initial_backoff_ms: 100,
        };

        let pipeline = Arc::new(Pipeline::new(operation, pipeline_config).await?);
        pipeline.run().await
    }
}
