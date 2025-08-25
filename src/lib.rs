pub mod history_format;
pub mod mirror_operation;
pub mod pipeline;
pub mod scan_operation;
pub mod storage;
pub mod utils;

use thiserror::Error;

/// Top-level error type for the Stellar Archivist library
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    MirrorOperation(#[from] mirror_operation::Error),

    #[error(transparent)]
    ScanOperation(#[from] scan_operation::Error),

    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests;

// Test helper modules with config structures and run functions
// Available for integration tests
pub mod test_helpers {
    use crate::{
        mirror_operation::MirrorOperation,
        pipeline::{self, Pipeline, PipelineConfig},
        scan_operation::ScanOperation,
    };
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

    pub async fn run_scan(config: ScanConfig) -> Result<(), crate::Error> {
        let operation = ScanOperation::new(config.low, config.high).await?;

        let pipeline_config = PipelineConfig {
            source: config.archive,
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            max_retries: 3,
            initial_backoff_ms: 100,
        };

        let pipeline = Arc::new(
            Pipeline::new(operation, pipeline_config)
                .await
                .map_err(|e| match e {
                    pipeline::Error::ScanOperation(scan_err) => crate::Error::ScanOperation(scan_err),
                    pipeline::Error::Io(io_err) => crate::Error::Io(io_err),
                    other => crate::Error::Other(other.to_string()),
                })?,
        );
        pipeline
            .run()
            .await
            .map_err(|e| match e {
                pipeline::Error::ScanOperation(scan_err) => crate::Error::ScanOperation(scan_err),
                pipeline::Error::Io(io_err) => crate::Error::Io(io_err),
                other => crate::Error::Other(other.to_string()),
            })
    }

    pub async fn run_mirror(config: MirrorConfig) -> Result<(), crate::Error> {
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

        let pipeline = Arc::new(
            Pipeline::new(operation, pipeline_config)
                .await
                .map_err(|e| match e {
                    pipeline::Error::MirrorOperation(mirror_err) => crate::Error::MirrorOperation(mirror_err),
                    pipeline::Error::Io(io_err) => crate::Error::Io(io_err),
                    other => crate::Error::Other(other.to_string()),
                })?,
        );
        pipeline
            .run()
            .await
            .map_err(|e| match e {
                pipeline::Error::MirrorOperation(mirror_err) => crate::Error::MirrorOperation(mirror_err),
                pipeline::Error::Io(io_err) => crate::Error::Io(io_err),
                other => crate::Error::Other(other.to_string()),
            })
    }
}
