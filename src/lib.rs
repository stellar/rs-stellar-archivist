#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::fn_params_excessive_bools)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::trivially_copy_pass_by_ref)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::unused_async)]
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::semicolon_if_nothing_returned)]
#![allow(clippy::use_debug)]
#![allow(clippy::doc_markdown)]

pub mod history_format;
pub mod mirror_operation;
pub mod pipeline;
pub mod scan_operation;
pub mod storage;
pub mod utils;
pub mod verify;

#[cfg(feature = "cli")]
pub mod cli;

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
        pipeline::{Pipeline, PipelineConfig},
        scan_operation::ScanOperation,
        storage::StorageConfig,
    };
    use std::sync::Arc;
    use std::time::Duration;

    /// Create a `StorageConfig` suitable for testing (generous timeouts, limited concurrency)
    #[must_use]
    pub fn test_storage_config() -> StorageConfig {
        StorageConfig::new(
            3,                          // max_retries
            Duration::from_millis(100), // retry_min_delay
            Duration::from_secs(30),    // retry_max_delay
            64,                         // max_concurrent
            Duration::from_secs(30),    // timeout
            Duration::from_secs(300),   // io_timeout
            0,                          // bandwidth_limit (unlimited)
            false,                      // atomic_file_writes
        )
    }

    #[derive(Debug)]
    pub struct ScanConfig {
        pub archive: String,
        pub concurrency: usize,
        pub skip_optional: bool,
        pub low: Option<u32>,
        pub high: Option<u32>,
        pub storage_config: StorageConfig,
        pub verify: bool,
    }

    impl ScanConfig {
        /// Create a new `ScanConfig` with sensible defaults for testing
        pub fn new(archive: impl Into<String>) -> Self {
            Self {
                archive: archive.into(),
                concurrency: 4,
                skip_optional: false,
                low: None,
                high: None,
                storage_config: test_storage_config(),
                verify: false,
            }
        }

        /// Set concurrency level
        #[must_use]
        pub fn concurrency(mut self, concurrency: usize) -> Self {
            self.concurrency = concurrency;
            self
        }

        /// Skip optional files (SCP)
        #[must_use]
        pub fn skip_optional(mut self) -> Self {
            self.skip_optional = true;
            self
        }

        /// Set the low ledger bound
        #[must_use]
        pub fn low(mut self, low: u32) -> Self {
            self.low = Some(low);
            self
        }

        /// Set the high ledger bound
        #[must_use]
        pub fn high(mut self, high: u32) -> Self {
            self.high = Some(high);
            self
        }

        /// Set custom storage config
        #[must_use]
        pub fn storage_config(mut self, config: StorageConfig) -> Self {
            self.storage_config = config;
            self
        }

        /// Enable hash verification
        pub fn verify(mut self) -> Self {
            self.verify = true;
            self
        }
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
        pub storage_config: StorageConfig,
        pub verify: bool,
    }

    impl MirrorConfig {
        /// Create a new `MirrorConfig` with sensible defaults for testing
        pub fn new(src: impl Into<String>, dst: impl Into<String>) -> Self {
            Self {
                src: src.into(),
                dst: dst.into(),
                concurrency: 4,
                skip_optional: false,
                low: None,
                high: None,
                overwrite: false,
                allow_mirror_gaps: false,
                storage_config: test_storage_config(),
                verify: false,
            }
        }

        /// Set concurrency level
        #[must_use]
        pub fn concurrency(mut self, concurrency: usize) -> Self {
            self.concurrency = concurrency;
            self
        }

        /// Skip optional files (SCP)
        #[must_use]
        pub fn skip_optional(mut self) -> Self {
            self.skip_optional = true;
            self
        }

        /// Set the low ledger bound
        #[must_use]
        pub fn low(mut self, low: u32) -> Self {
            self.low = Some(low);
            self
        }

        /// Set the high ledger bound
        #[must_use]
        pub fn high(mut self, high: u32) -> Self {
            self.high = Some(high);
            self
        }

        /// Enable overwrite mode
        #[must_use]
        pub fn overwrite(mut self) -> Self {
            self.overwrite = true;
            self
        }

        /// Allow gaps in the mirrored archive
        #[must_use]
        pub fn allow_mirror_gaps(mut self) -> Self {
            self.allow_mirror_gaps = true;
            self
        }

        /// Set custom storage config
        #[must_use]
        pub fn storage_config(mut self, config: StorageConfig) -> Self {
            self.storage_config = config;
            self
        }

        /// Enable hash verification
        pub fn verify(mut self) -> Self {
            self.verify = true;
            self
        }
    }

    pub async fn run_scan(config: ScanConfig) -> Result<(), crate::Error> {
        let operation = ScanOperation::new(
            config.low,
            config.high,
            config.storage_config.max_retries as u32,
            config.storage_config.retry_min_delay.as_millis() as u64,
            config.verify,
        )
        .await?;

        let pipeline_config = PipelineConfig {
            source: config.archive,
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            storage_config: config.storage_config,
        };

        let pipeline = Arc::new(
            Pipeline::new(operation, pipeline_config)
                .await
                .map_err(crate::utils::map_pipeline_error)?,
        );
        pipeline
            .run()
            .await
            .map_err(crate::utils::map_pipeline_error)
    }

    pub async fn run_mirror(config: MirrorConfig) -> Result<(), crate::Error> {
        let operation = MirrorOperation::new(
            &config.dst,
            config.overwrite,
            config.low,
            config.high,
            config.allow_mirror_gaps,
            &config.storage_config,
            config.verify,
        )
        .await?;

        let pipeline_config = PipelineConfig {
            source: config.src,
            concurrency: config.concurrency,
            skip_optional: config.skip_optional,
            storage_config: config.storage_config,
        };

        let pipeline = Arc::new(
            Pipeline::new(operation, pipeline_config)
                .await
                .map_err(crate::utils::map_pipeline_error)?,
        );
        pipeline
            .run()
            .await
            .map_err(crate::utils::map_pipeline_error)
    }
}
