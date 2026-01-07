//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::ReaderResult,
};
use futures_util::{
    future::{join, join3, join_all},
    stream, StreamExt,
};
use lru::LruCache;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use thiserror::Error;
use tokio::{io::AsyncReadExt, sync::Semaphore};
use tracing::{debug, error, info};

/// Pipeline errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("History format error: {0}")]
    HistoryFormatError(#[from] crate::history_format::Error),

    #[error("Mirror operation error: {0}")]
    MirrorOperation(#[from] crate::mirror_operation::Error),

    #[error("Scan operation error: {0}")]
    ScanOperation(#[from] crate::scan_operation::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

/// Maximum number of bucket entries to cache in the LRU for deduplication
const BUCKET_LRU_CACHE_SIZE: usize = 1_000_000;

/// How often to report progress (every N checkpoints)
const PROGRESS_REPORTING_FREQUENCY: usize = 100;

/// Unified operation trait for scan and mirror
#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    /// Get the checkpoint bounds for this operation
    /// Returns (lower_bound, upper_bound) checkpoints to process
    async fn get_checkpoint_bounds(
        &self,
        source: &crate::storage::StorageRef,
    ) -> Result<(u32, u32), Error>;

    /// Process an object
    /// The Pipeline provides either a reader that streams the object content,
    /// or an error if the reader couldn't be obtained (e.g., file not found)
    /// Operations should track failures internally and not propagate errors from individual files
    async fn process_object(&self, object: &str, reader_result: ReaderResult);

    /// Called when all work is complete
    /// The pipeline passes the highest checkpoint that was processed
    /// This is where operations should check their internal failure counts and return an error if needed
    async fn finalize(&self, highest_checkpoint: u32) -> Result<(), Error>;

    /// Indicates if this operation only needs to check existence (uses HEAD requests)
    /// Returns true for scan operations to avoid HTTP/2 stream resets
    fn use_head_requests(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Source archive URL, as provided by the user
    pub source: String,
    /// Number of concurrent workers for processing
    pub concurrency: usize,
    /// Whether to skip optional SCP files
    pub skip_optional: bool,
    /// Maximum number of HTTP retry attempts
    pub max_retries: u32,
    /// Initial backoff in milliseconds for HTTP retries
    pub initial_backoff_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            source: String::new(),
            concurrency: 64,
            skip_optional: false,
            max_retries: 3,
            initial_backoff_ms: 100,
        }
    }
}

pub struct Pipeline<Op: Operation> {
    operation: Op,
    config: PipelineConfig,
    source_op: crate::storage::StorageRef,
    bucket_lru: Mutex<LruCache<String, ()>>,
    io_permits: Semaphore,
    progress_tracker: AtomicUsize,
}

impl<Op: Operation> Pipeline<Op> {
    /// Create a new pipeline
    pub async fn new(operation: Op, config: PipelineConfig) -> Result<Self, Error> {
        let retry_config = crate::storage::HttpRetryConfig {
            max_retries: config.max_retries,
            initial_backoff_ms: config.initial_backoff_ms,
        };
        let source_op =
            crate::storage::StorageBackend::from_url(&config.source, Some(retry_config))
                .await
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "Failed to create storage backend for {}: {}",
                            config.source, e
                        ),
                    )
                })?;

        let bucket_lru = Mutex::new(LruCache::new(
            std::num::NonZeroUsize::new(BUCKET_LRU_CACHE_SIZE).unwrap(),
        ));
        let io_permits = Semaphore::new(config.concurrency);

        Ok(Self {
            operation,
            config,
            source_op,
            bucket_lru,
            io_permits,
            progress_tracker: AtomicUsize::new(0),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Error> {
        // Get checkpoint bounds from the operation
        let (lower_bound, upper_bound) = self
            .operation
            .get_checkpoint_bounds(&self.source_op)
            .await?;

        let total_count = history_format::count_checkpoints_in_range(lower_bound, upper_bound);
        if total_count == 0 {
            info!("No checkpoints to process");
            return Ok(());
        }

        // Form a lazy iterator that produces checkpoint-processing futures
        let checkpoints = (lower_bound..=upper_bound)
            .step_by(history_format::CHECKPOINT_FREQUENCY as usize)
            .map(|ck| {
                let pipeline = self.clone();
                async move {
                    pipeline.process_checkpoint(ck).await;
                }
            });

        // Process checkpoints concurrently, limiting to config.concurrency at a time
        stream::iter(checkpoints)
            .for_each_concurrent(self.config.concurrency, |fut| async {
                fut.await;
                let n = self.progress_tracker.fetch_add(1, Ordering::Relaxed) + 1;
                if n % PROGRESS_REPORTING_FREQUENCY == 0 || n == total_count {
                    info!("Progress: {}/{} checkpoints processed", n, total_count);
                }
            })
            .await;

        // Finalize operation with the highest checkpoint we processed
        self.operation.finalize(upper_bound).await?;

        Ok(())
    }

    async fn process_checkpoint(self: Arc<Self>, checkpoint: u32) {
        // Process history file and categories concurrently
        let hist = self.clone().process_history(checkpoint);
        let cats = join_all(["ledger", "transactions", "results"].map(|cat| {
            let path = checkpoint_path(cat, checkpoint);
            self.clone().process_file(path, false)
        }));

        // Optionally add SCP files
        if self.config.skip_optional {
            let _ = join(hist, cats).await;
        } else {
            let path = checkpoint_path("scp", checkpoint);
            let scp = self.clone().process_file(path, true);
            let _ = join3(hist, cats, scp).await;
        }
    }

    async fn process_history(self: Arc<Self>, checkpoint: u32) {
        let history_path = checkpoint_path("history", checkpoint);

        // Download and parse the history file
        let history_result: Result<(HistoryFileState, Vec<u8>), Error> = async {
            let mut reader = self.source_op.open_reader(&history_path).await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;

            let state: HistoryFileState = serde_json::from_slice(&buffer).map_err(|e| {
                history_format::Error::InvalidJson {
                    path: history_path.clone(),
                    error: e.to_string(),
                }
            })?;
            state.validate()?;
            Ok((state, buffer))
        }
        .await;

        match history_result {
            Ok((state, buffer)) => {
                // Process the history file with buffered content and buckets concurrently
                let histfile = self.clone().process_buffered_file(history_path, buffer);
                let buckets = self.clone().process_buckets(state);
                let _ = join(histfile, buckets).await;
            }
            Err(e) => {
                debug!(
                    "Failed to fetch history JSON for checkpoint {:08x}: {}",
                    checkpoint, e
                );
                // Forward error to operation
                let err = crate::storage::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to download/parse history file: {}", e),
                ));
                self.operation
                    .process_object(&history_path, ReaderResult::Err(err))
                    .await;
            }
        }
    }

    async fn process_buckets(self: Arc<Self>, state: HistoryFileState) {
        // Filter buckets through LRU cache for deduplication
        let bucket_futures: Vec<_> = {
            let mut cache = self.bucket_lru.lock().unwrap();
            state
                .buckets()
                .iter()
                .filter_map(|bucket| {
                    if cache.put(bucket.clone(), ()).is_none() {
                        // New bucket, process it
                        bucket_path(bucket).ok().map(|path| {
                            let pipeline = self.clone();
                            async move {
                                pipeline.process_file(path, false).await;
                            }
                        })
                    } else {
                        // Already seen, skip
                        None
                    }
                })
                .collect()
        };

        join_all(bucket_futures).await;
    }

    async fn process_file(self: Arc<Self>, path: String, is_optional: bool) {
        // Acquire I/O permit
        let _permit = self.io_permits.acquire().await.unwrap();

        let reader_result = if self.operation.use_head_requests() {
            // Avoid HTTP2 stream when just checking existence.
            match self.source_op.exists(&path).await {
                Ok(true) => {
                    // File exists - create a dummy reader that won't be used
                    let empty =
                        Box::new(std::io::Cursor::new(Vec::new())) as crate::storage::BoxedAsyncRead;
                    ReaderResult::Ok(empty)
                }
                Ok(false) => {
                    if is_optional {
                        debug!("Optional file doesn't exist: {}", path);
                    } else {
                        error!("File doesn't exist: {}", path);
                    }
                    ReaderResult::Err(crate::storage::Error::NotFound)
                }
                Err(e) => {
                    if is_optional {
                        debug!("Failed to check existence for optional {}: {}", path, e);
                    } else {
                        error!("Failed to check existence for {}: {}", path, e);
                    }
                    ReaderResult::Err(e.into())
                }
            }
        } else {
            // Normal path - open a reader for actual content streaming
            match self.source_op.open_reader(&path).await {
                Ok(reader) => ReaderResult::Ok(reader),
                Err(e) => {
                    if is_optional {
                        debug!("Failed to get reader for optional {}: {}", path, e);
                    } else {
                        error!("Failed to get reader for {}: {}", path, e);
                    }
                    ReaderResult::Err(e.into())
                }
            }
        };

        self.operation.process_object(&path, reader_result).await;
    }

    async fn process_buffered_file(self: Arc<Self>, path: String, buffer: Vec<u8>) {
        // Acquire I/O permit (for consistency, though buffered files don't need network I/O)
        let _permit = self.io_permits.acquire().await.unwrap();

        let reader = Box::new(std::io::Cursor::new(buffer)) as crate::storage::BoxedAsyncRead;
        self.operation
            .process_object(&path, ReaderResult::Ok(reader))
            .await;
    }
}

pub use async_trait::async_trait;
