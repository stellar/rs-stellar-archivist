//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! Retry, concurrency limiting, timeout handling, and bandwidth throttling are delegated
//! to the storage layer (OpenDAL).

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::StorageConfig,
};
use bytes::Buf;
use futures_util::{
    future::{join, join3, join_all},
    stream, StreamExt,
};
use lru::LruCache;
use opendal::{Buffer, Reader};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use thiserror::Error;
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

    /// Process an object by streaming its content from the provided reader.
    /// Only called when existence_check_only() returns false.
    ///
    /// Returns:
    /// - Ok(()) on success
    /// - Err(e) on failure (storage layer handles retries internally)
    async fn process_object(&self, path: &str, reader: Reader)
        -> Result<(), crate::storage::Error>;

    /// Process an object from an in-memory buffer.
    /// Used when the content is already loaded (e.g., history files that are parsed).
    async fn process_buffer(&self, path: &str, buffer: Buffer)
        -> Result<(), crate::storage::Error>;

    /// Record a successful file processing
    fn record_success(&self, path: &str);

    /// Record a failed file processing
    async fn record_failure(&self, path: &str);

    /// Record a skipped file (already exists at destination)
    fn record_skipped(&self, path: &str);

    /// Called when all work is complete
    /// The pipeline passes the highest checkpoint that was processed
    /// This is where operations should check their internal failure counts and return an error if needed
    async fn finalize(&self, highest_checkpoint: u32) -> Result<(), Error>;

    /// Indicates if this operation only needs to check file existence, not read content.
    fn existence_check_only(&self) -> bool {
        false
    }

    /// Pre-check before hitting the source. Allows operations to skip files
    /// without making a source request (e.g., if destination already exists).
    ///
    /// Returns:
    /// - Some(Ok(())) to skip (file already handled)
    /// - Some(Err(e)) if pre-check failed
    /// - None to proceed with fetching from source
    async fn pre_check(&self, _path: &str) -> Option<Result<(), crate::storage::Error>> {
        None // Default: always proceed to source
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Source archive URL, as provided by the user
    pub source: String,
    /// Number of concurrent checkpoint workers
    pub concurrency: usize,
    /// Whether to skip optional SCP files
    pub skip_optional: bool,
    /// Storage layer configuration (retry, timeout, bandwidth, etc.)
    pub storage_config: StorageConfig,
}

pub struct Pipeline<Op: Operation> {
    operation: Op,
    config: PipelineConfig,
    src_store: crate::storage::StorageRef,
    bucket_lru: Mutex<LruCache<String, ()>>,
    progress_tracker: AtomicUsize,
}

impl<Op: Operation> Pipeline<Op> {
    /// Create a new pipeline
    pub async fn new(operation: Op, config: PipelineConfig) -> Result<Self, Error> {
        let src_store =
            crate::storage::from_url_with_config(&config.source, &config.storage_config)
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

        Ok(Self {
            operation,
            config,
            src_store,
            bucket_lru,
            progress_tracker: AtomicUsize::new(0),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Error> {
        // Get checkpoint bounds from the operation
        let (lower_bound, upper_bound) = self
            .operation
            .get_checkpoint_bounds(&self.src_store)
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
                let completed = self.progress_tracker.fetch_add(1, Ordering::Relaxed) + 1;
                if completed % PROGRESS_REPORTING_FREQUENCY == 0 || completed == total_count {
                    info!("Progress: {}/{} checkpoints processed", completed, total_count);
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
            self.clone().process_file(path)
        }));

        // Optionally add SCP files
        if self.config.skip_optional {
            let _ = join(hist, cats).await;
        } else {
            let path = checkpoint_path("scp", checkpoint);
            let scp = self.clone().process_file(path);
            let _ = join3(hist, cats, scp).await;
        }
    }

    /// Process a history file for a checkpoint.
    /// Downloads and parses the history JSON, then processes buckets.
    /// Retries are handled by the storage layer (OpenDAL).
    async fn process_history(self: Arc<Self>, checkpoint: u32) {
        use futures_util::TryStreamExt;

        let history_path = checkpoint_path("history", checkpoint);

        // Download the history file (storage layer handles retries)
        // Use into_stream to handle chunked transfer encoding properly
        let download_result: Result<Buffer, crate::storage::Error> = async {
            let reader = self.src_store.open_reader(&history_path).await?;
            // Convert to stream and collect all chunks into a single buffer
            let stream = reader
                .into_stream(..)
                .await
                .map_err(|e| crate::storage::Error::fatal(format!("Stream error: {}", e)))?;
            let chunks: Vec<Buffer> = stream
                .try_collect()
                .await
                .map_err(|e| crate::storage::Error::fatal(format!("Read error: {}", e)))?;
            // Merge chunks into a single buffer
            let buffer: Buffer = chunks.into_iter().flatten().collect();
            Ok(buffer)
        }
        .await;

        let buffer = match download_result {
            Ok(buf) => buf,
            Err(e) => {
                error!("Failed to download history file {}: {}", history_path, e);
                self.operation.record_failure(&history_path).await;
                return;
            }
        };

        // Parse and validate
        let parse_result: Result<HistoryFileState, Error> = (|| {
            let state: HistoryFileState = serde_json::from_reader(buffer.clone().reader())
                .map_err(|e| history_format::Error::InvalidJson {
                    path: history_path.clone(),
                    error: e.to_string(),
                })?;
            state.validate()?;
            Ok(state)
        })();

        match parse_result {
            Ok(state) => {
                // For existence-only checks, we already downloaded and parsed the history file,
                // so just record success. For mirror, we need to write it to destination.
                if self.operation.existence_check_only() {
                    self.operation.record_success(&history_path);
                    self.clone().process_buckets(state).await;
                } else {
                    // Write the history file (we already have content in buffer) and process buckets concurrently
                    let history_path_clone = history_path.clone();
                    let write_history = async {
                        match self
                            .operation
                            .process_buffer(&history_path_clone, buffer)
                            .await
                        {
                            Ok(()) => self.operation.record_success(&history_path_clone),
                            Err(e) => {
                                error!(
                                    "Failed to write history file {}: {}",
                                    history_path_clone, e
                                );
                                self.operation.record_failure(&history_path_clone).await;
                            }
                        }
                    };
                    let buckets = self.clone().process_buckets(state);
                    let _ = join(write_history, buckets).await;
                }
            }
            Err(e) => {
                error!(
                    "Failed to parse history JSON for checkpoint {} (0x{:08x}): {}",
                    checkpoint, checkpoint, e
                );
                self.operation.record_failure(&history_path).await;
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
                                pipeline.process_file(path).await;
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

    /// Process a single file (bucket, ledger, transactions, results, scp).
    /// Retries are handled by the storage layer (OpenDAL).
    async fn process_file(self: Arc<Self>, path: String) {
        // Pre-check: allow operation to skip without querying source
        if let Some(result) = self.operation.pre_check(&path).await {
            match result {
                Ok(()) => {
                    debug!("Skipping: {}", path);
                    self.operation.record_skipped(&path);
                    return;
                }
                Err(e) => {
                    error!("Pre-check failed for {}: {}", path, e);
                    self.operation.record_failure(&path).await;
                    return;
                }
            }
        }

        // For existence-only checks, we don't need to open a reader or call process_object
        if self.operation.existence_check_only() {
            match self.src_store.exists(&path).await {
                Ok(true) => {
                    debug!("Exists: {}", path);
                    self.operation.record_success(&path);
                }
                Ok(false) => {
                    error!("File not found: {}", path);
                    self.operation.record_failure(&path).await;
                }
                Err(e) => {
                    error!("Failed to check existence for {}: {}", path, e);
                    self.operation.record_failure(&path).await;
                }
            }
        } else {
            // Normal path - open a reader for actual content streaming
            let reader = match self.src_store.open_reader(&path).await {
                Ok(reader) => reader,
                Err(e) => {
                    error!("Failed to open reader for {}: {}", path, e);
                    self.operation.record_failure(&path).await;
                    return;
                }
            };

            // Process the file
            match self.operation.process_object(&path, reader).await {
                Ok(()) => {
                    debug!("Processed: {}", path);
                    self.operation.record_success(&path);
                }
                Err(e) => {
                    error!("Failed to process {}: {}", path, e);
                    self.operation.record_failure(&path).await;
                }
            }
        }
    }
}

pub use async_trait::async_trait;
