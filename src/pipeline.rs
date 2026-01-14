//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.

use crate::{
    history_format::{self, bucket_path, checkpoint_path, HistoryFileState},
    storage::ErrorClass,
};
use futures_util::{
    future::{join, join3, join_all},
    stream, StreamExt,
};
use lru::LruCache;
use std::sync::{Arc, Mutex};
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
        max_retries: u32,
        initial_backoff_ms: u64,
    ) -> Result<(u32, u32), Error>;

    /// Process an object by streaming its content from the provided reader.
    /// Only called when existence_check_only() returns false.
    ///
    /// Returns:
    /// - Ok(()) on success
    /// - Err(e) where e.class == Retry for transient errors
    /// - Err(e) where e.class == Fatal/NotFound for permanent errors
    async fn process_object(
        &self,
        path: &str,
        reader: crate::storage::BoxedAsyncRead,
    ) -> Result<(), crate::storage::Error>;

    /// Record a successful file processing
    fn record_success(&self, path: &str);

    /// Record a failed file processing
    async fn record_failure(&self, path: &str);

    /// Record a retry attempt
    fn record_retry(&self);

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
    src_store: crate::storage::StorageRef,
    bucket_lru: Mutex<LruCache<String, ()>>,
    io_permits: Semaphore,
}

use crate::utils::RetryState;

impl<Op: Operation> Pipeline<Op> {
    /// Handle a storage error with retry logic.
    /// Returns `false` if we should give up, `true` if we should retry.
    async fn maybe_backoff_for_retry(
        &self,
        path: &str,
        error: &crate::storage::Error,
        retry: &mut RetryState,
        action: &str,
    ) -> bool {
        match error.class {
            ErrorClass::Retry => {
                self.operation.record_retry();
                if !retry.record_attempt() {
                    error!(
                        "Failed to {} {} after {} attempts: {}",
                        action, path, retry.attempt, error
                    );
                    self.operation.record_failure(path).await;
                    return false; // give up
                }
                debug!(
                    "Retrying {} (attempt {}/{}): {}, backing off {}ms",
                    path, retry.attempt, retry.max_retries, error, retry.backoff_ms
                );
                retry.backoff().await;
                true // retry
            }
            ErrorClass::Fatal | ErrorClass::NotFound => {
                error!("Failed to {} {}: {}", action, path, error);
                self.operation.record_failure(path).await;
                false // give up
            }
        }
    }

    /// Create a new pipeline
    pub async fn new(operation: Op, config: PipelineConfig) -> Result<Self, Error> {
        let src_store = crate::storage::StorageBackend::from_url(&config.source)
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
            src_store,
            bucket_lru,
            io_permits,
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Error> {
        // Get checkpoint bounds from the operation
        let (lower_bound, upper_bound) = self
            .operation
            .get_checkpoint_bounds(
                &self.src_store,
                self.config.max_retries,
                self.config.initial_backoff_ms,
            )
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
            .enumerate()
            .for_each_concurrent(self.config.concurrency, |(i, fut)| async move {
                fut.await;
                if i % PROGRESS_REPORTING_FREQUENCY == 0 || i == total_count {
                    info!("Progress: {}/{} checkpoints processed", i, total_count);
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

    async fn process_history(self: Arc<Self>, checkpoint: u32) {
        let history_path = checkpoint_path("history", checkpoint);
        let mut retry = RetryState::new(self.config.max_retries, self.config.initial_backoff_ms);

        // Download the history file
        let buffer = loop {
            let download_result: Result<Vec<u8>, crate::storage::Error> = async {
                let mut reader = self.src_store.open_reader(&history_path).await?;
                let mut buffer = Vec::new();
                reader
                    .read_to_end(&mut buffer)
                    .await
                    .map_err(|e| crate::storage::Error::retry(format!("Read error: {}", e)))?;
                Ok(buffer)
            }
            .await;

            match download_result {
                Ok(bytes) => break bytes,
                Err(e) => {
                    if !self
                        .maybe_backoff_for_retry(&history_path, &e, &mut retry, "download")
                        .await
                    {
                        return;
                    }
                }
            }
        };

        // Parse and validate
        let parse_result: Result<HistoryFileState, Error> = (|| {
            let state: HistoryFileState = serde_json::from_slice(&buffer).map_err(|e| {
                history_format::Error::InvalidJson {
                    path: history_path.clone(),
                    error: e.to_string(),
                }
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
                        let reader = Box::new(std::io::Cursor::new(buffer))
                            as crate::storage::BoxedAsyncRead;
                        match self
                            .operation
                            .process_object(&history_path_clone, reader)
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

    async fn process_file(self: Arc<Self>, path: String) {
        let mut retry = RetryState::new(self.config.max_retries, self.config.initial_backoff_ms);

        // Acquire I/O permit
        let _permit = self.io_permits.acquire().await.unwrap();

        loop {
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
                        return;
                    }
                    Ok(false) => {
                        error!("File not found: {}", path);
                        self.operation.record_failure(&path).await;
                        return;
                    }
                    Err(e) => {
                        if !self
                            .maybe_backoff_for_retry(&path, &e, &mut retry, "check")
                            .await
                        {
                            return;
                        }
                    }
                }
            } else {
                // Normal path - open a reader for actual content streaming
                let reader = match self.src_store.open_reader(&path).await {
                    Ok(reader) => reader,
                    Err(e) => {
                        if !self
                            .maybe_backoff_for_retry(&path, &e, &mut retry, "open")
                            .await
                        {
                            return;
                        }
                        continue;
                    }
                };

                // Process the file
                match self.operation.process_object(&path, reader).await {
                    Ok(()) => {
                        debug!("Processed: {}", path);
                        self.operation.record_success(&path);
                        return;
                    }
                    Err(e) => {
                        if !self
                            .maybe_backoff_for_retry(&path, &e, &mut retry, "process")
                            .await
                        {
                            return;
                        }
                    }
                }
            }
        }
    }
}

pub use async_trait::async_trait;
