//! Pipeline for processing Stellar History Archives
//!
//! The pipeline coordinates parallel processing of archive checkpoints and their files.
//! It discovers checkpoints to process, distributes work to concurrent workers, and
//! delegates file processing to the provided Operation implementation. The pipeline
//! is responsible for tasks common to all operations. This includes handling worker concurrency,
//! fetching history files, deduping bucket files, and spawning work for each file.
//! Each operation defines business logic for actually processing the files.
//!
//! Step 1: Get checkpoint bounds from the operation - Bounds vary based on operation
//!         and user parameters.
//!
//! Step 2: Process history checkpoint files to generate file fetching work - For each checkpoint,
//!         download and parse the history JSON file to identify all files that need processing.
//!
//! Step 3: Dedup bucket files and dispatch work to the operation - For each deduped file,
//!         spawn a file processing task. The task will open a reader and hand it off to the
//!         operation's process_object method.
//!
use lru::LruCache;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use thiserror::Error;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::history_format;
use crate::storage::ReaderResult;

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
/// 1 million entries * ~64 bytes per hash = ~64MB memory max
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

/// Represents the source of data for a file fetching task.
enum FileTaskSource {
    /// Fetch from storage backend.
    Storage { is_optional: bool },
    /// Fetch from already-buffered content. We use this for history files, since both
    /// the pipeline and the archival op need to read the file.
    Buffered(Vec<u8>),
    /// Report an error. This is only used for history files, where the previous
    /// Buffered download failed and we need to propagate the error to the archival op.
    Error(crate::storage::Error),
}

pub struct Pipeline<Op: Operation> {
    operation: Arc<Op>,
    config: PipelineConfig,
    source_op: crate::storage::StorageRef,
    bucket_lru: Arc<Mutex<LruCache<String, ()>>>,

    /// Semaphore to limit total concurrent I/O operations
    io_permits: Arc<tokio::sync::Semaphore>,
    /// Counter for outstanding (spawned but not completed) download tasks
    inflight_files: Arc<std::sync::atomic::AtomicUsize>,
}

impl<Op: Operation> Pipeline<Op> {
    /// Create a new pipeline
    pub async fn new(operation: Op, config: PipelineConfig) -> Result<Self, Error> {
        // Create source storage backend from URL with retry config
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
        // Create LRU cache for bucket deduplication
        let bucket_lru = Arc::new(Mutex::new(LruCache::new(
            std::num::NonZeroUsize::new(BUCKET_LRU_CACHE_SIZE).unwrap(),
        )));

        // Create semaphore to limit total concurrent I/O operations
        // This ensures we never have more than 'concurrency' I/O operations at once
        let io_permits = Arc::new(tokio::sync::Semaphore::new(config.concurrency));

        Ok(Self {
            operation: Arc::new(operation),
            source_op,
            config,
            bucket_lru,
            io_permits,
            inflight_files: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Error> {
        // Get checkpoint bounds from the operation
        let (lower_bound, upper_bound) = self
            .operation
            .get_checkpoint_bounds(&self.source_op)
            .await?;

        // Calculate total count for progress tracking
        let total_count = history_format::count_checkpoints_in_range(lower_bound, upper_bound);
        if total_count == 0 {
            info!("No checkpoints to process");
            return Ok(());
        }

        // Store the highest checkpoint for passing to finalize later
        let highest_checkpoint = upper_bound;

        // Create checkpoint channel for distributing work
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(100);
        let checkpoint_rx = Arc::new(tokio::sync::Mutex::new(checkpoint_rx));

        let progress_tracker = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Create checkpoint processors that will take checkpoints from the channel
        // and spawn file download tasks for each checkpoint's files
        let num_checkpoint_processors = self.config.concurrency;
        debug!(
            "Starting {} checkpoint processors",
            num_checkpoint_processors
        );

        // Spawn checkpoint producer
        let producer_lower_bound = lower_bound;
        let producer_upper_bound = upper_bound;
        let producer = tokio::spawn(async move {
            let mut checkpoint = producer_lower_bound;

            while checkpoint <= producer_upper_bound {
                if checkpoint_tx.send(checkpoint).await.is_err() {
                    // Channel closed, workers have stopped
                    break;
                }
                checkpoint += history_format::CHECKPOINT_FREQUENCY;
            }
            drop(checkpoint_tx); // Signal no more checkpoints
            debug!("Checkpoint producer finished");
        });

        let mut checkpoint_processors = Vec::new();

        // Since a single checkpoint may produce many file fetches, limit
        // number of outstanding file fetches to prevent unbounded memory growth.
        let max_outstanding = self.config.concurrency * 3;

        for processor_id in 0..num_checkpoint_processors {
            let pipeline = self.clone();
            let rx = checkpoint_rx.clone();
            let tracker = progress_tracker.clone();
            let processor_total_count = total_count;
            let sem = self.io_permits.clone();

            let processor = tokio::spawn(async move {
                loop {
                    // Acquire permit before taking checkpoint work
                    let _permit = sem.acquire().await.unwrap();

                    // Check if we have too many outstanding downloads
                    let outstanding = pipeline
                        .inflight_files
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if outstanding > max_outstanding {
                        // Too many tasks queued, release permit and yield
                        drop(_permit);
                        debug!(
                            "Processor {} yielding, {} outstanding downloads > {} limit",
                            processor_id, outstanding, max_outstanding
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        continue;
                    }

                    // Get next checkpoint from channel
                    let checkpoint = {
                        let mut rx = rx.lock().await;
                        match rx.recv().await {
                            Some(cp) => cp,
                            None => {
                                // No more work, release permit and exit
                                drop(_permit);
                                break;
                            }
                        }
                    };

                    debug!(
                        "Processor {} processing checkpoint {:08x} (outstanding: {})",
                        processor_id, checkpoint, outstanding
                    );

                    // Process this checkpoint (permit will be held until we've downloaded
                    // the checkpoint and spawned all the file fetching tasks).
                    pipeline.spawn_checkpoint_tasks(checkpoint, _permit).await;

                    let total = tracker.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                    if total % PROGRESS_REPORTING_FREQUENCY == 0 || total == processor_total_count {
                        info!(
                            "Progress: {}/{} checkpoints processed",
                            total, processor_total_count
                        );
                    }
                }
                debug!("Processor {} finished", processor_id);
            });
            checkpoint_processors.push(processor);
        }

        // Wait for producer to finish sending all checkpoints
        producer
            .await
            .map_err(|e| Error::Other(format!("Producer task failed: {}", e)))?;

        // Wait for all checkpoint processors to finish spawning file tasks
        for processor in checkpoint_processors {
            processor
                .await
                .map_err(|e| Error::Other(format!("Checkpoint processor task failed: {}", e)))?;
        }

        // At this point, we've finished processing all the checkpoint files,
        // but we still have to wait for outstanding file tasks to complete.
        let mut wait_count = 0;
        loop {
            let outstanding = self
                .inflight_files
                .load(std::sync::atomic::Ordering::Relaxed);
            if outstanding == 0 {
                break;
            }
            if wait_count % 100 == 0 {
                debug!(
                    "Waiting for {} outstanding file tasks to complete",
                    outstanding
                );
            }
            wait_count += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        debug!("All file tasks completed");

        // Finalize operation with the highest checkpoint we processed
        self.operation.finalize(highest_checkpoint).await?;

        Ok(())
    }

    /// Unified helper to spawn a file processing task with proper concurrency control
    fn spawn_file_task(&self, path: String, source: FileTaskSource) {
        let op = self.operation.clone();
        let sem = self.io_permits.clone();
        let outstanding = self.inflight_files.clone();
        let src = self.source_op.clone();

        // Increment outstanding counter before spawning
        outstanding.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        tokio::spawn(async move {
            // Acquire semaphore permit before processing
            let _permit = sem.acquire().await.unwrap();

            // Get or create the reader based on the source
            let reader_result = match source {
                // Open a reader stream for the file from backend storage
                FileTaskSource::Storage { is_optional } => {
                    if op.use_head_requests() {
                        // For scan operations, just check existence to avoid HTTP/2 stream resets
                        match src.exists(&path).await {
                            Ok(true) => {
                                // File exists - create a dummy reader that won't be used
                                let empty = Box::new(std::io::Cursor::new(Vec::new()))
                                    as crate::storage::BoxedAsyncRead;
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
                                    debug!(
                                        "Failed to check existence for optional {}: {}",
                                        path, e
                                    );
                                } else {
                                    error!("Failed to check existence for {}: {}", path, e);
                                }
                                ReaderResult::Err(e.into())
                            }
                        }
                    } else {
                        // Normal path - open a reader for actual content streaming
                        match src.open_reader(&path).await {
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
                    }
                }
                // Use already-buffered content
                FileTaskSource::Buffered(buffer) => {
                    let reader =
                        Box::new(std::io::Cursor::new(buffer)) as crate::storage::BoxedAsyncRead;
                    ReaderResult::Ok(reader)
                }
                // We tried to fetch this file earlier but failed, so just pass the error to the op.
                FileTaskSource::Error(error) => ReaderResult::Err(error),
            };

            op.process_object(&path, reader_result).await;

            // Decrement outstanding counter when done
            outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    /// Spawn tasks for processing a checkpoint. This has three steps:
    /// 1. Fetch and buffer the history file for the checkpoint.
    /// 2. Parse the history file to see what else we need to process for the checkpoint.
    /// 3. Fetch and spawn workers for the remaining files.
    ///
    /// The permit ensures we don't spawn too many checkpoint processing tasks at once.
    async fn spawn_checkpoint_tasks(
        &self,
        checkpoint: u32,
        _permit: tokio::sync::SemaphorePermit<'_>,
    ) {
        let source_op = self.source_op.clone();
        let bucket_lru = self.bucket_lru.clone();

        let history_path = history_format::checkpoint_path("history", checkpoint);
        let history_path_for_fetch = history_path.clone();
        let source_op_for_history = source_op.clone();

        let history_future = async move {
            use tokio::io::AsyncReadExt;

            // Download the history file into a buffer. We'll need to process it to generate
            // file fetching work and also forward a stream iterator to the archival op. Buffer
            // so we don't have to re-download the file for the archival op.
            let mut reader = source_op_for_history
                .open_reader(&history_path_for_fetch)
                .await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;

            let state: crate::history_format::HistoryFileState = serde_json::from_slice(&buffer)
                .map_err(|e| crate::history_format::Error::InvalidJson {
                    path: history_path_for_fetch.clone(),
                    error: e.to_string(),
                })?;
            state.validate()?;
            Ok::<(crate::history_format::HistoryFileState, Vec<u8>), Error>((state, buffer))
        };

        // Start fetching other checkpoint files (ledger, transactions, results)
        for category in &["ledger", "transactions", "results"] {
            let path = history_format::checkpoint_path(category, checkpoint);
            self.spawn_file_task(path, FileTaskSource::Storage { is_optional: false });
        }

        // Add optional SCP files if not skipping
        if !self.config.skip_optional {
            let path = history_format::checkpoint_path("scp", checkpoint);
            self.spawn_file_task(path, FileTaskSource::Storage { is_optional: true });
        }

        // Wait for history JSON to be downloaded and parsed
        match history_future.await {
            Ok((checkpoint_state, buffer)) => {
                // Process the history file using the buffered content
                self.spawn_file_task(history_path.clone(), FileTaskSource::Buffered(buffer));

                // We have the parsed state for bucket processing,
                // start spawning file tasks for all bucket files.
                let mut bucket_count = 0;
                let mut dedup_count = 0;
                for bucket_hash in checkpoint_state.buckets() {
                    if let Ok(path) = history_format::bucket_path(&bucket_hash) {
                        bucket_count += 1;
                        // Dedup buckets using cache. Note that we only need to dedup
                        // buckets, all other files are guaranteed to be unique.
                        {
                            let mut cache = bucket_lru.lock().unwrap();
                            if cache.put(bucket_hash.clone(), ()).is_some() {
                                // Bucket already fetched, skip
                                dedup_count += 1;
                                continue;
                            }
                        }

                        self.spawn_file_task(path, FileTaskSource::Storage { is_optional: false });
                    }
                }

                if dedup_count > 0 {
                    debug!(
                        "Checkpoint {:08x}: {} buckets, {} deduplicated by LRU cache",
                        checkpoint, bucket_count, dedup_count
                    );
                }
            }
            Err(e) => {
                debug!(
                    "Failed to fetch history JSON for checkpoint {:08x}: {}",
                    checkpoint, e
                );

                // Forward error to op since the history file was fetched by the pipeline,
                // not the archival op.
                let err = crate::storage::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to download/parse history file: {}", e),
                ));
                self.spawn_file_task(history_path, FileTaskSource::Error(err));
            }
        }
    }
}

pub use async_trait::async_trait;
