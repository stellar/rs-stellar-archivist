//! Mirror operation - copies files from source to destination

use crate::history_format;
use crate::pipeline::{async_trait, Operation};
use crate::storage::{BoxedAsyncRead, Error as StorageError, StorageRef, WRITE_BUF_BYTES};
use crate::utils::{compute_checkpoint_bounds, fetch_well_known_history_file, ArchiveStats};
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};

/// Mirror operation errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot mirror: destination archive ends at ledger {dest_ledger} (checkpoint {dest_checkpoint} (0x{dest_checkpoint:08x})) but --low is {low_ledger} (checkpoint {low_checkpoint} (0x{low_checkpoint:08x})). This would create a gap in the archive. Use --allow-mirror-gaps to proceed anyway.")]
    MirrorGapDetected {
        dest_ledger: u32,
        dest_checkpoint: u32,
        low_ledger: u32,
        low_checkpoint: u32,
    },

    #[error(transparent)]
    Utils(#[from] crate::utils::Error),

    #[error("Archive mirror failed")]
    MirrorFailed,

    #[error("History format error: {0}")]
    HistoryFormat(#[from] crate::history_format::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct MirrorOperation {
    dst_op: StorageRef,
    overwrite: bool,
    stats: ArchiveStats,

    // User-specified arguments from CLI
    low: Option<u32>,
    high: Option<u32>,
    allow_mirror_gaps: bool,

    // Cached destination checkpoint at start of operation (or None if destination doesn't exist)
    initial_dest_checkpoint: OnceCell<Option<u32>>,
}

impl MirrorOperation {
    pub async fn new(
        dst: &str,
        overwrite: bool,
        low: Option<u32>,
        high: Option<u32>,
        allow_mirror_gaps: bool,
    ) -> Result<Self, Error> {
        let dst_op = crate::storage::StorageBackend::from_url(dst).await?;

        // Destination must support write operations
        if !dst_op.supports_writes() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("Destination storage backend does not support write operations. Only filesystem destinations (file://) are currently supported: {}", dst),
            ).into());
        }

        Ok(Self {
            dst_op,
            overwrite,
            stats: ArchiveStats::new(),
            low,
            high,
            allow_mirror_gaps,
            initial_dest_checkpoint: OnceCell::new(),
        })
    }

    /// Get the destination's initial checkpoint from .well-known file, caching the result
    /// Returns None if destination doesn't have a .well-known file
    async fn get_initial_dest_well_known_checkpoint(&self) -> Option<u32> {
        self.initial_dest_checkpoint
            .get_or_init(|| async {
                // Try to read the destination's .well-known file (local file, no retries required)
                match fetch_well_known_history_file(&self.dst_op, 0, 0).await {
                    Ok(has) => Some(has.current_ledger),
                    Err(_) => None, // No existing archive
                }
            })
            .await
            .clone()
    }

    async fn maybe_update_well_known(&self, highest_checkpoint: u32) -> Result<(), Error> {
        // Check if we should update the .well-known file
        // We should only update if:
        // 1. There's no existing .well-known file, or
        // 2. The new checkpoint is higher than the existing .well-known file

        let should_update = match self.get_initial_dest_well_known_checkpoint().await {
            Some(existing_ledger) => {
                let existing_checkpoint =
                    history_format::round_to_lower_checkpoint(existing_ledger);
                if highest_checkpoint > existing_checkpoint {
                    info!(
                        "Updating .well-known from checkpoint {} (0x{:08x}) to {} (0x{:08x})",
                        existing_checkpoint,
                        existing_checkpoint,
                        highest_checkpoint,
                        highest_checkpoint
                    );
                    true
                } else {
                    info!(
                        "Keeping existing .well-known at checkpoint {} (0x{:08x}) (mirrored up to {} (0x{:08x}))",
                        existing_checkpoint, existing_checkpoint, highest_checkpoint, highest_checkpoint
                    );
                    false
                }
            }
            None => {
                // No existing .well-known file - create a new one
                info!(
                    "No existing .well-known, creating new one at checkpoint {} (0x{:08x})",
                    highest_checkpoint, highest_checkpoint
                );
                true
            }
        };

        if should_update {
            // Copy the history file at the specified checkpoint to be our .well-known file
            let history_path = history_format::checkpoint_path("history", highest_checkpoint);
            let well_known_path = ".well-known/stellar-history.json";

            let dst_base = self.dst_op.get_base_path().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Destination storage backend does not have a filesystem path",
                )
            })?;

            let src_file = dst_base.join(&history_path);
            let dst_file = dst_base.join(well_known_path);

            // Check if the history file exists (it might not if the mirror had failures)
            if !tokio::fs::try_exists(&src_file).await.unwrap_or(false) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "Cannot update .well-known: history file at checkpoint {} (0x{:08x}) was not successfully mirrored ({})",
                        highest_checkpoint, highest_checkpoint,
                        src_file.display()
                    ),
                )
                .into());
            }

            // Ensure the .well-known directory exists
            if let Some(parent) = dst_file.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to create directory {}: {}", parent.display(), e),
                    )
                })?;
            }

            tokio::fs::copy(&src_file, &dst_file).await.map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to copy {} to {}: {}",
                        src_file.display(),
                        dst_file.display(),
                        e
                    ),
                )
            })?;

            info!(
                "Updated destination .well-known to checkpoint {} (0x{:08x})",
                highest_checkpoint, highest_checkpoint
            );
        }

        Ok(())
    }
}

#[async_trait]
impl Operation for MirrorOperation {
    async fn get_checkpoint_bounds(
        &self,
        source: &StorageRef,
        max_retries: u32,
        initial_backoff_ms: u64,
    ) -> Result<(u32, u32), crate::pipeline::Error> {
        // Determine the effective low checkpoint based on destination .well-known/stellar-history.json and flags
        //
        // Starting ledger logic:
        // 1. If --low is specified:
        //    - Check for gaps between destination and requested low and fail early (error unless --allow-mirror-gaps)
        //    - If destination is ahead of --low:
        //      * With --overwrite: honor the --low value
        //      * Without --overwrite: ignore --low and resume from destination
        // 2. If --low is not specified:
        //    - If destination exists: continue from destination checkpoint (regardless of --overwrite)
        //    - If destination doesn't exist: start from genesis checkpoint

        // First, get the source's latest checkpoint to know what's available
        let source_state = fetch_well_known_history_file(source, max_retries, initial_backoff_ms)
            .await
            .map_err(|e| crate::pipeline::Error::MirrorOperation(Error::Utils(e)))?;
        let source_checkpoint =
            history_format::round_to_lower_checkpoint(source_state.current_ledger);

        let dest_checkpoint_opt = self.get_initial_dest_well_known_checkpoint().await;

        // Determine the target checkpoint (limited by source and --high if specified)
        let target_checkpoint = if let Some(high) = self.high {
            let high_checkpoint = history_format::round_to_upper_checkpoint(high);
            std::cmp::min(source_checkpoint, high_checkpoint)
        } else {
            source_checkpoint
        };

        // Check if destination is already at or beyond the target (unless --overwrite is set)
        if !self.overwrite {
            if let Some(dest_ledger) = dest_checkpoint_opt {
                let dest_checkpoint = history_format::round_to_lower_checkpoint(dest_ledger);
                if dest_checkpoint >= target_checkpoint {
                    info!(
                        "Destination is up to date (at checkpoint {} (0x{:08x}), target {} (0x{:08x}))",
                        dest_checkpoint, dest_checkpoint, target_checkpoint, target_checkpoint
                    );
                    // Return a range that results in 0 checkpoints to process
                    return Ok((
                        target_checkpoint + history_format::CHECKPOINT_FREQUENCY,
                        target_checkpoint,
                    ));
                }
            }
        }

        let effective_low = if let Some(requested_low) = self.low {
            // User specified --low
            if let Some(dest_ledger) = dest_checkpoint_opt {
                // Destination archive already exists and has a .well-known/stellar-history.json file
                let dest_checkpoint = history_format::round_to_lower_checkpoint(dest_ledger);
                let requested_checkpoint = history_format::round_to_lower_checkpoint(requested_low);

                // Check for gaps between highest destination checkpoint and requested low
                if dest_checkpoint < requested_checkpoint {
                    if !self.allow_mirror_gaps {
                        return Err(Error::MirrorGapDetected {
                            dest_ledger,
                            dest_checkpoint,
                            low_ledger: requested_low,
                            low_checkpoint: requested_checkpoint,
                        }
                        .into());
                    } else {
                        warn!(
                            "WARNING: Creating gap in archive! Destination ends at ledger {} (checkpoint {} (0x{:08x})) but mirroring from {} (checkpoint {} (0x{:08x}))",
                            dest_ledger, dest_checkpoint, dest_checkpoint, requested_low, requested_checkpoint, requested_checkpoint
                        );
                    }

                    // Start at --low value
                    Some(requested_low)
                } else {
                    // Destination has checkpoint >= --low request
                    if self.overwrite {
                        // In overwrite mode, honor the --low request
                        info!(
                            "Destination already has checkpoint {} (0x{:08x}) (ledger {}), overwriting from ledger {} (checkpoint {} (0x{:08x}))",
                            dest_checkpoint, dest_checkpoint, dest_ledger, requested_low, requested_checkpoint, requested_checkpoint
                        );
                        Some(requested_low)
                    } else {
                        // Not in overwrite mode - ignore --low and resume from next checkpoint
                        let next_checkpoint =
                            dest_checkpoint + history_format::CHECKPOINT_FREQUENCY;
                        warn!(
                            "Ignoring --low {} since destination already has checkpoint {} (0x{:08x}) (ledger {}). Resuming from next checkpoint {} (0x{:08x}).",
                            requested_low, dest_checkpoint, dest_checkpoint, dest_ledger, next_checkpoint, next_checkpoint
                        );
                        Some(next_checkpoint)
                    }
                }
            } else {
                debug!(
                    "Destination archive does not exist, proceeding with --low {}",
                    requested_low
                );
                // No destination archive, use the requested low
                Some(requested_low)
            }
        } else {
            // No --low specified
            if self.overwrite {
                // With --overwrite and no --low, start from the beginning
                debug!("Overwrite mode with no --low, starting from beginning");
                None
            } else if let Some(checkpoint_ledger) = dest_checkpoint_opt {
                // Resume from the next checkpoint after what's already mirrored
                let dest_checkpoint = history_format::round_to_lower_checkpoint(checkpoint_ledger);
                let next_checkpoint = dest_checkpoint + history_format::CHECKPOINT_FREQUENCY;
                info!(
                    "Found existing archive at destination with ledger {}, resuming from next checkpoint {} (0x{:08x})",
                    checkpoint_ledger, next_checkpoint, next_checkpoint
                );
                Some(next_checkpoint)
            } else {
                debug!("Destination archive does not exist, starting from beginning");
                None
            }
        };

        compute_checkpoint_bounds(source_checkpoint, effective_low, self.high)
            .map_err(|e| crate::pipeline::Error::MirrorOperation(Error::Utils(e)))
    }

    async fn pre_check(&self, path: &str) -> Option<Result<(), StorageError>> {
        // Check destination before querying source, skip if file already exists
        match self.dst_op.exists(path).await {
            Ok(true) => {
                // If file exists and we're not overwriting, skip it
                if !self.overwrite {
                    return Some(Ok(()));
                }
                // Overwrite mode - proceed to download
                None
            }
            Ok(false) => {
                // File doesn't exist - proceed to download
                None
            }
            Err(e) => {
                // Failed to check existence on destination
                Some(Err(StorageError::fatal(format!(
                    "Failed to check existence of destination {}: {}",
                    path, e
                ))))
            }
        }
    }

    async fn process_object(
        &self,
        path: &str,
        mut reader: BoxedAsyncRead,
    ) -> Result<(), StorageError> {
        use tokio::io::{AsyncWriteExt, BufWriter};

        // Stream from source to destination
        let write_result = async {
            let writer = self.dst_op.open_writer(path).await?;
            let mut buf_writer = BufWriter::with_capacity(WRITE_BUF_BYTES, writer);

            tokio::io::copy(&mut reader, &mut buf_writer).await?;
            buf_writer.flush().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;

        match write_result {
            Ok(_) => Ok(()),
            Err(e) => {
                // Streaming/write error - clean up partial file before retrying
                if let Some(base_path) = self.dst_op.get_base_path() {
                    let file_path = base_path.join(path);
                    if file_path.exists() {
                        if let Err(rm_err) = tokio::fs::remove_file(&file_path).await {
                            // If we fail to clean up the partial file, we can't retry
                            return Err(StorageError::fatal(format!(
                                "Failed to remove partial file {}: {}",
                                file_path.display(),
                                rm_err
                            )));
                        }
                    }
                }
                // Partial file cleaned up, safe to retry
                Err(StorageError::retry(format!("Stream error: {}", e)))
            }
        }
    }

    fn record_success(&self, path: &str) {
        self.stats.record_success(path);
    }

    async fn record_failure(&self, path: &str) {
        self.stats.record_failure(path).await;
    }

    fn record_retry(&self) {
        self.stats.record_retry();
    }

    fn record_skipped(&self, path: &str) {
        self.stats.record_skipped(path);
    }

    async fn finalize(&self, highest_checkpoint: u32) -> Result<(), crate::pipeline::Error> {
        self.stats.report("mirror").await;

        // Update .well-known file with the highest checkpoint we processed
        if let Err(e) = self.maybe_update_well_known(highest_checkpoint).await {
            error!("Failed to update .well-known file: {}", e);
            return Err(e.into());
        }

        // Report failure if there were any failed files
        if self.stats.has_failures() {
            return Err(Error::MirrorFailed.into());
        }

        Ok(())
    }
}
