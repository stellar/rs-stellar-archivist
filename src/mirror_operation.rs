//! Mirror operation - copies files from source to destination

use crate::history_format;
use crate::pipeline::{async_trait, Operation};
use crate::storage::{ReaderResult, StorageRef, WRITE_BUF_BYTES};
use crate::utils::{compute_checkpoint_bounds, fetch_well_known_history_file, ArchiveStats};
use thiserror::Error;
use tokio::sync::OnceCell;
use tracing::{debug, error, info, warn};

/// Mirror operation errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("Cannot mirror: destination archive ends at ledger {dest_ledger} (checkpoint 0x{dest_checkpoint:08x}) but --low is {low_ledger} (checkpoint 0x{low_checkpoint:08x}). This would create a gap in the archive. Use --allow-mirror-gaps to proceed anyway.")]
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
        // Even though HTTP destinations aren't writable, we need to provide retry config
        // to avoid assertion failure when checking if destination supports writes
        let retry_config = crate::storage::HttpRetryConfig {
            max_retries: 3,
            initial_backoff_ms: 100,
        };
        let dst_op = crate::storage::StorageBackend::from_url(dst, Some(retry_config))
            .await
            .map_err(|e| match e {
                crate::storage::Error::Io(io_err) => io_err,
            })?;

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
                // Try to read the destination's .well-known file
                match fetch_well_known_history_file(&self.dst_op).await {
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
                        "Updating .well-known from checkpoint {:08x} to {:08x}",
                        existing_checkpoint, highest_checkpoint
                    );
                    true
                } else {
                    info!(
                        "Keeping existing .well-known at checkpoint {:08x} (mirrored up to {:08x})",
                        existing_checkpoint, highest_checkpoint
                    );
                    false
                }
            }
            None => {
                // No existing .well-known file - create a new one
                info!(
                    "No existing .well-known, creating new one at checkpoint {:08x}",
                    highest_checkpoint
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
            if !src_file.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "Cannot update .well-known: history file at checkpoint {:08x} was not successfully mirrored ({})",
                        highest_checkpoint,
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
                "Updated destination .well-known to checkpoint {:08x}",
                highest_checkpoint
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

        let dest_checkpoint_opt = self.get_initial_dest_well_known_checkpoint().await;
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
                            "WARNING: Creating gap in archive! Destination ends at ledger {} (checkpoint 0x{:08x}) but mirroring from {} (checkpoint 0x{:08x})",
                            dest_ledger, dest_checkpoint, requested_low, requested_checkpoint
                        );
                    }

                    // Start at --low value
                    Some(requested_low)
                } else {
                    // Destination has checkpoint >= --low request
                    if self.overwrite {
                        // In overwrite mode, honor the --low request
                        info!(
                            "Destination already has checkpoint 0x{:08x} (ledger {}), overwriting from ledger {} (checkpoint 0x{:08x})",
                            dest_checkpoint, dest_ledger, requested_low, requested_checkpoint
                        );
                        Some(requested_low)
                    } else {
                        // Not in overwrite mode - ignore --low and resume from destination
                        warn!(
                            "Ignoring --low {} since destination already has checkpoint 0x{:08x} (ledger {}). Resuming from next checkpoint.",
                            requested_low, dest_checkpoint, dest_ledger
                        );
                        // Resume from the next checkpoint after what's already mirrored
                        Some(dest_ledger + 1)
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
            // No --low specified - mirror starting from last checkpoint in destination archive
            // (This applies whether --overwrite is set or not)
            if let Some(checkpoint_ledger) = dest_checkpoint_opt {
                info!(
                    "Found existing archive at destination with ledger {}, resuming from next checkpoint",
                    checkpoint_ledger
                );
                // Resume from the next checkpoint after what's already mirrored
                Some(checkpoint_ledger + 1)
            } else {
                debug!("Destination archive does not exist, starting from beginning");
                None
            }
        };

        compute_checkpoint_bounds(source, effective_low, self.high)
            .await
            .map_err(|e| crate::pipeline::Error::MirrorOperation(Error::Utils(e)))
    }

    async fn process_object(&self, path: &str, reader_result: ReaderResult) {
        let mut reader = match reader_result {
            ReaderResult::Ok(r) => r,
            ReaderResult::Err(e) => {
                // Source file couldn't be read - count as failure
                error!("Failed to read source file {}: {}", path, e);
                self.stats.record_failure(path).await;
                return;
            }
        };
        // Check if file exists and handle based on overwrite mode
        match self.dst_op.exists(path).await {
            Ok(true) => {
                if !self.overwrite {
                    debug!("Skipping existing file: {}", path);
                    self.stats.record_skipped(path);
                    return;
                } else {
                    // Proceed with overwrite
                    info!("Overwriting existing file: {}", path);
                }
            }
            Ok(false) => {
                // File doesn't exist, proceed with download
            }
            Err(e) => {
                // Failed to check existence - treat as error
                error!("Failed to check existence of file {}: {}", path, e);
                self.stats.record_failure(path).await;
                return;
            }
        }

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
            Ok(_) => {
                debug!("Successfully copied: {}", path);
                self.stats.record_success(path);
            }
            Err(e) => {
                error!("Failed to write file {}: {}", path, e);
                self.stats.record_failure(path).await;
            }
        }
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
