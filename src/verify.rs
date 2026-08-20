//! Bucket file verification module.
//!
//! Provides SHA256 hash verification for bucket files. The hash is embedded in the
//! filename and verified against the decompressed content.

use crate::history_format::bucket_hash_from_path;
use crate::storage::{from_opendal_error, Error as StorageError, StagedWriter, StorageRef};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use futures_util::StreamExt;
use opendal::Reader;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, BufReader};
use tracing::debug;

const HASH_BUFFER_SIZE: usize = 64 * 1024;
const CHANNEL_CAPACITY: usize = 64;

/// Decompress and hash the given reader's content and verify it against the expected hash,
/// returning the number of decompressed bytes.
///
/// If `staged` is provided, compressed bytes are streamed into it while verifying; the
/// staged write is not committed or aborted here — that is the caller's decision.
async fn verify_bucket_maybe_write(
    path: &str,
    reader: Reader,
    mut staged: Option<&mut StagedWriter>,
) -> Result<u64, StorageError> {
    let expected = bucket_hash_from_path(path)
        .ok_or_else(|| StorageError::fatal(format!("Invalid bucket path: {}", path)))?;

    let stream = reader
        .into_stream(..)
        .await
        .map_err(|e| from_opendal_error(e, &format!("Failed to create stream for {}", path)))?;

    // Channel to feed compressed bytes to the hasher
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_CAPACITY);

    let hash_task = tokio::spawn(async move {
        let _dg = crate::metrics::DecodeGuard::enter();
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let stream = stream.map(Ok::<_, std::io::Error>);
        let stream_reader = tokio_util::io::StreamReader::new(stream);
        let mut decoder = GzipDecoder::new(BufReader::new(stream_reader));

        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; HASH_BUFFER_SIZE];
        let mut decompressed_bytes: u64 = 0;

        loop {
            let n = decoder.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            decompressed_bytes += n as u64;
        }

        Ok::<_, std::io::Error>((hex::encode(hasher.finalize()), decompressed_bytes))
    });

    futures_util::pin_mut!(stream);
    let mut streaming_error: Option<StorageError> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok(buffer) => {
                // Write to destination if provided
                if let Some(w) = staged.as_deref_mut() {
                    if let Err(e) = w.write(buffer.clone()).await {
                        streaming_error = Some(e);
                        break;
                    }
                }
                // Send to hasher
                for chunk in buffer {
                    if tx.send(chunk).await.is_err() {
                        streaming_error =
                            Some(StorageError::fatal("Hash channel closed unexpectedly"));
                        break;
                    }
                }
                if streaming_error.is_some() {
                    break;
                }
            }
            Err(e) => {
                streaming_error = Some(from_opendal_error(
                    e,
                    &format!("Failed to read from {}", path),
                ));
                break;
            }
        }
    }

    drop(tx); // Signal EOF to hash task

    if let Some(err) = streaming_error {
        hash_task.abort();
        return Err(err);
    }

    let (actual, decompressed_bytes) = hash_task
        .await
        .map_err(|e| StorageError::fatal(format!("Hash task panicked for {}: {}", path, e)))?
        .map_err(|e| StorageError::retry(format!("Failed to decompress {}: {}", path, e)))?;

    if actual != expected {
        // Don't commit — caller aborts the staged write.
        return Err(StorageError::fatal(format!(
            "Hash mismatch: got {}",
            actual
        )));
    }

    Ok(decompressed_bytes)
}

/// Verify a bucket file's hash (scan operation) — stream + hash, no write.
pub async fn verify_bucket_stream(path: &str, reader: Reader) -> Result<(), StorageError> {
    debug!("Verifying bucket hash for {}", path);
    let bucket_phase = crate::phase!(crate::metrics::Phase::BucketStream);
    let decompressed_bytes = verify_bucket_maybe_write(path, reader, None).await?;
    bucket_phase.record_file(decompressed_bytes);
    Ok(())
}

/// Verify and write a bucket file (mirror/repair). The staged write is
/// committed only after the hash verifies; on any failure it is aborted,
/// so nothing becomes visible at the destination path.
pub async fn verify_and_write_bucket(
    path: &str,
    reader: Reader,
    dst_store: &StorageRef,
) -> Result<(), StorageError> {
    debug!("Verifying and writing bucket {}", path);
    // Opening the destination is not part of the streaming phase.
    let mut staged = dst_store.open_staged_writer(path).await?;
    // The phase spans streaming and the commit that makes the bytes visible;
    // the file is counted only once both have succeeded.
    let bucket_phase = crate::phase!(crate::metrics::Phase::BucketStream);
    match verify_bucket_maybe_write(path, reader, Some(&mut staged)).await {
        Ok(decompressed_bytes) => {
            staged.commit().await?;
            bucket_phase.record_file(decompressed_bytes);
            Ok(())
        }
        Err(e) => {
            staged.abort().await;
            Err(e)
        }
    }
}
