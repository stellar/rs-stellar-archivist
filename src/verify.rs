//! Bucket file verification module.
//!
//! Provides SHA256 hash verification for bucket files. The hash is embedded in the
//! filename and verified against the decompressed content.

use crate::history_format::bucket_hash_from_path;
use crate::storage::{from_opendal_error, Error as StorageError, StorageRef};
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use futures_util::StreamExt;
use opendal::{Reader, Writer};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, BufReader};
use tracing::debug;

const HASH_BUFFER_SIZE: usize = 64 * 1024;
const CHANNEL_CAPACITY: usize = 64;

/// Decompress and hash the given reader's content and verify it against the expected hash.
/// If `writer` is provided, compressed bytes are written to it while verifying.
async fn verify_bucket_internal(
    path: &str,
    reader: Reader,
    writer: Option<Writer>,
) -> Result<(), StorageError> {
    use futures_util::SinkExt;

    let expected = bucket_hash_from_path(path)
        .ok_or_else(|| StorageError::fatal(format!("Invalid bucket path: {}", path)))?;

    let stream = reader
        .into_stream(..)
        .await
        .map_err(|e| from_opendal_error(e, &format!("Failed to create stream for {}", path)))?;

    // Channel to feed compressed bytes to the hasher
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_CAPACITY);

    let hash_task = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let stream = stream.map(Ok::<_, std::io::Error>);
        let stream_reader = tokio_util::io::StreamReader::new(stream);
        let mut decoder = GzipDecoder::new(BufReader::new(stream_reader));

        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; HASH_BUFFER_SIZE];

        loop {
            let n = decoder.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        Ok::<_, std::io::Error>(hex::encode(hasher.finalize()))
    });

    futures_util::pin_mut!(stream);
    let mut sink = writer.map(|w| w.into_sink());
    let mut streaming_error: Option<StorageError> = None;

    while let Some(result) = stream.next().await {
        match result {
            Ok(buffer) => {
                // Write to destination if provided
                if let Some(ref mut s) = sink {
                    if let Err(e) = s.send(buffer.clone()).await {
                        streaming_error = Some(from_opendal_error(
                            e,
                            &format!("Failed to write to {}", path),
                        ));
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

    let actual = hash_task
        .await
        .map_err(|e| StorageError::fatal(format!("Hash task panicked for {}: {}", path, e)))?
        .map_err(|e| StorageError::retry(format!("Failed to decompress {}: {}", path, e)))?;

    if actual != expected {
        // Don't close sink - avoids committing corrupt data on atomic backends
        return Err(StorageError::fatal(format!(
            "Hash mismatch for {}: expected {}, got {}",
            path, expected, actual
        )));
    }

    if let Some(mut s) = sink {
        s.close()
            .await
            .map_err(|e| from_opendal_error(e, &format!("Failed to close {}", path)))?;
    }

    Ok(())
}

/// Verify a bucket file's hash (scan operation).
pub async fn verify_bucket_stream(path: &str, reader: Reader) -> Result<(), StorageError> {
    debug!("Verifying bucket hash for {}", path);
    verify_bucket_internal(path, reader, None).await
}

/// Verify and write a bucket file (mirror operation).
/// Hash is verified before committing the write.
pub async fn verify_and_write_bucket(
    path: &str,
    reader: Reader,
    dst_store: &StorageRef,
) -> Result<(), StorageError> {
    debug!("Verifying and writing bucket {}", path);
    let writer = dst_store.open_writer(path).await?;
    verify_bucket_internal(path, reader, Some(writer)).await
}
