//! Storage backends for accessing Stellar History Archives
//!
//! All storage backends are built on Apache `OpenDAL`, providing:
//! - Unified interface across file, HTTP, and cloud storage
//! - Built-in retry with exponential backoff
//! - Request timeouts
//! - Concurrent request limiting
//! - Request logging

use async_trait::async_trait;
use futures_util::StreamExt;
use normalize_path::NormalizePath;
use opendal::{
    layers, Buffer, ErrorKind, HttpTransporter, OperationContext, Operator, Reader, Writer,
};
use opendal_http_transport_reqwest::ReqwestTransport;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

/// Classification of errors for retry decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Transient error - worth retrying (503, 502, timeouts, connection errors)
    Retry,
    /// Fatal error - don't retry (403, invalid data, etc.)
    Fatal,
    /// File not found - don't retry, but may need special handling
    NotFound,
}

/// Storage-related errors with retry classification
#[derive(Error, Debug)]
#[error("{message}")]
pub struct Error {
    pub class: ErrorClass,
    pub message: String,
}

impl Error {
    pub fn retry(message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::Retry,
            message: message.into(),
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::Fatal,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn not_found() -> Self {
        Self {
            class: ErrorClass::NotFound,
            message: "File not found".into(),
        }
    }
}

pub type StorageRef = Arc<dyn Storage + Send + Sync>;

/// Archive object paths are relative, `/`-separated, and built only from
/// `[A-Za-z0-9._-]` segments, so no path can resolve outside the store root
/// on any platform.
fn validate_object_path(object: &str) -> Result<(), Error> {
    let valid = object.split('/').all(|segment| {
        !matches!(segment, "" | "." | "..")
            && segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    });
    if valid {
        Ok(())
    } else {
        Err(Error::fatal(format!(
            "Invalid archive object path: {object}"
        )))
    }
}

/// Upload part/block size for object-store writes. Meets every backend's
/// minimum part size (5 MiB on S3/GCS/B2), keeps large files under per-object
/// part-count caps (50,000 blocks on Azure, 10,000 parts elsewhere), and
/// bounds per-writer buffering.
const OBJECT_STORE_WRITE_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// A staged write: the data being written is not visible at the final object
/// path until `commit()` succeeds; `abort()` (or dropping without commit)
/// leaves the final path unchanged. Only an explicit `abort()` also cleans up
/// staged data — a bare drop can leave a `.tmp` sibling (plain fs) or an
/// unfinished multipart upload (object stores).
pub struct StagedWriter {
    inner: StagedInner,
    /// Final object path (archive-relative), for error messages.
    object: String,
    bytes_written: u64,
}

enum StagedInner {
    /// Backend commits atomically on `close()` — object stores and fs with
    /// `atomic_write_dir`. commit = close; abort = best-effort backend abort.
    AtomicOnClose { writer: Writer },
    /// Plain filesystem: direct `tokio::fs` writes to a `.tmp` sibling; commit
    /// = flush + rename; abort = remove the `.tmp`. No fsync or `OpenDAL`
    /// write buffering.
    FsStaged {
        file: tokio::fs::File,
        tmp_path: PathBuf,
        final_path: PathBuf,
    },
}

impl StagedWriter {
    fn atomic_on_close(writer: Writer, object: &str) -> Self {
        Self {
            inner: StagedInner::AtomicOnClose { writer },
            object: object.to_string(),
            bytes_written: 0,
        }
    }

    /// Stage at `<root>/<object>.tmp`, creating parent directories.
    async fn fs_staged(root: &Path, object: &str) -> Result<Self, Error> {
        validate_object_path(object)?;
        let final_path = root.join(object);
        let tmp_path = final_path.with_added_extension("tmp");

        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                from_io_error(
                    e,
                    &format!("Failed to create directory {}", parent.display()),
                )
            })?;
        }
        let file = tokio::fs::File::create(&tmp_path).await.map_err(|e| {
            from_io_error(
                e,
                &format!("Failed to create temp file {}", tmp_path.display()),
            )
        })?;

        Ok(Self {
            inner: StagedInner::FsStaged {
                file,
                tmp_path,
                final_path,
            },
            object: object.to_string(),
            bytes_written: 0,
        })
    }

    /// Append a chunk. Nothing becomes visible at the final path. On `Err` the
    /// caller must `abort()` (or drop) the writer.
    pub async fn write(&mut self, buffer: Buffer) -> Result<(), Error> {
        self.bytes_written += buffer.len() as u64;
        match &mut self.inner {
            StagedInner::AtomicOnClose { writer } => writer.write(buffer).await.map_err(|e| {
                from_opendal_error(e, &format!("Failed to write data to {}", self.object))
            }),
            StagedInner::FsStaged { file, tmp_path, .. } => {
                for bytes in buffer {
                    file.write_all(&bytes).await.map_err(|e| {
                        from_io_error(e, &format!("Failed to write to {}", tmp_path.display()))
                    })?;
                }
                Ok(())
            }
        }
    }

    /// Make the staged data visible at the final path. Returns bytes written.
    pub async fn commit(self) -> Result<u64, Error> {
        match self.inner {
            StagedInner::AtomicOnClose { mut writer } => {
                if let Err(e) = writer.close().await {
                    // Best-effort backend abort; always return the original
                    // close error.
                    if let Err(abort_err) = writer.abort().await {
                        tracing::warn!(
                            "failed to abort staged upload for {} after a failed close: {}",
                            self.object,
                            abort_err
                        );
                    }
                    return Err(from_opendal_error(
                        e,
                        &format!("Failed to close writer for {}", self.object),
                    ));
                }
            }
            StagedInner::FsStaged {
                mut file,
                tmp_path,
                final_path,
            } => {
                let flushed = file.flush().await.map_err(|e| {
                    from_io_error(e, &format!("Failed to flush {}", tmp_path.display()))
                });
                drop(file); // close the handle before renaming
                if let Err(e) = flushed {
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return Err(e);
                }
                if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return Err(from_io_error(
                        e,
                        &format!(
                            "Failed to rename {} to {}",
                            tmp_path.display(),
                            final_path.display()
                        ),
                    ));
                }
            }
        }
        Ok(self.bytes_written)
    }

    /// Discard the staged data, leaving the final path unchanged. Best-effort,
    /// infallible.
    pub async fn abort(self) {
        match self.inner {
            StagedInner::AtomicOnClose { mut writer } => {
                if let Err(e) = writer.abort().await {
                    tracing::warn!(
                        "failed to discard staged upload for {} on abort: {}",
                        self.object,
                        e
                    );
                }
            }
            StagedInner::FsStaged { file, tmp_path, .. } => {
                drop(file);
                if let Err(e) = tokio::fs::remove_file(&tmp_path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(
                            "failed to remove staged temp {} after abort: {}",
                            tmp_path.display(),
                            e
                        );
                    }
                }
            }
        }
    }
}

/// Core unified storage trait for all backends
#[async_trait]
pub trait Storage: Send + Sync {
    /// Open an `OpenDAL` reader for the object.
    /// Note: No automatic retries - retries are handled at the pipeline level.
    async fn open_reader(&self, object: &str) -> Result<Reader, Error>;

    /// Check if object exists.
    /// Note: No automatic retries - retries are handled at the pipeline level.
    async fn exists(&self, object: &str) -> Result<bool, Error>;

    /// Open a staged write to `object`: bytes become visible at the final
    /// path only on `commit()`. Only supported by writable backends.
    async fn open_staged_writer(&self, _object: &str) -> Result<StagedWriter, Error> {
        Err(Error::fatal("Write not supported by this backend"))
    }

    /// Write an entire buffer to an object.
    /// Only supported by writable backends (filesystem and cloud object stores).
    async fn write(&self, object: &str, data: Buffer) -> Result<(), Error> {
        let mut writer = self.open_staged_writer(object).await?;
        if let Err(e) = writer.write(data).await {
            writer.abort().await;
            return Err(e);
        }
        writer.commit().await?;
        Ok(())
    }

    /// Copy data from a source reader to a destination object, streaming in
    /// chunks. Nothing is visible at the destination on failure.
    /// Only supported by writable backends (filesystem and cloud object stores).
    async fn copy_from_reader(&self, object: &str, reader: Reader) -> Result<(), Error> {
        let copy_phase = crate::phase!(crate::metrics::Phase::Copy);
        let mut writer = self.open_staged_writer(object).await?;

        let mut stream = match reader.into_stream(..).await {
            Ok(s) => s,
            Err(e) => {
                writer.abort().await;
                return Err(from_opendal_error(
                    e,
                    &format!("Failed to create stream for {object}"),
                ));
            }
        };

        while let Some(result) = stream.next().await {
            let buffer = match result {
                Ok(b) => b,
                Err(e) => {
                    writer.abort().await;
                    return Err(from_opendal_error(
                        e,
                        &format!("Failed to read data for {object}"),
                    ));
                }
            };
            if let Err(e) = writer.write(buffer).await {
                writer.abort().await;
                return Err(e);
            }
        }

        let copied_bytes = writer.commit().await?;
        copy_phase.record_file(copied_bytes);
        Ok(())
    }

    /// Check if this backend supports write operations
    fn supports_writes(&self) -> bool {
        false
    }

    /// Get the base filesystem path if this is a filesystem backend.
    /// Introspection only — production write paths never consult this.
    fn get_base_path(&self) -> Option<&std::path::Path> {
        None
    }
}

// ===== Configuration =====

/// Configuration for storage layers
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Maximum number of retry attempts for transient errors
    pub max_retries: usize,
    /// Minimum delay between retries
    pub retry_min_delay: Duration,
    /// Maximum delay between retries
    pub retry_max_delay: Duration,
    /// Timeout for non-IO operations (stat, delete, etc.)
    pub timeout: Duration,
    /// Timeout for IO operations (read, write)
    pub io_timeout: Duration,
    /// Maximum concurrent requests
    pub max_concurrent: usize,
    /// Bandwidth limit in bytes per second (0 = unlimited)
    pub bandwidth_limit: u32,
    /// Use atomic file writes with fsync (write to temp file, fsync, then rename).
    /// When false, bypasses `OpenDAL` and writes directly via `tokio::fs` for better performance.
    pub atomic_file_writes: bool,
}

impl StorageConfig {
    /// Create a new `StorageConfig` with explicit values
    #[must_use]
    pub fn new(
        max_retries: usize,
        retry_min_delay: Duration,
        retry_max_delay: Duration,
        max_concurrent: usize,
        timeout: Duration,
        io_timeout: Duration,
        bandwidth_limit: u32,
        atomic_file_writes: bool,
    ) -> Self {
        Self {
            max_retries,
            retry_min_delay,
            retry_max_delay,
            timeout,
            io_timeout,
            max_concurrent,
            bandwidth_limit,
            atomic_file_writes,
        }
    }
}

// ===== Unified OpenDAL Backend =====

/// OpenDAL-based storage backend supporting all storage services
pub struct OpendalStore {
    operator: Operator,
    prefix: String,
    /// For filesystem backends, store the root path
    root_path: Option<PathBuf>,
    /// Whether this backend supports writes
    writable: bool,
    /// Whether writes are atomic (a failed write leaves nothing at the
    /// destination path): true for object stores and for filesystem stores
    /// using `atomic_write_dir`.
    atomic_writes: bool,
}

impl OpendalStore {
    /// Create a new `OpendalStore` from a configured operator
    pub(crate) fn from_operator(
        operator: Operator,
        prefix: impl Into<String>,
        root_path: Option<PathBuf>,
        writable: bool,
        atomic_writes: bool,
    ) -> Self {
        Self {
            operator,
            prefix: prefix.into(),
            root_path,
            writable,
            atomic_writes,
        }
    }

    /// Apply standard layers to an operator builder, and send every HTTP-based
    /// service's requests through our own client (see [`Self::create_http_client`]).
    /// Note: No `RetryLayer` - retries are handled at the pipeline level to avoid
    /// file corruption from partial writes during streaming operations.
    fn apply_layers<B: opendal::Builder>(
        builder: B,
        config: &StorageConfig,
    ) -> Result<Operator, Error> {
        // Build operator with layers
        // Order of layers (innermost to outermost):
        //   Service -> Timeout -> ConcurrentLimit -> Logging -> Throttle
        // Note: No RetryLayer - retries are handled at the pipeline level with proper error
        //       classification (see from_opendal_error)
        let transport = HttpTransporter::new(ReqwestTransport::new(Self::create_http_client()?));
        let op = Operator::new(builder)
            .map_err(|e| Error::fatal(format!("Failed to create operator: {e}")))?
            .with_context(OperationContext::new().with_http_transport(transport))
            .layer(
                layers::TimeoutLayer::default()
                    .with_timeout(config.timeout)
                    .with_io_timeout(config.io_timeout),
            )
            .layer(layers::ConcurrentLimitLayer::new(config.max_concurrent))
            .layer(layers::LoggingLayer::default());

        // Add bandwidth throttling if configured
        let op = if config.bandwidth_limit > 0 {
            // Burst is set to 2x bandwidth to allow some burstiness while still limiting overall throughput
            op.layer(layers::ThrottleLayer::new(
                config.bandwidth_limit,
                config.bandwidth_limit * 2,
            ))
        } else {
            op
        };

        Ok(op)
    }

    /// Convert an archive object path to the full key with prefix
    fn object_to_key(&self, object: &str) -> Result<String, Error> {
        validate_object_path(object)?;
        if self.prefix.is_empty() {
            Ok(object.to_string())
        } else {
            let prefix = self.prefix.trim_end_matches('/');
            Ok(format!("{prefix}/{object}"))
        }
    }

    // ===== Filesystem Backend =====

    /// Create a filesystem storage backend
    ///
    /// When `atomic_file_writes` is enabled, uses `OpenDAL`'s `atomic_write_dir` feature
    /// to ensure writes are atomic (temp file + rename).
    pub fn filesystem(root: impl Into<PathBuf>, config: &StorageConfig) -> Result<Self, Error> {
        use opendal::services::Fs;

        let root_path: PathBuf = root.into().normalize();
        let root_str = root_path.to_string_lossy().to_string();

        let builder = if config.atomic_file_writes {
            Fs::default().root(&root_str).atomic_write_dir(&root_str)
        } else {
            Fs::default().root(&root_str)
        };

        let operator = Self::apply_layers(builder, config)?;

        Ok(Self::from_operator(
            operator,
            "",
            Some(root_path),
            true, // filesystem is writable
            config.atomic_file_writes,
        ))
    }

    // ===== HTTP Backend =====

    /// User-Agent string for HTTP requests
    const USER_AGENT: &'static str = concat!("stellar-archivist/", env!("CARGO_PKG_VERSION"));

    /// Create the HTTP client used by every HTTP-based backend, with a proper
    /// User-Agent header and redirect following (up to 10 redirects)
    fn create_http_client() -> Result<reqwest::Client, Error> {
        // reqwest is built without a bundled TLS crypto provider, so use
        // rustls's ring provider. Installing fails only when a provider is
        // already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
        reqwest::Client::builder()
            .user_agent(Self::USER_AGENT)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|e| Error::fatal(format!("Failed to create HTTP client: {e}")))
    }

    /// Create an HTTP/HTTPS storage backend
    ///
    /// This backend configures a custom HTTP client with:
    /// - Proper User-Agent header for server compatibility
    /// - Redirect following (up to 10 redirects)
    pub fn http(base_url: &str, config: &StorageConfig) -> Result<Self, Error> {
        use opendal::services::Http;

        // Parse the URL to separate endpoint from root path
        // OpenDAL's HTTP service expects:
        // - endpoint: scheme + host[:port] (e.g., https://history.stellar.org)
        // - root: path portion (e.g., /prd/core-live/core_live_001)
        let url = url::Url::parse(base_url)
            .map_err(|e| Error::fatal(format!("Invalid URL {base_url}: {e}")))?;

        // Build endpoint with optional port
        let endpoint = if let Some(port) = url.port() {
            format!(
                "{}://{}:{}",
                url.scheme(),
                url.host_str().unwrap_or(""),
                port
            )
        } else {
            format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""))
        };
        let root = url.path();

        tracing::debug!("HTTP backend: endpoint={}, root={}", endpoint, root);

        let builder = Http::default().endpoint(&endpoint).root(root);
        let operator = Self::apply_layers(builder, config)?;

        Ok(Self::from_operator(
            operator, "", None, false, // HTTP is read-only
            false,
        ))
    }

    // ===== Cloud Storage Backends =====

    /// Create an S3 storage backend
    #[cfg(feature = "opendal-s3")]
    pub fn s3(
        bucket: &str,
        region: Option<&str>,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        prefix: impl Into<String>,
        config: &StorageConfig,
    ) -> Result<Self, Error> {
        use opendal::services::S3;

        let mut builder = S3::default().bucket(bucket);
        if let Some(region) = region {
            builder = builder.region(region);
        }
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(key) = access_key_id {
            builder = builder.access_key_id(key);
        }
        if let Some(secret) = secret_access_key {
            builder = builder.secret_access_key(secret);
        }

        let operator = Self::apply_layers(builder, config)?;
        Ok(Self::from_operator(operator, prefix, None, true, true))
    }

    /// Create a Google Cloud Storage backend
    #[cfg(feature = "opendal-gcs")]
    pub fn gcs(
        bucket: &str,
        credential: Option<&str>,
        credential_path: Option<&str>,
        prefix: impl Into<String>,
        config: &StorageConfig,
    ) -> Result<Self, Error> {
        use opendal::services::Gcs;

        let mut builder = Gcs::default().bucket(bucket);
        if let Some(cred) = credential {
            builder = builder.credential(cred);
        }
        if let Some(path) = credential_path {
            builder = builder.credential_path(path);
        }

        let operator = Self::apply_layers(builder, config)?;
        Ok(Self::from_operator(operator, prefix, None, true, true))
    }

    /// Create an Azure Blob Storage backend
    #[cfg(feature = "opendal-azblob")]
    pub fn azblob(
        container: &str,
        account_name: Option<&str>,
        account_key: Option<&str>,
        endpoint: Option<&str>,
        prefix: impl Into<String>,
        config: &StorageConfig,
    ) -> Result<Self, Error> {
        use opendal::services::Azblob;

        let mut builder = Azblob::default().container(container);
        if let Some(name) = account_name {
            builder = builder.account_name(name);
        }
        if let Some(key) = account_key {
            builder = builder.account_key(key);
        }
        if let Some(ep) = endpoint {
            builder = builder.endpoint(ep);
        }

        let operator = Self::apply_layers(builder, config)?;
        Ok(Self::from_operator(operator, prefix, None, true, true))
    }

    /// Create a Backblaze B2 storage backend
    #[cfg(feature = "opendal-b2")]
    pub fn b2(
        bucket: &str,
        bucket_id: &str,
        application_key_id: &str,
        application_key: &str,
        prefix: impl Into<String>,
        config: &StorageConfig,
    ) -> Result<Self, Error> {
        use opendal::services::B2;

        let builder = B2::default()
            .bucket(bucket)
            .bucket_id(bucket_id)
            .application_key_id(application_key_id)
            .application_key(application_key);

        let operator = Self::apply_layers(builder, config)?;
        Ok(Self::from_operator(operator, prefix, None, true, true))
    }

    /// Create an SFTP storage backend
    #[cfg(feature = "opendal-sftp")]
    pub fn sftp(
        endpoint: &str,
        user: Option<&str>,
        key: Option<&str>,
        root: Option<&str>,
        prefix: impl Into<String>,
        config: &StorageConfig,
    ) -> Result<Self, Error> {
        use opendal::services::Sftp;

        let mut builder = Sftp::default().endpoint(endpoint);
        if let Some(user) = user {
            builder = builder.user(user);
        }
        if let Some(key) = key {
            builder = builder.key(key);
        }
        if let Some(root) = root {
            builder = builder.root(root);
        }

        let operator = Self::apply_layers(builder, config)?;
        Ok(Self::from_operator(operator, prefix, None, false, false))
    }

    /// Create an `OpenStack` Swift storage backend.
    ///
    /// Read-only: opendal's Swift writer uploads an object in a single
    /// request and cannot stream archive-sized files.
    #[cfg(feature = "opendal-swift")]
    pub fn swift(
        container: &str,
        endpoint: &str,
        token: Option<&str>,
        prefix: impl Into<String>,
        config: &StorageConfig,
    ) -> Result<Self, Error> {
        use opendal::services::Swift;

        let mut builder = Swift::default().container(container).endpoint(endpoint);
        if let Some(token) = token {
            builder = builder.token(token);
        }

        let operator = Self::apply_layers(builder, config)?;
        Ok(Self::from_operator(operator, prefix, None, false, false))
    }
}

/// Check if an error message contains an HTTP status code and classify it.
/// Returns Some(ErrorClass) if an HTTP status was found, None otherwise.
fn classify_http_status_in_error(err_string: &str) -> Option<ErrorClass> {
    use crate::utils::{NON_STANDARD_RETRYABLE_HTTP_ERRORS, STANDARD_RETRYABLE_HTTP_ERRORS};

    // Check for retryable status codes first (5xx and special cases like 408, 429)
    for &(code, _) in STANDARD_RETRYABLE_HTTP_ERRORS
        .iter()
        .chain(NON_STANDARD_RETRYABLE_HTTP_ERRORS.iter())
    {
        if err_string.contains(&format!("status: {code}")) {
            return Some(ErrorClass::Retry);
        }
    }

    // Check for 4xx client errors (fatal, should not retry)
    for code in 400..500u16 {
        if err_string.contains(&format!("status: {code}")) {
            return Some(ErrorClass::Fatal);
        }
    }

    None
}

/// Classify an `OpenDAL` error into our `ErrorClass`
fn classify_opendal_error(err: &opendal::Error) -> ErrorClass {
    match err.kind() {
        ErrorKind::NotFound => ErrorClass::NotFound,
        ErrorKind::RateLimited => ErrorClass::Retry,
        // Fatal errors
        ErrorKind::Unsupported
        | ErrorKind::ConfigInvalid
        | ErrorKind::PermissionDenied
        | ErrorKind::IsSameFile
        | ErrorKind::NotADirectory
        | ErrorKind::IsADirectory
        | ErrorKind::AlreadyExists
        | ErrorKind::RangeNotSatisfied
        | ErrorKind::ConditionNotMatch => ErrorClass::Fatal,
        // For Unexpected and other errors, check the error message for HTTP status codes
        _ => {
            let err_string = err.to_string();
            classify_http_status_in_error(&err_string).unwrap_or(ErrorClass::Retry)
        }
    }
}

/// Convert an `OpenDAL` error to our Error type with proper classification
#[must_use]
pub fn from_opendal_error(err: opendal::Error, context: &str) -> Error {
    let class = classify_opendal_error(&err);
    Error {
        class,
        message: format!("{context}: {err}"),
    }
}

/// Convert a `std::io::Error` to our Error type with proper classification
#[must_use]
pub fn from_io_error(err: std::io::Error, context: &str) -> Error {
    let class = match err.kind() {
        std::io::ErrorKind::NotFound => ErrorClass::NotFound,
        std::io::ErrorKind::PermissionDenied => ErrorClass::Fatal,
        std::io::ErrorKind::AlreadyExists => ErrorClass::Fatal,
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => ErrorClass::Fatal,
        // Transient errors that may succeed on retry
        _ => ErrorClass::Retry,
    };
    Error {
        class,
        message: format!("{context}: {err}"),
    }
}

#[async_trait]
impl Storage for OpendalStore {
    async fn open_reader(&self, object: &str) -> Result<Reader, Error> {
        let key = self.object_to_key(object)?;
        tracing::debug!("open_reader: object={}, key={}", object, key);

        // Use plain reader() - the .chunk() option is for concurrent reading of
        // large files with known size, but HTTP responses with chunked transfer
        // encoding don't have a known content-length, so we use streaming instead.
        // The reader can then be converted to a stream via into_stream(..) which
        // provides zero-copy streaming of chunks as they arrive.
        let reader = self.operator.reader(&key).await.map_err(|e| {
            let class = classify_opendal_error(&e);
            Error {
                class,
                message: format!("Failed to open reader for {key}: {e}"),
            }
        })?;

        Ok(reader)
    }

    async fn exists(&self, object: &str) -> Result<bool, Error> {
        let key = self.object_to_key(object)?;

        match self.operator.stat(&key).await {
            Ok(metadata) => {
                // Check if the object has content
                if metadata.content_length() == 0 {
                    tracing::debug!("Object exists but is empty: {}", key);
                    Ok(false) // Treat empty files as non-existent
                } else {
                    Ok(true)
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => {
                let class = classify_opendal_error(&e);
                Err(Error {
                    class,
                    message: format!("Failed to check existence of {key}: {e}"),
                })
            }
        }
    }

    async fn open_staged_writer(&self, object: &str) -> Result<StagedWriter, Error> {
        if !self.writable {
            return Err(Error::fatal("Write not supported by this backend"));
        }
        if self.atomic_writes {
            let key = self.object_to_key(object)?;
            let mut writer_fut = self.operator.writer_with(&key);
            // Object stores need an explicit chunk size: without one, every
            // incoming stream chunk becomes its own upload part, and azblob
            // (no service-declared minimum) would exceed Azure's 50,000-block
            // cap on large files. Filesystem writers stream through unchanged.
            if self.root_path.is_none() {
                writer_fut = writer_fut.chunk(OBJECT_STORE_WRITE_CHUNK_SIZE);
            }
            let writer = writer_fut.await.map_err(|e| {
                let class = classify_opendal_error(&e);
                Error {
                    class,
                    message: format!("Failed to open writer for {key}: {e}"),
                }
            })?;
            Ok(StagedWriter::atomic_on_close(writer, object))
        } else {
            // Plain fs is the only non-atomic writable backend; it always has a root.
            let root = self.root_path.as_ref().ok_or_else(|| {
                Error::fatal("Non-atomic backend without a filesystem root cannot stage writes")
            })?;
            StagedWriter::fs_staged(root, object).await
        }
    }

    fn supports_writes(&self) -> bool {
        self.writable
    }

    fn get_base_path(&self) -> Option<&Path> {
        self.root_path.as_deref()
    }
}

// ===== URL-based Factory =====

/// Create a backend from a URL string with default configuration
/// Supports file://, http://, https://, and cloud storage schemes when features are enabled.
///
/// Cloud storage URL formats (requires corresponding opendal-* feature):
/// - `s3://bucket/prefix` - AWS S3 (uses `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_ENDPOINT` env vars)
/// - `gcs://bucket/prefix` - Google Cloud Storage (uses `GOOGLE_APPLICATION_CREDENTIALS` env var)
/// - `azblob://container/prefix` - Azure Blob Storage (uses `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY` env vars)
/// - `b2://bucket/prefix` - Backblaze B2 (uses `B2_APPLICATION_KEY_ID`, `B2_APPLICATION_KEY`, `B2_BUCKET_ID` env vars)
/// - `swift://container/prefix` - `OpenStack` Swift, read-only (uses `SWIFT_ENDPOINT`, `SWIFT_TOKEN` env vars)
/// - `sftp://[user@]host[:port]/path` - SFTP (uses `SFTP_USER`, `SFTP_KEY` env vars)
///
/// Creates a backend from a URL string with configuration.
pub fn from_url_with_config(url_str: &str, config: &StorageConfig) -> Result<StorageRef, Error> {
    use url::Url;

    let normalized_url_str = if url_str.starts_with("file://") {
        url_str.replace('\\', "/")
    } else {
        url_str.to_string()
    };

    let url = Url::parse(&normalized_url_str)
        .map_err(|e| Error::fatal(format!("Failed to parse URL '{url_str}': {e}")))?;

    match url.scheme() {
        "file" => {
            let path = file_url_to_path(&url, url_str)?;
            tracing::debug!(
                "Creating filesystem store with path: {} (from URL: {})",
                path.display(),
                url_str
            );
            let store = OpendalStore::filesystem(path, config)?;
            Ok(Arc::new(store))
        }
        "http" | "https" => {
            tracing::debug!("Creating HTTP store for URL: {}", url_str);
            let store = OpendalStore::http(url_str, config)?;
            Ok(Arc::new(store))
        }
        #[cfg(feature = "opendal-s3")]
        "s3" => {
            let bucket = url.host_str().ok_or_else(|| {
                Error::fatal(format!("S3 URL must have a bucket name: {url_str}"))
            })?;
            let prefix = url.path().trim_start_matches('/').to_string();

            let region = std::env::var("AWS_REGION").ok();
            let endpoint = std::env::var("S3_ENDPOINT").ok();
            let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok();
            let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();

            tracing::debug!(
                "Creating S3 store: bucket={}, prefix={}, region={:?}, endpoint={:?}",
                bucket,
                prefix,
                region,
                endpoint
            );

            let store = OpendalStore::s3(
                bucket,
                region.as_deref(),
                endpoint.as_deref(),
                access_key.as_deref(),
                secret_key.as_deref(),
                prefix,
                config,
            )?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-s3"))]
        "s3" => Err(Error::fatal(
            "S3 support not compiled in. Enable the 'opendal-s3' feature to use S3 URLs.",
        )),
        #[cfg(feature = "opendal-gcs")]
        "gcs" | "gs" => {
            let bucket = url.host_str().ok_or_else(|| {
                Error::fatal(format!("GCS URL must have a bucket name: {url_str}"))
            })?;
            let prefix = url.path().trim_start_matches('/').to_string();

            let credential_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();

            tracing::debug!(
                "Creating GCS store: bucket={}, prefix={}, credential_path={:?}",
                bucket,
                prefix,
                credential_path
            );

            let store =
                OpendalStore::gcs(bucket, None, credential_path.as_deref(), prefix, config)?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-gcs"))]
        "gcs" | "gs" => Err(Error::fatal(
            "GCS support not compiled in. Enable the 'opendal-gcs' feature to use GCS URLs.",
        )),
        #[cfg(feature = "opendal-azblob")]
        "azblob" | "azure" => {
            let container = url.host_str().ok_or_else(|| {
                Error::fatal(format!(
                    "Azure Blob URL must have a container name: {url_str}"
                ))
            })?;
            let prefix = url.path().trim_start_matches('/').to_string();

            let account_name = std::env::var("AZURE_STORAGE_ACCOUNT").ok();
            let account_key = std::env::var("AZURE_STORAGE_KEY").ok();
            let endpoint = std::env::var("AZURE_STORAGE_ENDPOINT").ok();

            tracing::debug!(
                "Creating Azure Blob store: container={}, prefix={}, account={:?}",
                container,
                prefix,
                account_name
            );

            let store = OpendalStore::azblob(
                container,
                account_name.as_deref(),
                account_key.as_deref(),
                endpoint.as_deref(),
                prefix,
                config,
            )?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-azblob"))]
        "azblob" | "azure" => Err(Error::fatal(
            "Azure Blob support not compiled in. Enable the 'opendal-azblob' feature to use Azure URLs.",
        )),
        #[cfg(feature = "opendal-b2")]
        "b2" => {
            let bucket = url.host_str().ok_or_else(|| {
                Error::fatal(format!("B2 URL must have a bucket name: {url_str}"))
            })?;
            let prefix = url.path().trim_start_matches('/').to_string();

            let bucket_id = std::env::var("B2_BUCKET_ID")
                .map_err(|_| Error::fatal("B2_BUCKET_ID environment variable must be set"))?;
            let app_key_id = std::env::var("B2_APPLICATION_KEY_ID").map_err(|_| {
                Error::fatal("B2_APPLICATION_KEY_ID environment variable must be set")
            })?;
            let app_key = std::env::var("B2_APPLICATION_KEY")
                .map_err(|_| Error::fatal("B2_APPLICATION_KEY environment variable must be set"))?;

            tracing::debug!("Creating B2 store: bucket={}, prefix={}", bucket, prefix);

            let store =
                OpendalStore::b2(bucket, &bucket_id, &app_key_id, &app_key, prefix, config)?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-b2"))]
        "b2" => Err(Error::fatal(
            "B2 support not compiled in. Enable the 'opendal-b2' feature to use B2 URLs.",
        )),
        #[cfg(feature = "opendal-swift")]
        "swift" => {
            let container = url.host_str().ok_or_else(|| {
                Error::fatal(format!("Swift URL must have a container name: {url_str}"))
            })?;
            let prefix = url.path().trim_start_matches('/').to_string();

            let endpoint = std::env::var("SWIFT_ENDPOINT")
                .map_err(|_| Error::fatal("SWIFT_ENDPOINT environment variable must be set"))?;
            let token = std::env::var("SWIFT_TOKEN").ok();

            tracing::debug!(
                "Creating Swift store: container={}, prefix={}, endpoint={}",
                container,
                prefix,
                endpoint
            );

            let store =
                OpendalStore::swift(container, &endpoint, token.as_deref(), prefix, config)?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-swift"))]
        "swift" => Err(Error::fatal(
            "Swift support not compiled in. Enable the 'opendal-swift' feature to use Swift URLs.",
        )),
        #[cfg(feature = "opendal-sftp")]
        "sftp" => {
            let host = url
                .host_str()
                .ok_or_else(|| Error::fatal(format!("SFTP URL must have a host: {url_str}")))?;
            let port = url.port().unwrap_or(22);
            let user = if url.username().is_empty() {
                std::env::var("SFTP_USER").ok()
            } else {
                Some(url.username().to_string())
            };
            let root = url.path().to_string();
            let root = if root.is_empty() {
                None
            } else {
                Some(root.as_str())
            };

            let key_path = std::env::var("SFTP_KEY").ok();

            // Build endpoint as host:port
            let endpoint = format!("{host}:{port}");

            tracing::debug!(
                "Creating SFTP store: endpoint={}, user={:?}, root={:?}, key={:?}",
                endpoint,
                user,
                root,
                key_path
            );

            let store = OpendalStore::sftp(
                &endpoint,
                user.as_deref(),
                key_path.as_deref(),
                root,
                "", // no additional prefix beyond root
                config,
            )?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-sftp"))]
        "sftp" => Err(Error::fatal(
            "SFTP support not compiled in. Enable the 'opendal-sftp' feature to use SFTP URLs.",
        )),
        scheme => Err(Error::fatal(format!("Unsupported URL scheme: {scheme}"))),
    }
}

fn file_url_to_path(url: &url::Url, url_str: &str) -> Result<PathBuf, Error> {
    if let Ok(path) = url.to_file_path() {
        return Ok(path);
    }

    if cfg!(windows) {
        if let Some(host) = url.host_str() {
            if host.len() == 1 {
                let fixed = format!("file:///{}:{}", host, url.path());
                if let Ok(fixed_url) = url::Url::parse(&fixed) {
                    if let Ok(path) = fixed_url.to_file_path() {
                        return Ok(path);
                    }
                }
                let mut path = String::with_capacity(host.len() + 1 + url.path().len());
                path.push_str(host);
                path.push(':');
                path.push_str(url.path());
                return Ok(PathBuf::from(path));
            }
        }
    }

    Err(Error::fatal(format!(
        "Invalid file URL '{url_str}': unable to convert to filesystem path"
    )))
}

/// Download a file into a buffer from a storage backend.
pub async fn download_buffer(store: &StorageRef, path: &str) -> Result<opendal::Buffer, Error> {
    use futures_util::TryStreamExt;

    let reader = store.open_reader(path).await?;
    let stream = reader
        .into_stream(..)
        .await
        .map_err(|e| from_opendal_error(e, "Stream error"))?;
    let chunks: Vec<opendal::Buffer> = stream
        .try_collect()
        .await
        .map_err(|e| from_opendal_error(e, "Read error"))?;
    Ok(chunks.into_iter().flatten().collect())
}
