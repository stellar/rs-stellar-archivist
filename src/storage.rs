//! Storage backends for accessing Stellar History Archives
//!
//! All storage backends are built on Apache OpenDAL, providing:
//! - Unified interface across file, HTTP, and cloud storage
//! - Built-in retry with exponential backoff
//! - Request timeouts
//! - Concurrent request limiting
//! - Request logging

use crate::retryable_error_layer::RetryableErrorLayer;
use async_trait::async_trait;
use futures_util::SinkExt;
use futures_util::StreamExt;
use opendal::{layers, Buffer, ErrorKind, Operator, Reader, Writer};
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

    pub fn not_found() -> Self {
        Self {
            class: ErrorClass::NotFound,
            message: "File not found".into(),
        }
    }
}

pub type StorageRef = Arc<dyn Storage + Send + Sync>;

/// Core unified storage trait for all backends
#[async_trait]
pub trait Storage: Send + Sync {
    /// Open an OpenDAL reader for the object with buffering enabled
    async fn open_reader(&self, object: &str) -> Result<Reader, Error>;

    /// Check if object exists
    async fn exists(&self, object: &str) -> Result<bool, Error>;

    /// Open an OpenDAL writer for the object with buffering enabled.
    /// Only supported by writable backends (e.g., filesystem).
    /// Caller is responsible for calling `writer.close()` after writing.
    async fn open_writer(&self, _object: &str) -> Result<Writer, Error> {
        Err(Error::fatal("Write not supported by this backend"))
    }

    /// Write an entire buffer to an object.
    /// This is a convenience method that opens a writer, writes, and closes.
    /// Only supported by writable backends (e.g., filesystem).
    async fn write(&self, object: &str, data: Buffer) -> Result<(), Error> {
        let mut writer = self.open_writer(object).await?;
        writer
            .write(data)
            .await
            .map_err(|e| Error::retry(format!("Failed to write to {}: {}", object, e)))?;
        writer
            .close()
            .await
            .map_err(|e| Error::retry(format!("Failed to close writer for {}: {}", object, e)))?;
        Ok(())
    }

    /// Copy data from a source reader to a destination object.
    /// Streams data in chunks without buffering the entire file in memory.
    /// Only supported by writable backends (e.g., filesystem).
    async fn copy_from_reader(&self, object: &str, reader: Reader) -> Result<(), Error> {
        let writer = self.open_writer(object).await?;

        // Convert reader to a stream of Buffer chunks (zero-copy)
        // The range `..` means read all data
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|e| Error::retry(format!("Failed to create stream for {}: {}", object, e)))?;

        let mut sink = writer.into_sink();
        sink.send_all(&mut stream)
            .await
            .map_err(|e| Error::retry(format!("Failed to write data to {}: {}", object, e)))?;
        sink.close()
            .await
            .map_err(|e| Error::retry(format!("Failed to close writer for {}: {}", object, e)))?;
        Ok(())
    }

    /// Check if this backend supports write operations
    fn supports_writes(&self) -> bool {
        false
    }

    /// Get the base filesystem path if this is a filesystem backend
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
    /// When false, bypasses OpenDAL and writes directly via tokio::fs for better performance.
    pub atomic_file_writes: bool,
}

impl StorageConfig {
    /// Create a new StorageConfig with explicit values
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
            max_concurrent,
            timeout,
            io_timeout,
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
    /// Whether to use atomic file writes (OpenDAL with fsync) vs direct writes (tokio::fs bypass)
    atomic_file_writes: bool,
}

impl OpendalStore {
    /// Create a new OpendalStore from a configured operator
    fn from_operator(
        operator: Operator,
        prefix: impl Into<String>,
        root_path: Option<PathBuf>,
        writable: bool,
        atomic_file_writes: bool,
    ) -> Self {
        Self {
            operator,
            prefix: prefix.into(),
            root_path,
            writable,
            atomic_file_writes,
        }
    }

    /// Apply standard layers to an operator builder
    fn apply_layers<B: opendal::Builder>(
        builder: B,
        config: &StorageConfig,
    ) -> Result<Operator, Error> {
        Self::apply_layers_with_http_client(builder, config, None)
    }

    /// Apply standard layers to an operator builder with optional custom HTTP client
    fn apply_layers_with_http_client<B: opendal::Builder>(
        builder: B,
        config: &StorageConfig,
        http_client: Option<opendal::raw::HttpClient>,
    ) -> Result<Operator, Error> {
        // Build operator with layers
        // IMPORTANT: TimeoutLayer must come BEFORE RetryLayer per OpenDAL docs
        // Order of layers (innermost to outermost):
        //   Service -> HttpClient -> Timeout -> RetryableError -> Retry -> ConcurrentLimit -> Logging
        // Note: RetryableErrorLayer must come before RetryLayer to mark non-standard 5xx codes as
        //       temporary before the retry decision is made.
        // Note: ThrottleLayer is applied at the end since it needs the final Operator type
        let op = Operator::new(builder)
            .map_err(|e| Error::fatal(format!("Failed to create operator: {}", e)))?
            .layer(
                layers::TimeoutLayer::default()
                    .with_timeout(config.timeout)
                    .with_io_timeout(config.io_timeout),
            )
            .layer(RetryableErrorLayer::new())
            .layer(
                layers::RetryLayer::new()
                    .with_max_times(config.max_retries)
                    .with_min_delay(config.retry_min_delay)
                    .with_max_delay(config.retry_max_delay)
                    .with_jitter(),
            )
            .layer(layers::ConcurrentLimitLayer::new(config.max_concurrent))
            .layer(layers::LoggingLayer::default())
            .finish();

        // Apply custom HTTP client if provided (must be applied after finish() for dynamic dispatch)
        let op = if let Some(client) = http_client {
            op.layer(layers::HttpClientLayer::new(client))
        } else {
            op
        };

        // Add bandwidth throttling if configured (applied at the end on the finished Operator)
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
    fn object_to_key(&self, object: &str) -> String {
        let object = object.trim_start_matches('/');
        if self.prefix.is_empty() {
            object.to_string()
        } else {
            let prefix = self.prefix.trim_end_matches('/');
            format!("{}/{}", prefix, object)
        }
    }

    /// Direct file write that bypasses OpenDAL's writer layer.
    /// This avoids the WriteGenerator buffering and the fsync in close().
    /// Used when fsync_file_writes is false for filesystem backends.
    async fn copy_from_reader_direct(
        &self,
        root_path: &Path,
        object: &str,
        reader: Reader,
    ) -> Result<(), Error> {
        let object = object.trim_start_matches('/');
        let file_path = root_path.join(object);

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Error::retry(format!("Failed to create directory {:?}: {}", parent, e))
            })?;
        }

        // Open file directly with tokio::fs
        let mut file = tokio::fs::File::create(&file_path)
            .await
            .map_err(|e| Error::retry(format!("Failed to create file {:?}: {}", file_path, e)))?;

        // Stream data from reader to file
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|e| Error::retry(format!("Failed to create stream for {}: {}", object, e)))?;

        while let Some(result) = stream.next().await {
            let buffer =
                result.map_err(|e| Error::retry(format!("Failed to read from source: {}", e)))?;
            for bytes in buffer {
                file.write_all(&bytes).await.map_err(|e| {
                    Error::retry(format!("Failed to write to {:?}: {}", file_path, e))
                })?;
            }
        }

        // Flush to OS buffers (but no fsync)
        file.flush()
            .await
            .map_err(|e| Error::retry(format!("Failed to flush {:?}: {}", file_path, e)))?;

        Ok(())
    }

    // ===== Filesystem Backend =====

    /// Create a filesystem storage backend
    ///
    /// When atomic_file_writes is enabled, uses OpenDAL's atomic_write_dir feature
    /// to ensure writes are atomic (temp file + rename).
    pub fn filesystem(root: impl Into<PathBuf>, config: &StorageConfig) -> Result<Self, Error> {
        use opendal::services::Fs;

        let root_path: PathBuf = root.into();
        let root_str = root_path.to_string_lossy().to_string();

        // Conditionally use atomic_write_dir based on config
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

    /// Create a custom HTTP client with proper User-Agent header
    fn create_http_client() -> Result<opendal::raw::HttpClient, Error> {
        let reqwest_client = reqwest::Client::builder()
            .user_agent(Self::USER_AGENT)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .map_err(|e| Error::fatal(format!("Failed to create HTTP client: {}", e)))?;

        Ok(opendal::raw::HttpClient::with(reqwest_client))
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
            .map_err(|e| Error::fatal(format!("Invalid URL {}: {}", base_url, e)))?;

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

        // Create custom HTTP client with proper User-Agent
        let http_client = Self::create_http_client()?;

        let operator = Self::apply_layers_with_http_client(builder, config, Some(http_client))?;

        Ok(Self::from_operator(
            operator, "", None, false, false, // HTTP is read-only
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

        Ok(Self::from_operator(operator, prefix, None, false, false))
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

        Ok(Self::from_operator(operator, prefix, None, false, false))
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

        Ok(Self::from_operator(operator, prefix, None, false, false))
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

        Ok(Self::from_operator(operator, prefix, None, false, false))
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

    /// Create an OpenStack Swift storage backend
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

/// Classify an OpenDAL error into our ErrorClass
fn classify_opendal_error(err: &opendal::Error) -> ErrorClass {
    match err.kind() {
        ErrorKind::NotFound => ErrorClass::NotFound,
        // Retryable errors
        ErrorKind::RateLimited | ErrorKind::Unexpected => ErrorClass::Retry,
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
        // Default to retry for unknown errors
        _ => ErrorClass::Retry,
    }
}

#[async_trait]
impl Storage for OpendalStore {
    async fn open_reader(&self, object: &str) -> Result<Reader, Error> {
        let key = self.object_to_key(object);
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
                message: format!("Failed to open reader for {}: {}", key, e),
            }
        })?;

        Ok(reader)
    }

    async fn exists(&self, object: &str) -> Result<bool, Error> {
        let key = self.object_to_key(object);

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
                    message: format!("Failed to check existence of {}: {}", key, e),
                })
            }
        }
    }

    async fn open_writer(&self, object: &str) -> Result<Writer, Error> {
        if !self.writable {
            return Err(Error::fatal("Write not supported by this backend"));
        }

        let key = self.object_to_key(object);

        // Use writer_with to enable buffered/chunked writing for better performance
        // All backends write through OpenDAL, which applies ConcurrentLimitLayer
        // Filesystem backends use atomic_write_dir for atomic writes (temp file + rename)
        let writer = self.operator.writer_with(&key).await.map_err(|e| {
            let class = classify_opendal_error(&e);
            Error {
                class,
                message: format!("Failed to open writer for {}: {}", key, e),
            }
        })?;

        Ok(writer)
    }

    async fn copy_from_reader(&self, object: &str, reader: Reader) -> Result<(), Error> {
        // If we have a filesystem root and atomic writes are disabled, use direct tokio::fs writes
        // to bypass OpenDAL's WriteGenerator buffering and avoid the fsync in close().
        if let Some(root_path) = &self.root_path {
            if !self.atomic_file_writes {
                return self
                    .copy_from_reader_direct(root_path, object, reader)
                    .await;
            }
        }

        // Otherwise use OpenDAL's writer (required for non-filesystem backends or when atomic writes are enabled)
        let writer = self.open_writer(object).await?;
        // Convert reader to a stream of Buffer chunks (zero-copy)
        // The range `..` means read all data
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|e| Error::retry(format!("Failed to create stream for {}: {}", object, e)))?;

        let mut sink = writer.into_sink();
        sink.send_all(&mut stream)
            .await
            .map_err(|e| Error::retry(format!("Failed to write data to {}: {}", object, e)))?;

        // Always call close() when using OpenDAL writer - it's required to flush internal buffers
        sink.close()
            .await
            .map_err(|e| Error::retry(format!("Failed to close writer for {}: {}", object, e)))?;
        Ok(())
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
/// - `s3://bucket/prefix` - AWS S3 (uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_ENDPOINT env vars)
/// - `gcs://bucket/prefix` - Google Cloud Storage (uses GOOGLE_APPLICATION_CREDENTIALS env var)
/// - `azblob://container/prefix` - Azure Blob Storage (uses AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY env vars)
/// - `b2://bucket/prefix` - Backblaze B2 (uses B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET_ID env vars)
/// - `swift://container/prefix` - OpenStack Swift (uses SWIFT_ENDPOINT, SWIFT_TOKEN env vars)
/// - `sftp://[user@]host[:port]/path` - SFTP (uses SFTP_USER, SFTP_KEY env vars)

/// Create a backend from a URL string with configuration
pub async fn from_url_with_config(
    url_str: &str,
    config: &StorageConfig,
) -> Result<StorageRef, Error> {
    use url::Url;

    let url = Url::parse(url_str)
        .map_err(|e| Error::fatal(format!("Failed to parse URL '{}': {}", url_str, e)))?;

    match url.scheme() {
        "file" => {
            let path = url.path().to_string();
            tracing::debug!(
                "Creating filesystem store with path: {} (from URL: {})",
                path,
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
                Error::fatal(format!("S3 URL must have a bucket name: {}", url_str))
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
                Error::fatal(format!("GCS URL must have a bucket name: {}", url_str))
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
                    "Azure Blob URL must have a container name: {}",
                    url_str
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
                Error::fatal(format!("B2 URL must have a bucket name: {}", url_str))
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
                Error::fatal(format!("Swift URL must have a container name: {}", url_str))
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
                .ok_or_else(|| Error::fatal(format!("SFTP URL must have a host: {}", url_str)))?;
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
            let endpoint = format!("{}:{}", host, port);

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
        scheme => Err(Error::fatal(format!("Unsupported URL scheme: {}", scheme))),
    }
}
