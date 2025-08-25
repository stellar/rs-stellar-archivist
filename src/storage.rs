//! Storage backends for accessing Stellar History Archives

use async_trait::async_trait;
use futures_util::TryStreamExt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::io::StreamReader;

pub const WRITE_BUF_BYTES: usize = 128 * 1024;

/// Storage-related errors - primarily wraps IO errors
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),
}

pub type BoxedAsyncRead = Box<dyn tokio::io::AsyncRead + Send + Unpin + 'static>;
pub type BoxedAsyncWrite = Box<dyn tokio::io::AsyncWrite + Send + Unpin + 'static>;

pub type StorageRef = Arc<dyn Storage + Send + Sync>;

/// Result from attempting to get a reader for an object
pub enum ReaderResult {
    /// Successfully obtained a reader
    Ok(BoxedAsyncRead),
    /// Failed to get reader (file missing or inaccessible)
    Err(Error),
}

#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub size_bytes: Option<u64>,
}

/// Core unified storage trait for filesystem and HTTP backends
#[async_trait]
pub trait Storage: Send + Sync {
    /// Open an async reader for the object
    async fn open_reader(&self, object: &str) -> io::Result<BoxedAsyncRead>;

    /// Check if object exists
    async fn exists(&self, object: &str) -> io::Result<bool>;

    /// Open a writer for streaming data to an object (only supported by filesystem backend)
    async fn open_writer(&self, _object: &str) -> io::Result<BoxedAsyncWrite> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Write not supported by this backend",
        ))
    }

    /// Check if this backend supports write operations
    fn supports_writes(&self) -> bool {
        false
    }

    /// Get the base filesystem path if this is a filesystem backend
    fn get_base_path(&self) -> Option<&std::path::Path> {
        None // Default to None, only FileStore overrides this
    }
}

// ===== Filesystem Backend =====

pub struct FileStore {
    pub root_dir: PathBuf,
}

impl FileStore {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }

    /// Convert a relative archive path to an absolute filesystem path.
    /// Strips leading slashes and joins with the root directory.
    fn relative_to_absolute(&self, p: &str) -> PathBuf {
        self.root_dir.join(p.trim_start_matches('/'))
    }
}

#[async_trait]
impl Storage for FileStore {
    async fn open_reader(&self, object: &str) -> io::Result<BoxedAsyncRead> {
        let path = self.relative_to_absolute(object);
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(file))
    }

    async fn exists(&self, object: &str) -> io::Result<bool> {
        Ok(tokio::fs::try_exists(self.relative_to_absolute(object)).await?)
    }

    async fn open_writer(&self, object: &str) -> io::Result<BoxedAsyncWrite> {
        let full_path = self.relative_to_absolute(object);

        // Create parent directories if they don't exist
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = tokio::fs::File::create(full_path).await?;
        Ok(Box::new(file))
    }

    fn supports_writes(&self) -> bool {
        true // FileStore supports write operations
    }

    fn get_base_path(&self) -> Option<&std::path::Path> {
        Some(&self.root_dir)
    }
}

// ===== HTTP Backend =====

/// Create a new HTTP client. Use HTTP/2 for HTTPS, HTTP/1.1 for HTTP.
fn new_http_client(use_http2: bool) -> Result<reqwest::Client, Error> {
    use reqwest::header::*;

    // Set identity encoding to avoid decompression by reqwest since the payload itself is already compressed
    let mut h = HeaderMap::new();
    h.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));

    // Add User-Agent to identify stellar-archivist traffic
    // Format: stellar-archivist/<version> (platform; arch)
    let user_agent = format!(
        "stellar-archivist/{} ({} {}; Rust)",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    h.insert(USER_AGENT, HeaderValue::from_str(&user_agent).unwrap());

    let mut builder = reqwest::Client::builder()
        .pool_max_idle_per_host(1024)
        .pool_idle_timeout(std::time::Duration::from_secs(120))
        .tcp_nodelay(true)
        .default_headers(h);

    if use_http2 {
        // For HTTPS: allow HTTP/2 with adaptive window
        builder = builder
            .http2_adaptive_window(true)
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .http2_keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(30));
    } else {
        // For HTTP: force HTTP/1.1 only
        builder = builder.http1_only();
    }

    Ok(builder.build().map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to create HTTP client: {}", e),
        )
    })?)
}

#[derive(Clone, Debug)]
pub struct HttpRetryConfig {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
}

/// HTTP storage backend with streaming support
pub struct HttpStore {
    base_url: reqwest::Url,
    client: Arc<reqwest::Client>,
    retry_config: HttpRetryConfig,
}

impl HttpStore {
    pub fn new(mut base_url: reqwest::Url, retry_config: HttpRetryConfig) -> Self {
        // Ensure base URL ends with /
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        tracing::debug!(
            "Creating HttpStore with retry config: max_retries={}, initial_backoff_ms={}",
            retry_config.max_retries,
            retry_config.initial_backoff_ms
        );

        // Use HTTP/2 only for HTTPS URLs
        let use_http2 = base_url.scheme() == "https";
        let client = Arc::new(new_http_client(use_http2).expect("Failed to build HTTP client"));

        Self {
            base_url,
            client,
            retry_config,
        }
    }

    /// Convert an archive object path to a complete URL.
    /// i.e. "bucket/ab/cd/ef/bucket-hash.xdr.gz"
    /// -> "https://archive.stellar.org/bucket/ab/cd/ef/bucket-hash.xdr.gz"
    fn object_path_to_url(&self, object: &str) -> reqwest::Url {
        self.base_url
            .join(object.trim_start_matches('/'))
            .expect("Failed to join URL")
    }
}

fn to_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[async_trait]
impl Storage for HttpStore {
    async fn open_reader(&self, object: &str) -> io::Result<BoxedAsyncRead> {
        let mut attempt = 0;
        let mut backoff_ms = self.retry_config.initial_backoff_ms;
        let url = self.object_path_to_url(object);

        // Retry logic: We retry up to max_retries times with exponential backoff for:
        // - Network/connection errors
        // - HTTP 408, 429, 500, 502, 503, 504
        // We do NOT retry client errors (4xx) except 408/429
        // TODO: Currently handles retries for initial connection request, but not for streaming
        // We still need to implement better error handling for bucket files.
        loop {
            let request = self.client.get(url.clone());
            let result = request.send().await;

            match result {
                Ok(resp) => {
                    match resp.error_for_status() {
                        Ok(resp) => {
                            let stream = resp.bytes_stream().map_err(to_io_error);
                            let reader = StreamReader::new(stream);
                            return Ok(Box::new(reader) as BoxedAsyncRead);
                        }
                        Err(e) => {
                            let status = e.status();

                            // Determine if error is retryable
                            let is_retryable = status.map_or(false, |s| {
                                matches!(
                                    s.as_u16(),
                                    408 | // Request Timeout
                                    429 | // Too Many Requests
                                    500 | // Internal Server Error
                                    502 | // Bad Gateway
                                    503 | // Service Unavailable
                                    504 // Gateway Timeout
                                )
                            });

                            if !is_retryable || attempt >= self.retry_config.max_retries {
                                // Fatal error or max retries exceeded
                                if let Some(status) = status {
                                    return Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("HTTP {} for {}: {}", status, object, e),
                                    ));
                                } else {
                                    return Err(to_io_error(e));
                                }
                            }

                            // Retryable error, continue to backoff
                        }
                    }
                }
                Err(e) => {
                    // Network/connection error - always retryable
                    if attempt >= self.retry_config.max_retries {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "Failed to fetch {} after {} retries: {}",
                                object, self.retry_config.max_retries, e
                            ),
                        ));
                    }
                }
            }

            // Exponential backoff before retry
            attempt += 1;
            tracing::debug!(
                "Retrying {} (attempt {}/{}), backing off {}ms",
                object,
                attempt,
                self.retry_config.max_retries,
                backoff_ms
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(5000); // Cap at 5 seconds
        }
    }

    async fn exists(&self, object: &str) -> io::Result<bool> {
        let mut attempt = 0;
        let mut backoff_ms = self.retry_config.initial_backoff_ms;
        let url = self.object_path_to_url(object);

        // Retry logic: Use HEAD to check existence
        // - 2xx returns true (exists)
        // - 404 returns false (doesn't exist)
        // - All other statuses are retried up to max_retries times
        loop {
            let result = self.client.head(url.clone()).send().await;

            match result {
                Ok(resp) => {
                    let status = resp.status();
                    match status.as_u16() {
                        200..=299 => return Ok(true), // Object exists
                        404 => return Ok(false),      // Object definitely doesn't exist
                        _ => {
                            // Any other status (5xx, 4xx, etc.) - retry
                            if attempt >= self.retry_config.max_retries {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!(
                                        "Failed to check existence of {} (HTTP {})",
                                        object, status
                                    ),
                                ));
                            }
                        }
                    }
                }
                Err(e) => {
                    // Network error - retry
                    if attempt >= self.retry_config.max_retries {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "Failed to check existence of {} after {} retries: {}",
                                object, self.retry_config.max_retries, e
                            ),
                        ));
                    }
                }
            }

            // Exponential backoff before retry
            attempt += 1;
            tracing::debug!(
                "Retrying exists check for {} (attempt {}/{}), backing off {}ms",
                object,
                attempt,
                self.retry_config.max_retries,
                backoff_ms
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(5000); // Cap at 5 seconds
        }
    }
}

// ===== Backend Enum =====

pub enum StorageBackend {
    File(FileStore),
    Http(HttpStore),
}

impl StorageBackend {
    /// Create a backend from a URL string
    /// Supports file://, http://, and https:// schemes
    pub async fn from_url(
        url_str: &str,
        retry_config: Option<HttpRetryConfig>,
    ) -> Result<std::sync::Arc<dyn Storage + Send + Sync>, Error> {
        use std::sync::Arc;
        use url::Url;

        let url = Url::parse(url_str).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Failed to parse URL '{}': {}", url_str, e),
            )
        })?;

        match url.scheme() {
            "file" => {
                let path = url.path().to_string();

                tracing::debug!(
                    "Creating FileStore with path: {} (from URL: {})",
                    path,
                    url_str
                );

                let file_store = FileStore::new(path);
                let backend = StorageBackend::File(file_store);
                Ok(Arc::new(backend))
            }
            "http" | "https" => {
                assert!(retry_config.is_some());
                let http_store = HttpStore::new(url.clone(), retry_config.unwrap());
                let backend = StorageBackend::Http(http_store);
                Ok(Arc::new(backend))
            }
            "s3" => {
                // TODO: Implement S3 support
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Unsupported URL scheme: s3"),
                )
                .into())
            }
            scheme => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Unsupported URL scheme: {}", scheme),
            )
            .into()),
        }
    }
}

#[async_trait]
impl Storage for StorageBackend {
    async fn open_reader(&self, object: &str) -> io::Result<BoxedAsyncRead> {
        match self {
            StorageBackend::File(s) => s.open_reader(object).await,
            StorageBackend::Http(s) => s.open_reader(object).await,
        }
    }

    async fn exists(&self, object: &str) -> io::Result<bool> {
        match self {
            StorageBackend::File(s) => s.exists(object).await,
            StorageBackend::Http(s) => s.exists(object).await,
        }
    }

    async fn open_writer(&self, object: &str) -> io::Result<BoxedAsyncWrite> {
        match self {
            StorageBackend::File(s) => s.open_writer(object).await,
            StorageBackend::Http(s) => s.open_writer(object).await,
        }
    }

    fn supports_writes(&self) -> bool {
        match self {
            StorageBackend::File(s) => s.supports_writes(),
            StorageBackend::Http(s) => s.supports_writes(),
        }
    }

    fn get_base_path(&self) -> Option<&std::path::Path> {
        match self {
            StorageBackend::File(s) => s.get_base_path(),
            StorageBackend::Http(s) => s.get_base_path(),
        }
    }
}
