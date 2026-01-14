//! Storage backends for accessing Stellar History Archives

use async_trait::async_trait;
use futures_util::TryStreamExt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::io::StreamReader;

pub const WRITE_BUF_BYTES: usize = 128 * 1024;

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

pub type BoxedAsyncRead = Box<dyn tokio::io::AsyncRead + Send + Unpin + 'static>;
pub type BoxedAsyncWrite = Box<dyn tokio::io::AsyncWrite + Send + Unpin + 'static>;

pub type StorageRef = Arc<dyn Storage + Send + Sync>;

/// Core unified storage trait for filesystem and HTTP backends
#[async_trait]
pub trait Storage: Send + Sync {
    /// Open an async reader for the object
    async fn open_reader(&self, object: &str) -> Result<BoxedAsyncRead, Error>;

    /// Check if object exists
    async fn exists(&self, object: &str) -> Result<bool, Error>;

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
    async fn open_reader(&self, object: &str) -> Result<BoxedAsyncRead, Error> {
        let path = self.relative_to_absolute(object);
        match tokio::fs::File::open(&path).await {
            Ok(file) => Ok(Box::new(file)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(Error::not_found()),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Err(Error::fatal(format!(
                "Permission denied: {}",
                path.display()
            ))),
            Err(e) => Err(Error::retry(format!("IO error: {}", e))),
        }
    }

    async fn exists(&self, object: &str) -> Result<bool, Error> {
        let path = self.relative_to_absolute(object);
        match tokio::fs::metadata(&path).await {
            Ok(meta) => {
                // File exists - also verify it's not empty
                if meta.len() == 0 {
                    tracing::debug!("File exists but is empty: {}", path.display());
                    Ok(false) // Treat empty files as non-existent
                } else {
                    Ok(true)
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::retry(format!("IO error: {}", e))),
        }
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
        // No timeout - allow unlimited time for large file transfers
        // The connect_timeout still ensures we don't hang on connection attempts
        .connect_timeout(std::time::Duration::from_secs(10))
        .tcp_nodelay(true)
        .default_headers(h);

    // Common pool settings
    builder = builder
        .pool_max_idle_per_host(1024)
        .pool_idle_timeout(std::time::Duration::from_secs(120))
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)));

    if use_http2 {
        // For HTTPS: allow HTTP/2 with adaptive window
        builder = builder
            .http2_keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(30))
            .http2_adaptive_window(true);
    } else {
        // For HTTP: force HTTP/1.1 only
        builder = builder.http1_only();
    }

    builder
        .build()
        .map_err(|e| Error::fatal(format!("Failed to create HTTP client: {}", e)))
}

/// HTTP storage backend with streaming support
pub struct HttpStore {
    base_url: reqwest::Url,
    client: Arc<reqwest::Client>,
}

impl HttpStore {
    pub fn new(mut base_url: reqwest::Url) -> Self {
        // Ensure base URL ends with /
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        // Use HTTP/2 only for HTTPS URLs
        let use_http2 = base_url.scheme() == "https";
        let client = Arc::new(new_http_client(use_http2).expect("Failed to build HTTP client"));

        Self { base_url, client }
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

/// Classify an HTTP status code
fn classify_http_status(status: u16) -> ErrorClass {
    match status {
        // Not found
        404 => ErrorClass::NotFound,
        // Retryable server/proxy errors
        408 | // Request Timeout
        429 | // Too Many Requests
        500 | // Internal Server Error
        502 | // Bad Gateway
        503 | // Service Unavailable
        504 => ErrorClass::Retry, // Gateway Timeout
        // Everything else is fatal
        _ => ErrorClass::Fatal,
    }
}

fn to_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[async_trait]
impl Storage for HttpStore {
    async fn open_reader(&self, object: &str) -> Result<BoxedAsyncRead, Error> {
        let url = self.object_path_to_url(object);
        let request = self.client.get(url.clone());

        match request.send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => {
                    let stream = resp.bytes_stream().map_err(to_io_error);
                    let reader = StreamReader::new(stream);
                    Ok(Box::new(reader) as BoxedAsyncRead)
                }
                Err(e) => {
                    let status = e.status();
                    let class = status
                        .map(|s| classify_http_status(s.as_u16()))
                        .unwrap_or(ErrorClass::Retry);
                    let message = status
                        .map(|s| format!("HTTP {}", s))
                        .unwrap_or_else(|| e.to_string());
                    Err(Error { class, message })
                }
            },
            Err(e) => {
                // Network/connection error - always retryable
                Err(Error::retry(format!("Connection error: {}", e)))
            }
        }
    }

    async fn exists(&self, object: &str) -> Result<bool, Error> {
        let url = self.object_path_to_url(object);
        let result = self.client.head(url.clone()).send().await;

        match result {
            Ok(resp) => {
                let status = resp.status();
                match status.as_u16() {
                    200..=299 => {
                        // Check Content-Length to ensure file is not empty
                        let content_length = resp
                            .headers()
                            .get(reqwest::header::CONTENT_LENGTH)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0);

                        if content_length == 0 {
                            tracing::debug!(
                                "File exists but is empty (Content-Length: 0): {}",
                                object
                            );
                            Ok(false) // Treat empty files as non-existent
                        } else {
                            Ok(true) // Object exists and has content
                        }
                    }
                    404 => Ok(false), // Object definitely doesn't exist
                    status => {
                        // Classify based on status code
                        let class = classify_http_status(status);
                        Err(Error {
                            class,
                            message: format!("HTTP {} checking existence of {}", status, object),
                        })
                    }
                }
            }
            Err(e) => {
                // Network error - always retryable
                Err(Error::retry(format!("Connection error: {}", e)))
            }
        }
    }
}

/// Create a backend from a URL string
/// Supports file://, http://, and https:// schemes
pub async fn from_url(url_str: &str) -> Result<StorageRef, Error> {
    use std::sync::Arc;
    use url::Url;

    let url = Url::parse(url_str)
        .map_err(|e| Error::fatal(format!("Failed to parse URL '{}': {}", url_str, e)))?;

    match url.scheme() {
        "file" => {
            let path = url.path().to_string();

            tracing::debug!(
                "Creating FileStore with path: {} (from URL: {})",
                path,
                url_str
            );

            let file_store = FileStore::new(path);
            let store: StorageRef = Arc::new(file_store);
            Ok(store)
        }
        "http" | "https" => {
            let http_store = HttpStore::new(url.clone());
            let store: StorageRef = Arc::new(http_store);
            Ok(store)
        }
        scheme => Err(Error::fatal(format!("Unsupported URL scheme: {}", scheme))),
    }
}
