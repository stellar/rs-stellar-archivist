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

// ===== OpenDAL Backend =====

/// Check if any OpenDAL service feature is enabled
#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azblob",
    feature = "opendal-b2",
    feature = "opendal-sftp",
    feature = "opendal-swift"
))]
mod opendal_backend {
    use super::*;
    use opendal::{ErrorKind, Operator};
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    /// OpenDAL-based storage backend supporting multiple cloud storage services
    pub struct OpendalStore {
        operator: Operator,
        prefix: String,
    }

    impl OpendalStore {
        /// Create a new OpendalStore with the given operator and prefix
        pub fn new(operator: Operator, prefix: impl Into<String>) -> Self {
            Self {
                operator,
                prefix: prefix.into(),
            }
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

        /// Create an S3 storage backend
        #[cfg(feature = "opendal-s3")]
        pub fn s3(
            bucket: &str,
            region: Option<&str>,
            endpoint: Option<&str>,
            access_key_id: Option<&str>,
            secret_access_key: Option<&str>,
            prefix: impl Into<String>,
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

            let operator = Operator::new(builder)
                .map_err(|e| Error::fatal(format!("Failed to create S3 operator: {}", e)))?
                .finish();

            Ok(Self::new(operator, prefix))
        }

        /// Create a Google Cloud Storage backend
        #[cfg(feature = "opendal-gcs")]
        pub fn gcs(
            bucket: &str,
            credential: Option<&str>,
            credential_path: Option<&str>,
            prefix: impl Into<String>,
        ) -> Result<Self, Error> {
            use opendal::services::Gcs;

            let mut builder = Gcs::default().bucket(bucket);

            if let Some(cred) = credential {
                builder = builder.credential(cred);
            }
            if let Some(path) = credential_path {
                builder = builder.credential_path(path);
            }

            let operator = Operator::new(builder)
                .map_err(|e| Error::fatal(format!("Failed to create GCS operator: {}", e)))?
                .finish();

            Ok(Self::new(operator, prefix))
        }

        /// Create an Azure Blob Storage backend
        #[cfg(feature = "opendal-azblob")]
        pub fn azblob(
            container: &str,
            account_name: Option<&str>,
            account_key: Option<&str>,
            endpoint: Option<&str>,
            prefix: impl Into<String>,
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

            let operator = Operator::new(builder)
                .map_err(|e| Error::fatal(format!("Failed to create Azure Blob operator: {}", e)))?
                .finish();

            Ok(Self::new(operator, prefix))
        }

        /// Create a Backblaze B2 storage backend
        #[cfg(feature = "opendal-b2")]
        pub fn b2(
            bucket: &str,
            bucket_id: &str,
            application_key_id: &str,
            application_key: &str,
            prefix: impl Into<String>,
        ) -> Result<Self, Error> {
            use opendal::services::B2;

            let builder = B2::default()
                .bucket(bucket)
                .bucket_id(bucket_id)
                .application_key_id(application_key_id)
                .application_key(application_key);

            let operator = Operator::new(builder)
                .map_err(|e| Error::fatal(format!("Failed to create B2 operator: {}", e)))?
                .finish();

            Ok(Self::new(operator, prefix))
        }

        /// Create an SFTP storage backend
        #[cfg(feature = "opendal-sftp")]
        pub fn sftp(
            endpoint: &str,
            user: Option<&str>,
            key: Option<&str>,
            root: Option<&str>,
            prefix: impl Into<String>,
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

            let operator = Operator::new(builder)
                .map_err(|e| Error::fatal(format!("Failed to create SFTP operator: {}", e)))?
                .finish();

            Ok(Self::new(operator, prefix))
        }

        /// Create an OpenStack Swift storage backend
        #[cfg(feature = "opendal-swift")]
        pub fn swift(
            container: &str,
            endpoint: &str,
            token: Option<&str>,
            prefix: impl Into<String>,
        ) -> Result<Self, Error> {
            use opendal::services::Swift;

            let mut builder = Swift::default()
                .container(container)
                .endpoint(endpoint);

            if let Some(token) = token {
                builder = builder.token(token);
            }

            let operator = Operator::new(builder)
                .map_err(|e| Error::fatal(format!("Failed to create Swift operator: {}", e)))?
                .finish();

            Ok(Self::new(operator, prefix))
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
        async fn open_reader(&self, object: &str) -> Result<BoxedAsyncRead, Error> {
            let key = self.object_to_key(object);

            let reader = self
                .operator
                .reader(&key)
                .await
                .map_err(|e| {
                    let class = classify_opendal_error(&e);
                    Error {
                        class,
                        message: format!("Failed to open reader for {}: {}", key, e),
                    }
                })?;

            // Convert OpenDAL reader to futures::AsyncRead, then to tokio::AsyncRead
            let futures_reader = reader.into_futures_async_read(0..);
            let tokio_reader = futures_reader.await.map_err(|e| {
                Error::retry(format!("Failed to create async reader for {}: {}", key, e))
            })?;

            Ok(Box::new(tokio_reader.compat()))
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
    }
}

// Re-export OpendalStore when any OpenDAL feature is enabled
#[cfg(any(
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-azblob",
    feature = "opendal-b2",
    feature = "opendal-sftp",
    feature = "opendal-swift"
))]
pub use opendal_backend::OpendalStore;

/// Create a backend from a URL string
/// Supports file://, http://, https://, and cloud storage schemes when features are enabled.
///
/// Cloud storage URL formats (requires corresponding opendal-* feature):
/// - `s3://bucket/prefix` - AWS S3 (uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_ENDPOINT env vars)
/// - `gcs://bucket/prefix` - Google Cloud Storage (uses GOOGLE_APPLICATION_CREDENTIALS env var)
/// - `azblob://container/prefix` - Azure Blob Storage (uses AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY env vars)
/// - `b2://bucket/prefix` - Backblaze B2 (uses B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET_ID env vars)
/// - `swift://container/prefix` - OpenStack Swift (uses SWIFT_ENDPOINT, SWIFT_TOKEN env vars)
/// - `sftp://[user@]host[:port]/path` - SFTP (uses SFTP_USER, SFTP_KEY env vars)
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

            let store = OpendalStore::gcs(bucket, None, credential_path.as_deref(), prefix)?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-gcs"))]
        "gcs" | "gs" => Err(Error::fatal(
            "GCS support not compiled in. Enable the 'opendal-gcs' feature to use GCS URLs.",
        )),
        #[cfg(feature = "opendal-azblob")]
        "azblob" | "azure" => {
            let container = url.host_str().ok_or_else(|| {
                Error::fatal(format!("Azure Blob URL must have a container name: {}", url_str))
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

            let bucket_id = std::env::var("B2_BUCKET_ID").map_err(|_| {
                Error::fatal("B2_BUCKET_ID environment variable must be set")
            })?;
            let app_key_id = std::env::var("B2_APPLICATION_KEY_ID").map_err(|_| {
                Error::fatal("B2_APPLICATION_KEY_ID environment variable must be set")
            })?;
            let app_key = std::env::var("B2_APPLICATION_KEY").map_err(|_| {
                Error::fatal("B2_APPLICATION_KEY environment variable must be set")
            })?;

            tracing::debug!(
                "Creating B2 store: bucket={}, prefix={}",
                bucket,
                prefix
            );

            let store = OpendalStore::b2(bucket, &bucket_id, &app_key_id, &app_key, prefix)?;
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

            let endpoint = std::env::var("SWIFT_ENDPOINT").map_err(|_| {
                Error::fatal("SWIFT_ENDPOINT environment variable must be set")
            })?;
            let token = std::env::var("SWIFT_TOKEN").ok();

            tracing::debug!(
                "Creating Swift store: container={}, prefix={}, endpoint={}",
                container,
                prefix,
                endpoint
            );

            let store = OpendalStore::swift(container, &endpoint, token.as_deref(), prefix)?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "opendal-swift"))]
        "swift" => Err(Error::fatal(
            "Swift support not compiled in. Enable the 'opendal-swift' feature to use Swift URLs.",
        )),
        #[cfg(feature = "opendal-sftp")]
        "sftp" => {
            let host = url.host_str().ok_or_else(|| {
                Error::fatal(format!("SFTP URL must have a host: {}", url_str))
            })?;
            let port = url.port().unwrap_or(22);
            let user = if url.username().is_empty() {
                std::env::var("SFTP_USER").ok()
            } else {
                Some(url.username().to_string())
            };
            let root = url.path().to_string();
            let root = if root.is_empty() { None } else { Some(root.as_str()) };

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
