//! Custom OpenDAL layer that marks non-standard HTTP error codes as retryable.
//!
//! OpenDAL's built-in retry layer only retries standard 5xx errors (500, 502, 503, 504).
//! This layer extends retry behavior to include:
//! - Cloudflare-specific errors (520-524, 530)
//! - Informal proxy errors (509, 529, 598, 599)

use opendal::raw::{
    oio, Access, Layer, LayeredAccess, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead,
    OpRename, OpStat, OpWrite, RpCopy, RpCreateDir, RpDelete, RpList, RpPresign, RpRead, RpRename,
    RpStat, RpWrite,
};
use opendal::{Buffer, Error, Metadata, Result};
use std::fmt::{Debug, Formatter};

/// Standard HTTP server errors that OpenDAL retries natively (500, 502, 503, 504).
pub const STANDARD_RETRYABLE_HTTP_ERRORS: &[(u16, &str)] = &[
    (500, "Internal Server Error"),
    (502, "Bad Gateway"),
    (503, "Service Unavailable"),
    (504, "Gateway Timeout"),
];

/// Non-standard HTTP status codes that should be treated as retryable.
///
/// References:
/// - Cloudflare: https://developers.cloudflare.com/support/troubleshooting/cloudflare-errors/troubleshooting-cloudflare-5xx-errors/
/// - Unofficial codes: https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#Unofficial_codes
pub const NON_STANDARD_RETRYABLE_HTTP_ERRORS: &[(u16, &str)] = &[
    // Standard retryable errors that OpenDAL doesn't retry by default
    (408, "Request Timeout"),
    // TODO: Consider parsing Retry-After header for 429 responses to respect server-specified delays
    (429, "Too Many Requests"),
    // Cloudflare-specific errors
    (520, "Cloudflare Unknown Error"),
    (521, "Cloudflare Web Server Is Down"),
    (522, "Cloudflare Connection Timed Out"),
    (523, "Cloudflare Origin Is Unreachable"),
    (524, "Cloudflare A Timeout Occurred"),
    (530, "Cloudflare Origin DNS Error"),
    // Informal/proxy-specific errors
    (509, "Bandwidth Limit Exceeded"),  // Apache/cPanel
    (529, "Site is Overloaded"),        // Qualys SSLLabs
    (598, "Network Read Timeout"),      // Informal, nginx
    (599, "Network Connect Timeout"),   // Informal, nginx
];

/// Layer that marks non-standard HTTP errors as retryable.
///
/// This layer should be placed between TimeoutLayer and RetryLayer:
///
/// Service -> HttpClient -> Timeout -> RetryableErrorLayer -> Retry -> ...
#[derive(Clone, Default)]
pub struct RetryableErrorLayer;

impl RetryableErrorLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<A: Access> Layer<A> for RetryableErrorLayer {
    type LayeredAccess = RetryableErrorAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        RetryableErrorAccessor { inner }
    }
}

pub struct RetryableErrorAccessor<A: Access> {
    inner: A,
}

impl<A: Access> Debug for RetryableErrorAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryableErrorAccessor")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Check if an error contains a non-standard retryable status code and mark it as temporary.
fn make_retryable_if_needed(err: Error) -> Error {
    // Skip if already temporary (will be retried anyway)
    if err.is_temporary() {
        return err;
    }

    // Check if the error message contains any of our retryable status codes
    let err_string = err.to_string();
    for &(code, _desc) in NON_STANDARD_RETRYABLE_HTTP_ERRORS {
        // Look for "status: 522" pattern from OpenDAL's error context
        if err_string.contains(&format!("status: {}", code)) {
            tracing::trace!(
                "Marking error with status code {} as retryable: {}",
                code,
                err
            );
            return err.set_temporary();
        }
    }

    err
}

impl<A: Access> LayeredAccess for RetryableErrorAccessor<A> {
    type Inner = A;
    type Reader = RetryableErrorWrapper<A::Reader>;
    type Writer = RetryableErrorWrapper<A::Writer>;
    type Lister = RetryableErrorWrapper<A::Lister>;
    type Deleter = RetryableErrorWrapper<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner
            .create_dir(path, args)
            .await
            .map_err(make_retryable_if_needed)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, RetryableErrorWrapper { inner: r }))
            .map_err(make_retryable_if_needed)
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, RetryableErrorWrapper { inner: w }))
            .map_err(make_retryable_if_needed)
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        self.inner
            .copy(from, to, args)
            .await
            .map_err(make_retryable_if_needed)
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        self.inner
            .rename(from, to, args)
            .await
            .map_err(make_retryable_if_needed)
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner
            .stat(path, args)
            .await
            .map_err(make_retryable_if_needed)
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner
            .delete()
            .await
            .map(|(rp, d)| (rp, RetryableErrorWrapper { inner: d }))
            .map_err(make_retryable_if_needed)
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner
            .list(path, args)
            .await
            .map(|(rp, l)| (rp, RetryableErrorWrapper { inner: l }))
            .map_err(make_retryable_if_needed)
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner
            .presign(path, args)
            .await
            .map_err(make_retryable_if_needed)
    }
}

/// Wrapper for Read/Write/List/Delete operations that marks errors as retryable.
pub struct RetryableErrorWrapper<T> {
    inner: T,
}

impl<T: oio::Read> oio::Read for RetryableErrorWrapper<T> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await.map_err(make_retryable_if_needed)
    }
}

impl<T: oio::Write> oio::Write for RetryableErrorWrapper<T> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await.map_err(make_retryable_if_needed)
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await.map_err(make_retryable_if_needed)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await.map_err(make_retryable_if_needed)
    }
}

impl<T: oio::List> oio::List for RetryableErrorWrapper<T> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await.map_err(make_retryable_if_needed)
    }
}

impl<T: oio::Delete> oio::Delete for RetryableErrorWrapper<T> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner
            .delete(path, args)
            .map_err(make_retryable_if_needed)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await.map_err(make_retryable_if_needed)
    }
}
