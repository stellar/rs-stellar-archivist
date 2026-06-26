//! Binary-local wiring for the feature-gated perf instrumentation.
//!
//! All `#[cfg(feature = "perf-metrics")]` / `#[cfg(tokio_unstable)]` gating for
//! the binary lives here so `main` reads cleanly: it unconditionally calls
//! [`start`] and [`Session::report`], which compile to no-ops unless built with
//! `--features perf-metrics`.

#[cfg(feature = "perf-metrics")]
mod imp {
    use std::time::{Duration, Instant};

    /// Start-of-window baselines plus the background loggers spawned by [`start`].
    /// [`Session::report`] emits the final report, windowed to the wall interval
    /// since `start`.
    pub struct Session {
        wall_start: Instant,
        cpu_start: u64,
        #[cfg(tokio_unstable)]
        worker_busy_start: Duration,
    }

    /// Capture the window baselines and start the perf background tasks.
    pub fn start() -> Session {
        let wall_start = Instant::now();
        // Baseline CPU and worker-busy at the start of the measured window so
        // cores_avg / worker_cores_avg divide in-window totals by in-window wall
        // (both underlying counters accumulate from process/runtime start).
        let cpu_start = stellar_archivist::metrics::process_cpu_nanos();
        #[cfg(tokio_unstable)]
        let worker_busy_start = worker_busy_total();

        // The Tokio runtime-metrics logger needs `--cfg tokio_unstable`;
        // otherwise emit a one-line note pointing at the rebuild flag.
        #[cfg(tokio_unstable)]
        start_runtime_metrics_logger();
        #[cfg(not(tokio_unstable))]
        eprintln!(
            "PERF_NOTE tokio_runtime_metrics=disabled reason=missing_tokio_unstable_cfg \
             rebuild_with='RUSTFLAGS=\"--cfg tokio_unstable\" cargo run --features perf-metrics -- ...'"
        );

        // Runtime-responsiveness heartbeat. Needs no tokio_unstable — just a
        // spawned task that times its own wakeups (see metrics::record_heartbeat).
        start_heartbeat();

        Session {
            wall_start,
            cpu_start,
            #[cfg(tokio_unstable)]
            worker_busy_start,
        }
    }

    impl Session {
        /// Emit the final PERF / PERF_CPU / PERF_HEARTBEAT / PERF_PHASE report,
        /// windowing CPU and worker-busy to the wall interval since [`start`].
        pub fn report(self) {
            // In-window process CPU (user+sys, all threads): now − baseline.
            let cpu_ns =
                stellar_archivist::metrics::process_cpu_nanos().saturating_sub(self.cpu_start);
            #[cfg(tokio_unstable)]
            let worker_busy = {
                // num_workers() is fixed for the runtime's lifetime (only the
                // blocking pool is dynamically sized), so the worker set can't
                // have shifted between baseline and now.
                let nw = tokio::runtime::Handle::current().metrics().num_workers();
                let busy = worker_busy_total().saturating_sub(self.worker_busy_start);
                Some((busy, nw))
            };
            #[cfg(not(tokio_unstable))]
            let worker_busy: Option<(Duration, usize)> = None;
            stellar_archivist::metrics::report(self.wall_start.elapsed(), cpu_ns, worker_busy);
        }
    }

    /// Summed `worker_total_busy_duration(i)`: Tokio's cumulative, monotonic
    /// total of how long each worker has spent *polling tasks* (vs parked/idle)
    /// since the runtime started, flushed at park/batch boundaries (not
    /// continuously). Sampled at the call site; the reported figure is
    /// end − baseline, windowed to wall, giving worker_cores_avg — the mean
    /// number of Tokio workers concurrently busy (worker-pool occupancy).
    #[cfg(tokio_unstable)]
    fn worker_busy_total() -> Duration {
        let m = tokio::runtime::Handle::current().metrics();
        (0..m.num_workers())
            .map(|i| m.worker_total_busy_duration(i))
            .sum()
    }

    #[cfg(tokio_unstable)]
    fn start_runtime_metrics_logger() {
        let handle = tokio::runtime::Handle::current();
        tokio::spawn(async move {
            let m = handle.metrics();
            let nw = m.num_workers();
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let inj = m.global_queue_depth();
                let locq: usize = (0..nw).map(|i| m.worker_local_queue_depth(i)).sum();
                let dec = stellar_archivist::metrics::ACTIVE_DECODES
                    .load(std::sync::atomic::Ordering::Relaxed);
                // worker_mean_poll_time(i): Tokio's exponentially-weighted moving
                // average of how long one `Future::poll` on worker `i` runs
                // before yielding — per-poll duration, NOT cumulative busy time.
                // We report the max across workers: a rising value means tasks
                // run long without yielding (CPU-bound work monopolizing the
                // worker pool) — the signal such work might belong on the
                // blocking pool. (Cumulative occupancy is the PERF_CPU
                // worker_cores_avg field instead.)
                let max_poll_us = (0..nw)
                    .map(|i| m.worker_mean_poll_time(i).as_micros() as u64)
                    .max()
                    .unwrap_or(0);
                eprintln!(
                    "RTM active_decodes={} runnable_q={} (inject={} local={}) max_poll_us={}",
                    dec,
                    inj + locq,
                    inj,
                    locq,
                    max_poll_us
                );
            }
        });
    }

    fn start_heartbeat() {
        let tick = Duration::from_millis(stellar_archivist::metrics::HEARTBEAT_TICK_MS);
        tokio::spawn(async move {
            loop {
                let t0 = Instant::now();
                tokio::time::sleep(tick).await;
                stellar_archivist::metrics::record_heartbeat(t0.elapsed().saturating_sub(tick));
            }
        });
    }
}

#[cfg(feature = "perf-metrics")]
pub use imp::Session;

/// Capture the window baselines and start the perf background tasks.
#[cfg(feature = "perf-metrics")]
pub fn start() -> Session {
    imp::start()
}

// ---- No-op surface when perf-metrics is off (everything inlines away). ----

/// Zero-sized stand-in so `main` can call `perf::start()` / `Session::report()`
/// unconditionally; both compile to nothing without `--features perf-metrics`.
#[cfg(not(feature = "perf-metrics"))]
pub struct Session;

#[cfg(not(feature = "perf-metrics"))]
#[inline]
pub fn start() -> Session {
    Session
}

#[cfg(not(feature = "perf-metrics"))]
impl Session {
    #[inline]
    pub fn report(self) {}
}
