//! Lightweight, feature-gated performance instrumentation.
//!
//! Compiled to no-ops unless built with `--features perf-metrics`. Phase timers
//! aggregate elapsed time across concurrent tasks via atomics. Phase times can
//! overlap, so percentages are a profiler-style breakdown rather than a
//! wall-clock critical path.

#[repr(usize)]
#[derive(Clone, Copy, Debug)]
pub enum Phase {
    HistoryFetch,
    HistoryParse,
    BucketStream,
    XdrDecompress,
    XdrParseLedger,
    XdrParseTx,
    XdrParseResult,
    XdrParseScp,
    CrossFileVerify,
    ChainVerify,
    Copy,
}

impl Phase {
    /// One past the last variant. The enum uses the default contiguous
    /// discriminants (`0..COUNT`), so this is the variant count; it sizes the
    /// per-phase `NAMES`/`PHASES` arrays. Keep `Copy` last (or update this).
    pub const COUNT: usize = Self::Copy as usize + 1;
}

/// Heartbeat probe tick. A spawned task sleeps this long each beat and records
/// how much later than this it actually woke (scheduler/timer lag); a growing
/// tail means the worker pool is too busy to promptly poll a ready task.
pub const HEARTBEAT_TICK_MS: u64 = 10;

/// Diagnostic gauge for Tokio runtime metrics: number of decode tasks (gzip+SHA
/// bucket `hash_task` and gzip XDR `decompress_task`) currently alive.
#[cfg(all(feature = "perf-metrics", tokio_unstable))]
pub static ACTIVE_DECODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// RAII guard for [`ACTIVE_DECODES`]: `enter()` at decode-task start, decrement on drop.
pub struct DecodeGuard;
impl DecodeGuard {
    #[inline]
    pub fn enter() -> Self {
        #[cfg(all(feature = "perf-metrics", tokio_unstable))]
        ACTIVE_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        DecodeGuard
    }
}
impl Drop for DecodeGuard {
    fn drop(&mut self) {
        #[cfg(all(feature = "perf-metrics", tokio_unstable))]
        ACTIVE_DECODES.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(feature = "perf-metrics")]
const NAMES: [&str; Phase::COUNT] = [
    "history_fetch",
    "history_parse",
    "bucket_stream",
    "xdr_decompress",
    "xdr_parse_ledger",
    "xdr_parse_tx",
    "xdr_parse_result",
    "xdr_parse_scp",
    "cross_file_verify",
    "chain_verify",
    "copy",
];

#[cfg(feature = "perf-metrics")]
mod imp {
    use super::{Phase, NAMES};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::time::{Duration, Instant};

    struct PhaseMetrics {
        elapsed_ns: AtomicU64, // wall time summed over concurrent calls
        calls: AtomicU64,
        bytes: AtomicU64,
    }
    impl PhaseMetrics {
        const fn new() -> Self {
            Self {
                elapsed_ns: AtomicU64::new(0),
                calls: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            }
        }

        fn record_elapsed(&self, elapsed: Duration) {
            self.elapsed_ns
                .fetch_add(elapsed.as_nanos() as u64, Relaxed);
            self.calls.fetch_add(1, Relaxed);
        }

        fn record_bytes(&self, bytes: u64) {
            self.bytes.fetch_add(bytes, Relaxed);
        }
    }

    struct FileObservations {
        count: AtomicU64,
        bytes: AtomicU64,
    }
    impl FileObservations {
        const fn new() -> Self {
            Self {
                count: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            }
        }

        fn record(&self, bytes: u64) {
            self.count.fetch_add(1, Relaxed);
            self.bytes.fetch_add(bytes, Relaxed);
        }

        fn snapshot(&self) -> (u64, u64) {
            (self.count.load(Relaxed), self.bytes.load(Relaxed))
        }
    }

    static PHASES: [PhaseMetrics; Phase::COUNT] = [const { PhaseMetrics::new() }; Phase::COUNT];
    static FILE_OBSERVATIONS: FileObservations = FileObservations::new();

    fn phase_metrics(phase: Phase) -> &'static PhaseMetrics {
        &PHASES[phase as usize]
    }

    /// Total process CPU time (user + system) in nanoseconds, across all threads,
    /// accumulated since process start. Uses POSIX `getrusage` (Linux, macOS,
    /// BSD); on non-Unix targets (e.g. Windows) it returns 0, so the feature
    /// still builds there with CPU metrics reported as 0.
    #[cfg(unix)]
    pub fn process_cpu_nanos() -> u64 {
        let mut ru = std::mem::MaybeUninit::<libc::rusage>::uninit();
        unsafe {
            if libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) != 0 {
                return 0;
            }
            let ru = ru.assume_init();
            // getrusage times are non-negative; try_from (vs `as`) makes that
            // explicit — a negative would clamp to 0 rather than wrap huge.
            let to_ns = |tv: libc::timeval| {
                u64::try_from(tv.tv_sec)
                    .unwrap_or(0)
                    .wrapping_mul(1_000_000_000)
                    .wrapping_add(u64::try_from(tv.tv_usec).unwrap_or(0).wrapping_mul(1000))
            };
            to_ns(ru.ru_utime).wrapping_add(to_ns(ru.ru_stime))
        }
    }

    /// Non-Unix fallback: `getrusage` is unavailable (e.g. Windows), report 0.
    #[cfg(not(unix))]
    pub fn process_cpu_nanos() -> u64 {
        0
    }

    /// RAII timer: records elapsed wall time plus a call count, on drop.
    pub struct Guard {
        phase: Phase,
        start: Instant,
    }
    impl Guard {
        pub fn new(phase: Phase) -> Self {
            Self {
                phase,
                start: Instant::now(),
            }
        }

        /// Record one completed file observed by this phase. The byte unit is
        /// phase-specific: copy phases count compressed bytes streamed, while
        /// verify/decode phases count decompressed bytes.
        pub fn record_file(&self, bytes: u64) {
            phase_metrics(self.phase).record_bytes(bytes);
            FILE_OBSERVATIONS.record(bytes);
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            phase_metrics(self.phase).record_elapsed(self.start.elapsed());
        }
    }

    // Heartbeat (runtime-responsiveness) accumulators. lag = how much later than
    // HEARTBEAT_TICK_MS a beat actually woke; the over_*ms tails are the signal
    // that the worker pool is too saturated to promptly poll a ready task.
    static HB_BEATS: AtomicU64 = AtomicU64::new(0);
    static HB_SUM_NS: AtomicU64 = AtomicU64::new(0);
    static HB_MAX_NS: AtomicU64 = AtomicU64::new(0);
    static HB_OVER_1MS: AtomicU64 = AtomicU64::new(0);
    static HB_OVER_10MS: AtomicU64 = AtomicU64::new(0);
    static HB_OVER_100MS: AtomicU64 = AtomicU64::new(0);

    /// Record one heartbeat's scheduling lag (actual wake delay beyond the tick).
    pub fn record_heartbeat(lag: Duration) {
        let ns = lag.as_nanos() as u64;
        HB_BEATS.fetch_add(1, Relaxed);
        HB_SUM_NS.fetch_add(ns, Relaxed);
        HB_MAX_NS.fetch_max(ns, Relaxed);
        if ns >= 1_000_000 {
            HB_OVER_1MS.fetch_add(1, Relaxed);
        }
        if ns >= 10_000_000 {
            HB_OVER_10MS.fetch_add(1, Relaxed);
        }
        if ns >= 100_000_000 {
            HB_OVER_100MS.fetch_add(1, Relaxed);
        }
    }

    /// Snapshot: (beats, sum_ns, max_ns, over_1ms, over_10ms, over_100ms).
    pub fn heartbeat_snapshot() -> (u64, u64, u64, u64, u64, u64) {
        (
            HB_BEATS.load(Relaxed),
            HB_SUM_NS.load(Relaxed),
            HB_MAX_NS.load(Relaxed),
            HB_OVER_1MS.load(Relaxed),
            HB_OVER_10MS.load(Relaxed),
            HB_OVER_100MS.load(Relaxed),
        )
    }

    /// Peak resident set size in **bytes**, normalized across platforms. Uses
    /// POSIX `getrusage`; `ru_maxrss` is bytes on macOS and kilobytes on Linux.
    /// On non-Unix targets (e.g. Windows) it returns 0.
    #[cfg(unix)]
    pub fn peak_rss_bytes() -> u64 {
        let mut ru = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let max = unsafe {
            if libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) != 0 {
                return 0;
            }
            // ru_maxrss is non-negative; try_from avoids a sign-loss `as` cast.
            u64::try_from(ru.assume_init().ru_maxrss).unwrap_or(0)
        };
        #[cfg(target_os = "macos")]
        {
            max // already bytes
        }
        #[cfg(not(target_os = "macos"))]
        {
            max.saturating_mul(1024) // Linux & other POSIX report kibibytes
        }
    }

    /// Non-Unix fallback: `getrusage` is unavailable (e.g. Windows), report 0.
    #[cfg(not(unix))]
    pub fn peak_rss_bytes() -> u64 {
        0
    }

    /// Format the ` worker_cores_avg=... workers=...` suffix appended to the
    /// `PERF_CPU` line. `worker_busy` is `Some((in-window summed
    /// worker_total_busy_duration, num_workers))` when built with
    /// `--cfg tokio_unstable`; otherwise it is `None` and the suffix is empty.
    /// `worker_cores_avg = busy / wall`: the mean number of Tokio workers
    /// concurrently inside a poll (worker-pool occupancy, not CPU cores).
    pub(crate) fn worker_cores_suffix(
        wall: Duration,
        worker_busy: Option<(Duration, usize)>,
    ) -> String {
        match worker_busy {
            Some((busy, nw)) => {
                let wca = if wall.as_secs_f64() > 0.0 {
                    busy.as_secs_f64() / wall.as_secs_f64()
                } else {
                    0.0
                };
                format!(" worker_cores_avg={wca:.2} workers={nw}")
            }
            None => String::new(),
        }
    }

    /// `cpu_ns`: process CPU time (user+sys, all threads) consumed during the
    /// measured window. `worker_busy`: `Some((in-window summed
    /// worker_total_busy_duration, num_workers))` under `--cfg tokio_unstable`,
    /// else `None` (see [`worker_cores_suffix`]).
    // u64 counters (nanos/bytes/counts) -> f64 for ms/MB/MB/s/%/cores_avg
    // display math; precision loss above 2^53 is irrelevant for these readouts.
    #[allow(clippy::cast_precision_loss)]
    pub fn report(wall: Duration, cpu_ns: u64, worker_busy: Option<(Duration, usize)>) {
        let total_ns: u64 = (0..Phase::COUNT)
            .map(|i| PHASES[i].elapsed_ns.load(Relaxed))
            .sum();
        let peak_mb = peak_rss_bytes() as f64 / 1e6;
        let (files, bytes) = FILE_OBSERVATIONS.snapshot();
        let mbps = if wall.as_secs_f64() > 0.0 {
            (bytes as f64 / 1e6) / wall.as_secs_f64()
        } else {
            0.0
        };
        let cores_avg = if wall.as_secs_f64() > 0.0 {
            cpu_ns as f64 / 1e9 / wall.as_secs_f64()
        } else {
            0.0
        };
        eprintln!(
            "PERF wall_ms={} peak_rss_mb={:.1} files_measured={} measured_bytes={} measured_mb_per_s={:.1}",
            wall.as_millis(),
            peak_mb,
            files,
            bytes,
            mbps
        );
        let worker_str = worker_cores_suffix(wall, worker_busy);
        eprintln!(
            "PERF_CPU total_cpu_ms={:.1} cores_avg={:.2}{}",
            cpu_ns as f64 / 1e6,
            cores_avg,
            worker_str
        );
        // Runtime responsiveness. Counts only — scripts derive percentages
        // (e.g. over_10ms / beats). over_10ms/over_100ms are the real signal;
        // sub-ms lag is mostly timer granularity.
        let (hb_beats, hb_sum_ns, hb_max_ns, hb_1, hb_10, hb_100) = heartbeat_snapshot();
        let hb_mean_us = if hb_beats > 0 {
            hb_sum_ns as f64 / 1000.0 / hb_beats as f64
        } else {
            0.0
        };
        eprintln!(
            "PERF_HEARTBEAT beats={} tick_ms={} mean_us={:.1} max_us={:.1} over_1ms={} over_10ms={} over_100ms={}",
            hb_beats,
            super::HEARTBEAT_TICK_MS,
            hb_mean_us,
            hb_max_ns as f64 / 1000.0,
            hb_1,
            hb_10,
            hb_100
        );
        // Columns: name, elapsed_ms, elapsed_pct, calls, phase_mb, phase_mb_per_s.
        // Elapsed time includes .await wait for async-scoped phases.
        for i in 0..Phase::COUNT {
            let ns = PHASES[i].elapsed_ns.load(Relaxed);
            let pct = if total_ns > 0 {
                ns as f64 * 100.0 / total_ns as f64
            } else {
                0.0
            };
            let mb = PHASES[i].bytes.load(Relaxed) as f64 / 1e6;
            let secs = ns as f64 / 1e9;
            let phase_mbps = if secs > 0.0 { mb / secs } else { 0.0 };
            eprintln!(
                "PERF_PHASE {},{:.1},{:.1},{},{:.1},{:.1}",
                NAMES[i],
                ns as f64 / 1e6,
                pct,
                PHASES[i].calls.load(Relaxed),
                mb,
                phase_mbps
            );
        }
    }
}

#[cfg(feature = "perf-metrics")]
pub use imp::{
    heartbeat_snapshot, peak_rss_bytes, process_cpu_nanos, record_heartbeat, report, Guard,
};

/// Zero-sized stand-in for the timing [`imp::Guard`] when the feature is off, so
/// the `phase!` macro binds a real value (no unit-binding lints) and inlines away.
#[cfg(not(feature = "perf-metrics"))]
pub struct Guard;
#[cfg(not(feature = "perf-metrics"))]
impl Guard {
    #[inline]
    pub fn new(_phase: Phase) -> Self {
        Guard
    }

    #[inline]
    pub fn record_file(&self, _bytes: u64) {}
}

#[cfg(all(test, feature = "perf-metrics"))]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::cast_precision_loss)] // bytes -> MB for the log line
    fn guard_records_and_rss_is_sane() {
        {
            let bucket_phase = Guard::new(Phase::BucketStream);
            bucket_phase.record_file(4096);
        }
        let rss = peak_rss_bytes();
        // On Unix a live process must have a positive peak RSS of a plausible
        // magnitude (1 MB–100 GB) — catches unit mistakes. On non-Unix it's the
        // 0 fallback (no getrusage).
        #[cfg(unix)]
        {
            assert!(
                rss > 1_000_000,
                "peak RSS too small ({rss} bytes) — unit bug?"
            );
            assert!(
                rss < 100_000_000_000,
                "peak RSS implausibly large ({rss} bytes) — unit bug?"
            );
            eprintln!("peak_rss_bytes() = {rss} ({:.1} MB)", rss as f64 / 1e6);
        }
        #[cfg(not(unix))]
        assert_eq!(rss, 0, "non-Unix peak_rss_bytes() is the 0 fallback");
    }

    #[test]
    fn worker_cores_suffix_formats_and_is_bounded() {
        use std::time::Duration;
        // No worker metrics (built without tokio_unstable) -> empty suffix.
        assert_eq!(
            super::imp::worker_cores_suffix(Duration::from_secs(1), None),
            ""
        );
        // busy == num_workers × wall -> worker_cores_avg == num_workers exactly:
        // the bound holds with equality and never overshoots.
        assert_eq!(
            super::imp::worker_cores_suffix(
                Duration::from_secs(2),
                Some((Duration::from_secs(20), 10))
            ),
            " worker_cores_avg=10.00 workers=10"
        );
        // Half the workers busy on average over the window.
        assert_eq!(
            super::imp::worker_cores_suffix(
                Duration::from_secs(2),
                Some((Duration::from_secs(10), 10))
            ),
            " worker_cores_avg=5.00 workers=10"
        );
        // wall == 0 -> no division by zero.
        assert_eq!(
            super::imp::worker_cores_suffix(Duration::ZERO, Some((Duration::from_secs(5), 8))),
            " worker_cores_avg=0.00 workers=8"
        );
    }

    #[test]
    fn heartbeat_records_lags_and_buckets() {
        let (beats_before, _, _, _, ten_ms_before, hundred_ms_before) = heartbeat_snapshot();
        record_heartbeat(std::time::Duration::from_micros(200)); // < 1ms
        record_heartbeat(std::time::Duration::from_millis(5)); //   >= 1ms
        record_heartbeat(std::time::Duration::from_millis(50)); //  >= 10ms
        record_heartbeat(std::time::Duration::from_millis(500)); // >= 100ms
        let (beats_after, _, max_ns, _, ten_ms_after, hundred_ms_after) = heartbeat_snapshot();
        // This is the only test that records heartbeats, so deltas are
        // attributable even under parallel execution.
        assert_eq!(beats_after - beats_before, 4);
        assert!(
            ten_ms_after - ten_ms_before >= 2,
            "the 50ms and 500ms beats exceed 10ms"
        );
        assert!(
            hundred_ms_after - hundred_ms_before >= 1,
            "the 500ms beat exceeds 100ms"
        );
        assert!(max_ns >= 500_000_000, "max captures the 500ms beat");
    }
}
