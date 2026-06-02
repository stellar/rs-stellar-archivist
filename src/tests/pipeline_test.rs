//! Unit tests for `Pipeline::run_checkpoints` and `Pipeline::stats()`.
//!
//! Uses a minimal `TestOperation` implementing the `Operation` trait. The test
//! operation does no real I/O: `fetch_history_buffer` returns a fatal error
//! (so the pipeline records HISTORY failure and falls through to per-cp file
//! processing), `process_object` records each invocation, and
//! `finalize_checkpoint` / `finalize` record their respective calls.
//!
//! Per-cp file count under `skip_optional=true`: 3 files (ledger,
//! transactions, results) → 3 `process_object` calls per cp.

use crate::pipeline::{
    async_trait, HistoryFetch, Operation, Pipeline, PipelineConfig, ProcessOutcome,
};
use crate::storage::{Error as StorageError, StorageConfig, StorageRef};
use crate::utils::ArchiveStats;
use opendal::{Buffer, Reader};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

//=============================================================================
// Stub storage — fills the StorageRef slot; never actually used by TestOperation
//=============================================================================

struct StubStorage;

#[async_trait::async_trait]
impl crate::storage::Storage for StubStorage {
    async fn open_reader(&self, _object: &str) -> Result<Reader, StorageError> {
        Err(StorageError::not_found())
    }
    async fn exists(&self, _object: &str) -> Result<bool, StorageError> {
        Ok(false)
    }
}

fn stub_storage() -> StorageRef {
    Arc::new(StubStorage)
}

//=============================================================================
// TestOperation
//=============================================================================

#[derive(Default)]
struct TestOperationState {
    /// `(checkpoint_context, path)` for each `process_object` call.
    process_object_calls: Vec<(u32, String)>,
    /// `Some(highest_checkpoint)` if `finalize()` was invoked.
    finalize_highest_checkpoint: Option<u32>,
    /// Live set of cps currently in flight (added on first `process_object`
    /// for that cp, removed when its last `process_object` returns).
    in_flight_cps: BTreeSet<u32>,
    /// Max size of `in_flight_cps` observed during the run.
    peak_in_flight_cps: usize,
}

struct TestOperation {
    state: Arc<Mutex<TestOperationState>>,
    bounds: (u32, u32),
    /// Optional delay inside `process_object` to keep cps in flight long
    /// enough for the peak-concurrency assertion to observe overlap.
    delay_per_object: Option<Duration>,
}

impl TestOperation {
    fn new(bounds: (u32, u32)) -> (Self, Arc<Mutex<TestOperationState>>) {
        let state = Arc::new(Mutex::new(TestOperationState::default()));
        (
            Self {
                state: state.clone(),
                bounds,
                delay_per_object: None,
            },
            state,
        )
    }

    fn with_delay(mut self, d: Duration) -> Self {
        self.delay_per_object = Some(d);
        self
    }
}

#[async_trait]
impl Operation for TestOperation {
    async fn get_checkpoint_bounds(
        &self,
        _source: &StorageRef,
    ) -> Result<(u32, u32), crate::pipeline::Error> {
        Ok(self.bounds)
    }

    async fn process_object(
        &self,
        path: &str,
        _src_store: &StorageRef,
        _manager: Option<&crate::xdr_verify::XdrVerificationManager>,
    ) -> Result<ProcessOutcome, StorageError> {
        // `skip_optional=true` in `make_config` => 3 per-cp files. First call
        // for a cp inserts into in_flight; third call removes. Tracks peak
        // cp-level concurrency.
        const FILES_PER_CP: usize = 3;
        let checkpoint = crate::history_format::checkpoint_from_path(path)
            .expect("test paths always carry a checkpoint");
        {
            let mut s = self.state.lock().unwrap();
            s.process_object_calls.push((checkpoint, path.to_string()));
            let per_cp_count = s
                .process_object_calls
                .iter()
                .filter(|(c, _)| *c == checkpoint)
                .count();
            if per_cp_count == 1 {
                s.in_flight_cps.insert(checkpoint);
                let n = s.in_flight_cps.len();
                if n > s.peak_in_flight_cps {
                    s.peak_in_flight_cps = n;
                }
            }
        }
        if let Some(d) = self.delay_per_object {
            tokio::time::sleep(d).await;
        }
        {
            let mut s = self.state.lock().unwrap();
            let per_cp_count = s
                .process_object_calls
                .iter()
                .filter(|(c, _)| *c == checkpoint)
                .count();
            if per_cp_count >= FILES_PER_CP {
                s.in_flight_cps.remove(&checkpoint);
            }
        }
        Ok(ProcessOutcome::Processed)
    }

    async fn finalize(
        &self,
        highest_checkpoint: u32,
        _stats: &ArchiveStats,
        _report_path: Option<&std::path::Path>,
    ) -> Result<(), crate::pipeline::Error> {
        self.state.lock().unwrap().finalize_highest_checkpoint = Some(highest_checkpoint);
        Ok(())
    }

    async fn fetch_history_buffer(
        &self,
        _history_path: &str,
        _src_store: &StorageRef,
        _dst_store: Option<&StorageRef>,
    ) -> Result<HistoryFetch, StorageError> {
        // Fatal so `with_retries` returns immediately; pipeline records HISTORY
        // failure and falls through to per-cp file processing.
        Err(StorageError::fatal("test stub: no history"))
    }

    async fn process_buffer(
        &self,
        _path: &str,
        _buffer: Buffer,
    ) -> Result<ProcessOutcome, StorageError> {
        // Not invoked because `fetch_history_buffer` always fails.
        Ok(ProcessOutcome::Processed)
    }
}

fn make_config(concurrency: usize) -> PipelineConfig {
    let storage_config = StorageConfig::new(
        0, // max_retries = 0 (no retries → fast tests)
        Duration::from_millis(1),
        Duration::from_secs(1),
        16,
        Duration::from_secs(1),
        Duration::from_secs(1),
        0,
        false,
    );
    PipelineConfig {
        concurrency,
        skip_optional: true, // 3 per-cp files (no SCP)
        skip_history_and_buckets: false,
        verify: false,
        storage_config,
    }
}

//=============================================================================
// Tests
//=============================================================================

#[tokio::test]
async fn test_run_checkpoints_empty_iter_is_noop() {
    let (op, state) = TestOperation::new((63, 63));
    let pipeline = Pipeline::new(op, make_config(1), stub_storage(), None, None);

    pipeline
        .run_checkpoints(Vec::<u32>::new())
        .await
        .expect("empty run should succeed");

    let s = state.lock().unwrap();
    assert!(
        s.process_object_calls.is_empty(),
        "no per-cp work should run for empty iter"
    );
    assert!(
        s.finalize_highest_checkpoint.is_none(),
        "empty input should skip finalize"
    );
}

#[tokio::test]
async fn test_run_checkpoints_single_cp() {
    let (op, state) = TestOperation::new((63, 63));
    let pipeline = Pipeline::new(op, make_config(1), stub_storage(), None, None);

    pipeline.run_checkpoints(vec![63]).await.unwrap();

    let s = state.lock().unwrap();
    // skip_optional=true → 3 per-cp files (ledger, transactions, results).
    assert_eq!(
        s.process_object_calls.len(),
        3,
        "one cp should produce 3 process_object calls"
    );
    assert!(
        s.process_object_calls.iter().all(|(cp, _)| *cp == 63),
        "all calls should be for cp=63"
    );
    assert!(
        s.finalize_highest_checkpoint.is_none(),
        "run_checkpoints must NOT call operation.finalize"
    );
}

#[tokio::test]
async fn test_run_checkpoints_disjoint_cps() {
    let (op, state) = TestOperation::new((0, 0)); // bounds unused by this test
    let pipeline = Pipeline::new(op, make_config(2), stub_storage(), None, None);

    // Disjoint, non-consecutive cps.
    pipeline.run_checkpoints(vec![63, 575, 4095]).await.unwrap();

    let s = state.lock().unwrap();
    assert_eq!(s.process_object_calls.len(), 9, "3 cps × 3 files = 9 calls");

    let process_cps: BTreeSet<u32> = s.process_object_calls.iter().map(|(cp, _)| *cp).collect();
    let expected: BTreeSet<u32> = [63, 575, 4095].into_iter().collect();
    assert_eq!(process_cps, expected);

    assert!(
        s.finalize_highest_checkpoint.is_none(),
        "run_checkpoints must NOT call operation.finalize"
    );
}

#[tokio::test]
async fn test_run_checkpoints_does_not_call_finalize() {
    let (op, state) = TestOperation::new((0, 0));
    let pipeline = Pipeline::new(op, make_config(1), stub_storage(), None, None);

    pipeline.run_checkpoints(vec![63, 127]).await.unwrap();

    let s = state.lock().unwrap();
    assert!(
        s.finalize_highest_checkpoint.is_none(),
        "run_checkpoints must not invoke operation.finalize; that's pipeline.finish's job"
    );
}

#[tokio::test]
async fn test_pipeline_finish_calls_finalize_with_supplied_highest() {
    let (op, state) = TestOperation::new((0, 0));
    let pipeline = Pipeline::new(op, make_config(1), stub_storage(), None, None);

    pipeline.finish(8191).await.expect("finish should succeed");

    let s = state.lock().unwrap();
    assert_eq!(
        s.finalize_highest_checkpoint,
        Some(8191),
        "pipeline.finish must call operation.finalize with the supplied highest cp"
    );
}

#[tokio::test]
async fn test_run_checkpoints_respects_concurrency() {
    let (op, state) = TestOperation::new((0, 0));
    let op = op.with_delay(Duration::from_millis(50));
    let pipeline = Pipeline::new(op, make_config(2), stub_storage(), None, None);

    // 4 cps with concurrency=2 → peak in-flight should be exactly 2.
    pipeline
        .run_checkpoints(vec![63, 127, 191, 255])
        .await
        .unwrap();

    let s = state.lock().unwrap();
    assert_eq!(
        s.peak_in_flight_cps, 2,
        "peak in-flight cps should equal configured concurrency"
    );
}

#[tokio::test]
async fn test_stats_accessor_returns_live_ref() {
    let (op, _state) = TestOperation::new((0, 0));
    let pipeline = Pipeline::new(op, make_config(1), stub_storage(), None, None);

    // Mutations through the accessor must be visible on subsequent reads.
    pipeline.stats().record_success("foo");
    pipeline.stats().record_success("bar");

    assert_eq!(
        pipeline.stats().successful_files.load(Ordering::Relaxed),
        2,
        "stats() should return a live reference"
    );
}
