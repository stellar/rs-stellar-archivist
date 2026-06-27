# Architecture

This document is a high-level guide to how `stellar-archivist` is put together.
It is meant to orient contributors and users, not to duplicate every detail from
the source.

## What It Does

`stellar-archivist` works with [Stellar History Archives](https://developers.stellar.org/docs/run-core-node/publishing-history-archives):
published checkpoint trees that let operators replay or audit Stellar ledger
history. The CLI has three modes, all built on the same pipeline:

- **`scan`**: read-only audit of an archive. It checks expected files exist and,
  with `--verify`, validates their contents.
- **`mirror`**: copy from a source archive to a writable destination, with
  resume support.
- **`repair`**: fix a local destination archive by re-fetching missing or corrupt
  files from a known-good source.

## Archive Format

The code follows the shape of the Stellar history archive format, modeled in
`history_format.rs`.

- The ledger is divided into checkpoints of 64 ledgers. Checkpoints are ledgers
  63, 127, 191, and so on.
- Each checkpoint has one history JSON file plus `ledger`, `transactions`, and
  `results` XDR files. `scp` is optional.
- History files describe an 11-level bucket list. Buckets are content-addressed
  by SHA-256 hash and can be shared by many checkpoints.
- `.well-known/stellar-history.json` is the root pointer to the latest archive
  checkpoint.
- Version 1 history files contain `current_buckets`; version 2 also contains
  `hot_archive_buckets`.

Paths are deterministic: checkpoint files are derived from checkpoint ledger
numbers, and bucket paths are derived from bucket hashes.

## System Shape

The main division of responsibility is:

```text
CLI -> Pipeline -> Operation -> Storage -> Backend
```

- **CLI** (`cli/`) parses arguments, configures logging and storage settings,
  chooses a mode, and constructs the matching operation.
- **Pipeline** (`pipeline.rs`) owns concurrency, retries, bucket deduplication,
  verification lifecycle, and shared stats.
- **Operations** (`scan_operation.rs`, `mirror_operation.rs`,
  `repair_operation.rs`) decide what a file means for a mode: inspect, copy,
  skip, repair, or report.
- **Storage** (`storage.rs`) provides one I/O interface over filesystem, HTTP,
  and feature-gated cloud backends.

In short: the pipeline owns orchestration, operations own policy, and storage
owns I/O.

## Pipeline

`Pipeline<Op: Operation>` is the shared engine. It asks the operation for a
checkpoint range, then processes checkpoints concurrently up to `--concurrency`.
For each checkpoint, it processes the history file, required checkpoint files,
optional SCP file, and any buckets discovered from the parsed history file.

The pipeline handles several cross-cutting concerns centrally:

- **Retries**: file work is wrapped in retry logic for transient errors. Storage
  does not use an OpenDAL retry layer; retrying the whole file operation is safer
  than retrying in the middle of a stream.
- **Bucket deduplication**: buckets are content-addressed and repeated across
  checkpoints, so the pipeline keeps an LRU of bucket hashes already seen during
  the run.
- **Verification lifecycle**: with `--verify`, the pipeline coordinates the XDR
  verification manager so per-file checks and cross-file checks happen at the
  right time.
- **Stats**: operations report non-error outcomes as processed, skipped, or
  needs repair; exhausted errors are recorded as failures. The pipeline
  accumulates both into reports.

## Operations

All modes implement the `Operation` trait in `pipeline.rs`. The trait gives the
pipeline enough hooks to ask for checkpoint bounds, process files, process
history files, and finalize the run.

### Scan

`ScanOperation` is source-only and read-only. It derives its range from the
source `.well-known`, bounded by `--low` and `--high`. Without `--verify`, scan
checks existence. With `--verify`, it streams and validates contents.

### Mirror

`MirrorOperation` copies source to destination. It uses the source and
destination `.well-known` files to decide whether to start from genesis, resume,
or skip work because the destination is already current.

Existing destination files are skipped unless `--overwrite` is set. That matters
for verification: `mirror --verify` validates files it downloads from the
source, but it does not audit destination files it skips. To inspect an existing
destination, use `scan --verify` or `repair --verify --dry-run`.

Mirror has a narrow gap check. If a destination already advertises checkpoint
`A` in `.well-known` and `--low` starts after `A`, mirror rejects the run unless
`--allow-mirror-gaps` is set. Mirror does not scan for older missing files below
`.well-known`; middle gaps are audit/repair work.

At the end of a successful run, mirror updates destination `.well-known` from
the highest successfully mirrored history file when appropriate.

### Repair

`RepairOperation` fixes a local destination against a known-good source. It is a
small multi-phase workflow:

1. Discover missing or corrupt destination files. In `--dry-run`, stop here and
   write a plan/report.
2. Re-fetch individual failed files and buckets from the source.
3. Re-fetch whole checkpoints for cross-file or hash-chain failures.
4. Restore `.well-known` when it is missing or corrupt.

Repair can also run from a plan with `--plan`. A plan may come from
`repair --dry-run` or from a `scan`/`mirror` report. In plan mode, each listed
file, bucket, or checkpoint is fetched from the source even if the destination
already has something at that path; `--verify` still validates fetched content
before writing.

## Verification

`--verify` checks content, not just presence. The implementation is split across
`xdr_verify.rs` for XDR files and `verify.rs` for bucket SHA-256 checks.

There are two tiers:

- **Per-file checks before writing**: each downloaded file must be valid by
  itself. Ledger headers, transaction/result files, and buckets are rejected
  before they are committed if they fail their own structural or hash checks.
- **Cross-file and chain checks after per-file success**: once the files for a
  checkpoint are individually valid, the verifier checks that they agree with
  each other and that ledger hash links are continuous across checkpoints.

This split is important. A file that is corrupt in itself should never be
written. A cross-file failure means the pieces looked valid alone but did not
agree as a set, so repair may re-fetch the whole checkpoint.

## Storage

`storage.rs` exposes a `Storage` trait with reader, existence, writer, and copy
operations. Backends are selected from URL schemes by `from_url_with_config`.

| Scheme | Backend | Writable |
|---|---|---|
| `file://` | local filesystem | yes |
| `http://`, `https://` | HTTP/HTTPS | no |
| `s3://`, `gcs://`/`gs://`, `azblob://`/`azure://`, `b2://`, `swift://`, `sftp://` | feature-gated cloud backends | no |

Each backend is built as an OpenDAL operator with common layers: timeout,
concurrency limit, logging, an HTTP client where needed, and optional bandwidth
throttling.

Filesystem writes are the only supported destination writes. With
`--atomic-file-writes`, OpenDAL handles atomic writes with fsync. Without it,
the filesystem destination write path uses direct `tokio::fs` writes to a temp
file followed by rename, which avoids OpenDAL writer overhead but bypasses
OpenDAL layers on the destination write side.

## Concurrency

Two limits matter:

- `--concurrency` controls how many checkpoints the pipeline processes at once.
- `--max-concurrent` controls concurrent I/O operations per storage backend.
  `scan` has one backend. `mirror` and `repair` construct separate source and
  destination backends, so each side gets its own I/O limit.

Within a checkpoint, independent file work runs in parallel. Shared state is
kept small: an LRU for bucket deduplication, counters for stats, and failure
tracking for reports.

## Reports and Plans

Reports are built from `ArchiveStats` and `FailureTracker` in `utils.rs`, then
serialized by `report.rs`. The same JSON schema is used for status reports and
repair plans. That gives the main workflow:

```text
scan --verify --report findings.json
repair --plan findings.json
```

or:

```text
repair --verify --dry-run --report plan.json
repair --verify --plan plan.json
```

## Module Map

| Module | Responsibility |
|---|---|
| `cli/` | argument parsing, logging setup, config wiring |
| `pipeline.rs` | concurrent orchestration, retries, bucket dedup, verification lifecycle |
| `scan_operation.rs` | scan mode |
| `mirror_operation.rs` | mirror mode, resume, overwrite, mirror gap checks |
| `repair_operation.rs` | repair mode, dry-run, plan mode, retry phases |
| `storage.rs` | storage trait, OpenDAL backends, I/O layers, error classes |
| `history_format.rs` | history JSON model, checkpoint math, path construction |
| `xdr_verify.rs` / `verify.rs` | XDR and bucket content verification |
| `report.rs` | shared report/plan JSON schema |
| `utils.rs` | retry/backoff, stats, failure tracking, bounds math |
| `metrics.rs` | optional performance instrumentation |
