```
                      ███████╗████████╗███████╗██╗     ██╗      █████╗ ██████╗
                      ██╔════╝╚══██╔══╝██╔════╝██║     ██║     ██╔══██╗██╔══██╗
                      ███████╗   ██║   █████╗  ██║     ██║     ███████║██████╔╝
                      ╚════██║   ██║   ██╔══╝  ██║     ██║     ██╔══██║██╔══██╗
                      ███████║   ██║   ███████╗███████╗███████╗██║  ██║██║  ██║
                      ╚══════╝   ╚═╝   ╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝
                    █████╗ ██████╗  ██████╗██╗  ██╗██╗██╗   ██╗██╗███████╗████████╗
                   ██╔══██╗██╔══██╗██╔════╝██║  ██║██║██║   ██║██║██╔════╝╚══██╔══╝
                   ███████║██████╔╝██║     ███████║██║██║   ██║██║███████╗   ██║
                   ██╔══██║██╔══██╗██║     ██╔══██║██║╚██╗ ██╔╝██║╚════██║   ██║
                   ██║  ██║██║  ██║╚██████╗██║  ██║██║ ╚████╔╝ ██║███████║   ██║
                   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═══╝  ╚═╝╚══════╝   ╚═╝
```

# Stellar Archivist

A Rust implementation of tools for working with Stellar History Archives.

## Description

Stellar Archivist provides utilities to scan, mirror, and repair Stellar History Archives. It supports both HTTP/HTTPS and filesystem sources, allowing you to verify archive integrity, create local mirrors of remote archives, and repair a local archive against a known-good source.

## Build

Requires Rust 1.91 or newer.

```bash
# Development build
cargo build

# Optimized CLI binary
cargo build --release

# Optimized build with performance instrumentation and Tokio runtime metrics enabled
RUSTFLAGS="--cfg tokio_unstable" cargo build --release --features perf-metrics

# Run from the workspace without installing
cargo run -- scan https://history.stellar.org/prd/core-testnet/core_testnet_001
```

The release binary is written to `target/release/stellar-archivist`.

Install the published release from crates.io:

```bash
cargo install stellar-archivist
```

Install the CLI from a local checkout:

```bash
cargo install --path .
```

### Cargo Features

The default feature set builds the CLI and enables the cross-platform cloud
backends (`s3`, `gcs`, `azblob`, `b2`, and `swift`). Filesystem and HTTP(S)
support are always available.

- `cli`: Builds the `stellar-archivist` command-line binary.
- `opendal-all`: Enables all cross-platform cloud storage backends.
- `opendal-unix`: Enables Unix-only backends, currently SFTP.
- `perf-metrics`: Adds optional performance instrumentation for local profiling;
  Tokio runtime metrics require `RUSTFLAGS="--cfg tokio_unstable"`.

For a library-only build, disable default features:

```bash
cargo build --no-default-features
```

## Usage

### Scan an archive

Verify the integrity of a Stellar History Archive:

```bash
# Scan a remote archive
stellar-archivist scan https://history.stellar.org/prd/core-testnet/core_testnet_001

# Scan a local archive
stellar-archivist scan file:///path/to/archive

# Scan with a specific range
stellar-archivist scan https://history.stellar.org/prd/core-testnet/core_testnet_001 --low 1000 --high 5000
```

### Mirror an archive

Copy files from a source archive to a local filesystem:

```bash
# Mirror an entire archive
stellar-archivist mirror https://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror

# Mirror up to a specific checkpoint
stellar-archivist mirror https://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror --high 1000

# Resume mirroring from a specific ledger
stellar-archivist mirror https://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror --low 5000

# Skip optional files (scp)
stellar-archivist mirror https://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/mirror --skip-optional
```

### Repair an archive

Fixes a **local** archive in place by re-fetching broken or
missing files from a **known-good source** archive.

```bash
# Repair a local archive against a source (existence-only: restores missing files)
stellar-archivist repair https://history.stellar.org/prd/core-testnet/core_testnet_001 file:///local/archive

# Repair with full verification (detects corrupt-but-present files, not just missing ones)
stellar-archivist repair https://history.stellar.org/... file:///local/archive --verify

# Repair only a range
stellar-archivist repair https://history.stellar.org/... file:///local/archive --low 1000 --high 5000
```

**Dry-run and plans.** A dry-run writes a JSON **plan** of everything it
would repair, without touching the archive. You can review the plan and
then apply it:

```bash
# 1. Inspect: write a plan of what is broken (no writes)
stellar-archivist repair https://src/... file:///local/archive --verify --dry-run --report plan.json

# 2. Apply the plan verbatim (--verify validates the downloads, same as in a regular repair)
stellar-archivist repair https://src/... file:///local/archive --verify --plan plan.json
```

Or take the report from a previous operation:

```bash
stellar-archivist scan file:///local/archive --verify --report status.json

stellar-archivist repair https://src/... file:///local/archive --verify --plan status.json
```

#### Plan / report JSON

```jsonc
{
  // Schema version.
  "version": 1,
  // Checkpoint to restore .well-known from; null when healthy.
  "well_known": 16383,
  // Checkpoint -> broken file types: history, ledger, transactions,
  // results, scp.
  "files": { "127": ["ledger", "transactions"] },
  // Missing or SHA-256-failing bucket hashes, as 64-hex strings.
  "buckets": ["ab…ef"],
  // Cross-file or hash-chain failures, re-fetched whole.
  "checkpoints": [191],
  // Outcome counters; repair reports repeat these per stage.
  "summary": { "succeeded": 0, "skipped": 0, "failed": 3, "retries": 0 }
}
```

### Command-line options

#### Shared options

These options are accepted by `scan`, `mirror`, and `repair`.

- `-c, --concurrency N`: Number of checkpoints processed concurrently (default: 32).
- `--skip-optional`: Skip optional SCP files.
- `--low N`: Start from ledger `N`; rounded down to the previous checkpoint
  (unless `N` is already a checkpoint).
- `--high N`: Stop at ledger `N`; rounded up to the next checkpoint (unless
  `N` is already a checkpoint).
- `--verify`: Verify content, not just existence: XDR structure, hashes, chain
  continuity, and bucket SHA-256.
- `--report <file>`: Write a JSON status report. For `repair --dry-run`, this
  writes a repair plan.
- `--debug/--trace`: Set the log level to debug/trace.
- `--max-retries N`: Retry failed requests up to `N` times (default: 3).
- `--retry-min-delay-ms N`: Minimum retry delay in milliseconds (default: 100).
- `--retry-max-delay-secs N`: Maximum retry delay in seconds (default: 30).
- `--max-concurrent N`: Maximum concurrent I/O operations per storage backend
  (default: 128). Source and destination each have their own limit.
- `--timeout-secs N`: Metadata request timeout in seconds (default: 30).
- `--io-timeout-secs N`: Read/write I/O timeout in seconds (default: 300).
- `--bandwidth-limit N`: Per-backend OpenDAL bandwidth limit in bytes per
  second; `0` means unlimited (default: 0).
- `--atomic-file-writes`: Use fsync-backed temporary files and rename when
  writing to local filesystem destinations. More durable, but slower.

#### Mirror options

- `--overwrite`: Re-fetch and replace existing destination files in the mirror
  range. Without this, existing destination files are skipped (`--verify`
  does not check them).
- `--allow-mirror-gaps`: Allow `--low` to start after the destination's
  `.well-known` checkpoint, intentionally leaving a new checkpoint gap. Without
  this, mirror fails rather than extending an archive non-contiguously.

#### Repair options

- `--plan <file>`: Apply a JSON plan from a prior `repair --dry-run`, or any
  `scan`/`mirror` `--report`. Every listed file, bucket, and checkpoint is
  fetched again from the source even if it already exists on the destination;
  `--verify` still validates fetched content before writing. Mutually exclusive
  with `--low`, `--high`, and `--dry-run`.
- `--dry-run`: Report what would be repaired without writing.

#### Verification scope

`--verify` validates content in two tiers that commit at different points.
(`scan` runs the same checks read-only, so the write timing below applies only
to `mirror` and `repair`.)

- **Per-file — checked before the file is committed.** Each file is validated
  against *itself* as it streams from the source: every XDR frame must parse,
  each ledger header must match its own embedded hash, and each bucket's
  contents must match the SHA-256 in its filename. A file that is corrupt on its
  own is rejected and never committed to the destination.
- **Cross-file and cross-checkpoint — checked after the files are committed.**
  Once a checkpoint's individually-valid files are written, the archivist checks
  that they *agree*: each ledger's transaction-set and result hashes match the
  transactions / results files, and the ledger hash chain is continuous within
  and across checkpoints. Because this runs *after* the writes, a mismatch means
  files that each passed their own check were already committed but are mutually
  inconsistent. The run reports failure, and (for `mirror`) the destination is
  left holding those inconsistent files — re-run `repair --verify` against a
  known-good source to reconcile them.

### Which mode should I use?

Pick by what you're trying to do:

| Your goal | Mode |
|---|---|
| Check whether an archive is complete and valid | **`scan`** — read-only; add `--verify` to check file *contents*, not just that they exist |
| Build a local copy, or keep one up to date | **`mirror`** |
| Fix a local archive that has missing or corrupt files | **`repair`** |

One thing worth knowing: **`mirror` validates the data it downloads — not the
files already on the destination.** So to check or fix an archive you *already
have*, reach for `scan` or `repair`, not `mirror --verify`.

#### Common workflows

- **Build or sync a mirror:** `mirror <src> <dst>` — add `--verify` to validate as you copy.
- **Fix a damaged archive in one shot:** `repair --verify <src> <dst>`.
- **Audit, then fix:** `scan --verify <dst> --report findings.json`
  → `repair --verify <src> <dst> --plan findings.json`.

Any `--report` from `scan`, `mirror`, or `repair --dry-run` can be fed straight
back in as a `repair --plan` — they share one schema.

## Architecture

For a high-level tour of how the tool is structured — see [`docs/architecture.md`](docs/architecture.md).

## License

Apache-2.0
