# Stellar Archivist

> **This software is under active development and has not undergone a security
> audit. It is provided "as is" with no warranties. Use at your own risk. This
> project is not covered by the
> [Stellar Bug Bounty Program](https://stellar.org/bug-bounty-program).**

A Rust implementation of tools for working with Stellar History Archives.

## Description

Stellar Archivist provides utilities to scan, mirror, and repair Stellar History Archives. It supports both HTTP/HTTPS and filesystem sources, allowing you to verify archive integrity, create local mirrors of remote archives, and repair a local archive against a known-good source.

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

### Which mode should I use?

Three subcommands, three jobs:

- **`scan`** — *audit* an archive. Read-only: checks every expected file is
  present and, with `--verify`, internally valid (XDR structure;
  ledger/transaction/result hashes; hash-chain continuity; bucket SHA-256).
  Never writes.
- **`mirror`** — *build or sync* a local copy of an archive. Copies
  source → destination, incrementally.
- **`repair`** — *fix* a damaged local archive against a known-good source.
  Re-fetches only the broken files.

#### `mirror`: existence, `--overwrite`, and `--verify`

These interact, and the order matters:

1. **Existence + `--overwrite`** decide *whether mirror touches a file at all*.
   A file already on the destination is **skipped** unless `--overwrite` is set;
   a missing file is always fetched.
2. **`--verify`** decides *how* mirror copies a file it does fetch: it validates
   the **source** content as it streams it to the destination.

| Destination has the file? | `--overwrite` | mirror does | `--verify` validates |
|---|---|---|---|
| yes | no (default) | **skips it** | nothing — the file is left untouched |
| yes | yes | re-fetches and overwrites | the source content |
| no | either | fetches | the source content |

**`mirror --verify` validates the source bytes it copies, never the files
already on the destination.** Under the default (`--overwrite` off), a
corrupt-but-present file on the destination is skipped and never checked — so
`mirror --verify` is *not* a way to audit a destination's health.

**`mirror --verify --overwrite` is a brute-force *resync*, not an audit or a
dry-run.** It re-fetches the whole range and overwrites the destination
(validating the *source* as it copies), so its report reflects source/write
failures, never destination health — a corrupt-but-present file on the
destination is silently overwritten, not reported. To *inspect* a destination,
use `scan`/`repair` (below); to *fix* one surgically, use `repair`.

#### Auditing a destination's health: use `scan` or `repair`, not `mirror`

To answer "is my local archive complete and valid?":

- `scan --verify <local>` — read-only audit; reports missing and corrupt files.
  (Requires the archive's `.well-known` to be intact, since it derives the
  checkpoint range from it.)
- `repair --verify --dry-run <src> <local> --report plan.json` — the same audit,
  but it also tolerates a **damaged `.well-known`** (it falls back to the source
  for the range and flags `.well-known` for restoration), and the report doubles
  as a repair plan.

Use these — not `mirror --verify` — because mirror skips, and so never checks,
files already present on the destination.

#### Recommended workflows

- **Build or sync a mirror:** `mirror <src> <dst>` — add `--overwrite` to
  force-refresh existing files, `--verify` to validate as you copy.
- **Fix a damaged archive (one shot):** `repair --verify <src> <dst>`.
- **Inspect, then fix:** `repair --verify --dry-run <src> <dst> --report plan.json`
  → review `plan.json` → `repair --verify <src> <dst> --plan plan.json`.
- **Audit with scan, fix with repair:**
  `scan --verify <dst> --report findings.json` → `repair --verify <src> <dst> --plan findings.json`.
- **Retry a failed mirror:** `mirror <src> <dst> --report mirror.json`
  → `repair --plan mirror.json` (optionally against a better source).

A `--report` from any of `scan`, `mirror`, or `repair --dry-run` is a valid
`repair --plan` input — they share one schema. `--verify` works exactly as in a
regular repair (it validates the downloaded content and content-checks files
discovered during the repair).

### Repair an archive

Repair fixes a **local** archive in place by re-fetching broken or
missing files from a **known-good source** archive. It tries the local
copy first and only fetches from the source what is actually broken
(when applying a plan, listed items are re-fetched directly — see
"Dry-run and plans" below).

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

`--plan` cannot be combined with `--low`, `--high`, or `--dry-run` — the
plan already defines the scope.

Applying a plan re-fetches every listed item from the source
unconditionally**: the plan is trusted, and the destination is not
re-audited (no per-item existence check or content re-verification). A
stale plan therefore re-downloads items that have since been fixed — and
fails if the source no longer has them. Items not listed in the plan are
left untouched, except that buckets referenced by a listed history file
are checked on the destination and fetched only if missing or invalid. To
re-audit the destination instead of trusting an old plan, generate a
fresh one with `--dry-run`.

**Status reports.** `--report <file>` writes a JSON status file for any
operation (scan, mirror, or repair). Any such report is also a valid
`repair --plan` input — see "Which mode should I use?" above.

```bash
stellar-archivist scan https://history.stellar.org/... --verify --report status.json
```

#### Plan / report JSON

```jsonc
{
  "version": 1,
  "well_known": 16383,                              // see below; null when healthy
  "files": { "127": ["ledger", "transactions"] },   // checkpoint -> broken file types
  "buckets": ["ab…ef"],                             // bucket content hashes (64 hex)
  "checkpoints": [191],                              // cross-file / chain failures
  "summary": { "succeeded": 0, "skipped": 0, "failed": 3, "retries": 0 }
}
```

- **`version`** — schema version (currently `1`).
- **`well_known`** — the checkpoint number that
  `.well-known/stellar-history.json` should be restored from when that
  root file is missing or corrupt; `null` when it is healthy.
- **`files`** — a map from **checkpoint number** (as a string) to that
  checkpoint's broken per-checkpoint file types (`history`, `ledger`,
  `transactions`, `results`, `scp`). For example `"127": ["ledger"]`
  means the ledger file at checkpoint 127 is missing or corrupt.
- **`buckets`** — content hashes (64-hex) of bucket files that are
  missing or fail their SHA-256 check.
- **`checkpoints`** — checkpoints whose files each parse individually but
  fail *cross-file* checks (a ledger's transaction-set or result hash not
  matching the transactions/results file) or *hash-chain* checks (a break
  in the ledger-to-ledger chain). Unlike `files` (individually broken
  files), these are re-fetched whole. A checkpoint here means "the pieces
  look individually valid but don't agree with each other."
- **`summary`** — outcome counters (`succeeded` / `skipped` / `failed` =
  file + bucket failures / `retries`). A non-dry-run repair instead emits
  a multi-section report keyed by stage (`main_pass`, `file_retry`,
  `checkpoint_retry`), each section having this same shape.

#### Verification scope

With `--verify`, content is checked in two tiers, which commit at different
times:

- **Per-file, before writing.** Each file's own content — XDR structure, its
  self-hash, bucket SHA-256 — is validated as it streams from the source, so a
  file that is corrupt *in itself* is rejected and never written.
- **Cross-file and cross-checkpoint, after writing.** Whether individually-valid
  files *agree* — a ledger's transaction-set / result hash matching the
  transactions / results file, and hash-chain continuity across ledgers and
  checkpoints — is checked only *after* those files have been written. So a
  cross-file/chain failure means files that each passed their own check were
  committed but are mutually inconsistent. The run reports failure, and (for
  `mirror`) the destination is left holding those inconsistent files — re-run
  `repair --verify` against a known-good source to reconcile them.

Two further limitations:

1. Without `--verify`, scan/mirror/repair only check that files *exist* — they
   cannot detect a file that is present but corrupt.
2. Even with `--verify`, repair trusts a source that passes verification as
   canonical; it validates internal integrity but does not independently
   re-verify its own writes after the run.

### Command-line options

- `-c, --concurrency N`: Number of concurrent workers (default: 32)
- `--skip-optional`: Skip optional files (scp)
- `--low N`: Start from ledger N
- `--high N`: Stop at checkpoint N
- `--verify`: Verify content (XDR structure, hashes, chain continuity, bucket SHA-256), not just existence
- `--report <file>`: Write a JSON status report (or, for `repair --dry-run`, a plan)
- `--overwrite`: Overwrite existing files when mirroring
- `--allow-mirror-gaps`: Allow creating gaps in destination archive
- `--plan <file>` (repair only): Apply a JSON plan from a prior `--dry-run`, or any `scan`/`mirror` `--report`. Every listed item is re-fetched unconditionally; `--verify` works as in a regular repair (validates downloaded content). Mutually exclusive with `--low`/`--high`/`--dry-run`
- `--dry-run` (repair only): Report what would be repaired without writing

## Architecture

The codebase is organized into three main layers:

### Pipeline

The pipeline (`pipeline.rs`) coordinates concurrent processing of archive checkpoints. It:

- Accepts a checkpoint range (derived from CLI args or archive metadata)
- Iterates through checkpoints and processes them concurrently using a configurable worker pool
- For each checkpoint, downloads and parses the history JSON file to discover what additional files to process
- Dedups Bucket files
- Schedules processing work for each file, the implementation of which is handled by a given op implementation

### Operations

Operations implement the `Operation` trait (`pipeline.rs:48-102`) and define what the pipeline does with each file. The pipeline calls into the operation for:

- `get_checkpoint_bounds()`: Determine the range of checkpoints to process
- `pre_check()`: Skip files without hitting the source (e.g., something exists in the destination already during a mirror op)
- `process_object()`: Handle file content (copy, verify, etc.)
- `record_success/failure/retry/skipped()`: Track statistics
- `finalize()`: Complete the operation and report results

Three operations are implemented:
- **ScanOperation** (`scan_operation.rs`): Validates that files exist in the archive (optionally verifying content)
- **MirrorOperation** (`mirror_operation.rs`): Copies files from source to destination
- **RepairOperation** (`repair_operation.rs`): Fixes a local archive in place by re-fetching broken/missing files from a known-good source

### Storage

The storage layer (`storage.rs`) provides a unified `Storage` trait for I/O operations:

- `open_reader()`: Stream file content
- `exists()`: Check if a file exists
- `open_writer()`: Write file content (filesystem only)

Two backends implement this trait:
- **FileStore**: Local filesystem access
- **HttpStore**: HTTP/HTTPS access with connection pooling and HTTP/2 support

## License

Apache-2.0
