# Stellar Archivist

A Rust implementation of tools for working with Stellar History Archives.

## Description

Stellar Archivist provides utilities to scan and mirror Stellar History Archives. It supports both HTTP/HTTPS and filesystem sources, allowing you to verify archive integrity and create local mirrors of remote archives.

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

### Command-line options

- `-c, --concurrency N`: Number of concurrent workers (default: 32)
- `--skip-optional`: Skip optional files (scp)
- `--low N`: Start from ledger N
- `--high N`: Stop at checkpoint N
- `--overwrite`: Overwrite existing files when mirroring
- `--allow-mirror-gaps`: Allow creating gaps in destination archive

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

Two operations are implemented:
- **ScanOperation** (`scan_operation.rs`): Validates that files exist in the archive
- **MirrorOperation** (`mirror_operation.rs`): Copies files from source to destination

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
