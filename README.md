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
- `--force`: Overwrite existing files when mirroring
- `--allow-mirror-gaps`: Allow creating gaps in destination archive

## License

Apache-2.0
