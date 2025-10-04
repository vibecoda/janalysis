# Janalysis

Tools for financial analysis of Japanese Equities. Currently contains integation with JQuants API to fetch, normalize and store stock data. Also contains, a framework to store blob storage using different backends.

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd janalysis

# Install git hooks (IMPORTANT: Run this after cloning)
./setup-hooks.sh

# Use `uv` to create a venv
uv venv -p 3.13

# Install dependencies
uv pip install -e ".[dev]"
```

### 2. Run Tests

```bash
# Run all tests
python run_tests.py

# Or use pytest directly
pytest tests/ -v
```

## Git Hooks

This repository uses git hooks to ensure code quality:

- **Pre-commit hook**: Automatically runs all tests before each commit
- **Test enforcement**: Commits are blocked if any tests fail
- **Automatic setup**: Run `./setup-hooks.sh` after cloning

### Hook Setup (Required)

After cloning the repository, **you must run the setup script** to install git hooks:

```bash
./setup-hooks.sh
```

This ensures that:
- All tests pass before commits are allowed
- Code quality is maintained across all contributions
- The build remains stable

### Manual Hook Installation

If you prefer to install hooks manually:

```bash
cp hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Development

The repository includes:
- **Three-layer Data Architecture**: Bronze (raw) → Silver (normalized) → Gold (stock-centric)
- **Blob Storage Infrastructure**: Pluggable backends (filesystem, MinIO, MongoDB)
- **Backend Registry**: Configuration-driven storage with inheritance support
- **Query Engine**: SQL interface for financial analysis (DuckDB)
- **Stock & Portfolio APIs**: High-level analysis tools
- **Comprehensive Tests**: 231 tests ensuring reliability

### Project Structure

```
jqsys/
├── core/                    # Core infrastructure
│   ├── storage/            # Storage abstraction layer
│   │   ├── blob.py         # Blob storage interface
│   │   ├── registry.py     # Backend registry
│   │   └── backends/       # Storage backend implementations
│   │       ├── filesystem_backend.py
│   │       ├── minio_backend.py
│   │       ├── mongodb_backend.py
│   │       └── prefixed_backend.py
│   └── utils/              # Shared utilities
│       ├── config.py       # Configuration with inheritance
│       └── env.py          # Environment variables
└── data/                    # Data layer
    ├── auth.py             # J-Quants authentication
    ├── client.py           # J-Quants API client
    ├── ingest.py           # Data ingestion utilities
    ├── layers/             # Storage layers
    │   ├── bronze.py       # Raw data storage
    │   ├── silver.py       # Normalized data storage
    │   └── gold.py         # Stock-centric storage
    ├── query.py            # SQL query engine
    ├── stock.py            # Stock analysis API
    └── portfolio.py        # Portfolio management API
```

### Environment Variables

```bash
# Required for J-Quants API access
export JQ_REFRESH_TOKEN=your_token_here
```

### Using MinIO Backend with Docker Compose

The project includes a MinIO backend for object storage. To use it locally:

```bash
# Start MinIO (and MongoDB) services
docker compose up -d

# Verify MinIO is running
docker compose ps

# Access MinIO console at http://localhost:9001
# Login: minioadmin / minioadmin
```

The MinIO backend is configured in `configs/blob_backends.py` with the following backends:
- `minio`: Base MinIO configuration (localhost:9000, bucket: jq-data)
- `bronze`, `silver`, `gold`: Storage layers with automatic prefix separation
  - Each layer uses the same bucket but different prefixes (bronze/, silver/, gold/)
  - Configuration uses inheritance to avoid duplication

To stop the services:

```bash
docker compose down
```

## Testing

The test suite includes comprehensive tests covering:
- Authentication and API client functionality
- Blob storage backends (filesystem, MinIO, prefixed)
- Backend registry with configuration inheritance
- Data storage layers (Bronze/Silver/Gold)
- SQL query engine with security testing
- Integration tests with realistic data
- Environment variable configuration

Run tests with detailed output:

```bash
python run_tests.py
```