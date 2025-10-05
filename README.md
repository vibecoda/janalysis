# Janalysis

Tools for financial analysis of Japanese Equities. Currently contains integation with JQuants API to fetch, normalize and store stock data. Also contains, a framework to store blob storage using different backends.

## Prerequisites

Before getting started, ensure you have the following installed:

### 1. uv (Python Package Manager)

[uv](https://docs.astral.sh/uv/) is a fast Python package installer and resolver.

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with Homebrew (macOS)
brew install uv

# Or with pip
pip install uv
```

### 2. J-Quants API Token

You'll need a refresh token from J-Quants to access their API:

1. Sign up for a J-Quants account at [https://jpx-jquants.com/](https://jpx-jquants.com/)
2. Navigate to your account settings or API section
3. Generate a **refresh token** (also called API token)
4. Add the token to your environment:

   **Option A: Create a `.env` file** (recommended for local development):
   ```bash
   # In the project root directory
   echo "JQ_REFRESH_TOKEN=your_token_here" > .env
   ```

   **Option B: Export as environment variable**:
   ```bash
   export JQ_REFRESH_TOKEN=your_token_here
   ```

   **Option C: Add to your shell profile** (for persistent configuration):
   ```bash
   # Add to ~/.bashrc, ~/.zshrc, or equivalent
   echo 'export JQ_REFRESH_TOKEN=your_token_here' >> ~/.zshrc
   source ~/.zshrc
   ```

### 3. Docker with Compose Support (Optional)

Docker is **only required** if you want to use MinIO object storage. The project works perfectly fine with local filesystem storage without Docker.

If you want to use MinIO for object storage:

**macOS**: We recommend [OrbStack](https://orbstack.dev/) as a fast, lightweight alternative to Docker Desktop:
```bash
# Install with Homebrew
brew install orbstack
```

**Linux**: Install Docker Engine and Docker Compose:
```bash
# See https://docs.docker.com/engine/install/
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
```

**Windows**: Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)

**Note**: If you skip Docker, the project will use filesystem-based storage at `./var/blob_storage/` by default. See [Storage Backend Configuration](#storage-backend-configuration) for details.


**Important**: The `.env` file is gitignored and safe for local tokens. Never commit tokens to version control.

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

### 2. Run Data Pipeline

```bash
# Run all tests
python scripts/batch/run_data_pipeline.py
```
### 3. Run the demos in notebooks/stock_demo.ipynb

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
├── data/                    # Data layer
│   ├── auth.py             # J-Quants authentication
│   ├── client.py           # J-Quants API client
│   ├── ingest.py           # Data ingestion utilities
│   ├── layers/             # Storage layers
│   │   ├── bronze.py       # Raw data storage
│   │   ├── silver.py       # Normalized data storage
│   │   └── gold.py         # Stock-centric storage
│   ├── query.py            # SQL query engine
│   ├── stock.py            # Stock analysis API
│   └── portfolio.py        # Portfolio management API
├── fin/                     # Financial domain APIs
│   └── stock.py            # Stock API with price adjustments
├── notebooks/               # Jupyter notebooks
│   └── stock_demo.ipynb    # Stock API demo with plotting
└── scripts/                 # Utility scripts
    ├── batch/              # Batch data ingestion
    └── demo/               # Demo scripts
```

### Jupyter Notebooks

The `notebooks/` directory contains interactive examples demonstrating the Stock API:

- **`stock_demo.ipynb`**: Comprehensive demo showing:
  - Stock search and instantiation
  - Price history retrieval with date filtering
  - Visualization of close prices, volume, and OHLC data
  - Adjustment factor handling for corporate actions
  - Using convenience methods (close_series, volume_series, etc.)

To run the notebooks:
```bash
# Install Jupyter if not already installed (included in dev dependencies)
uv pip install -e ".[dev]"

# Start Jupyter
jupyter notebook

# Navigate to notebooks/stock_demo.ipynb
```

### Environment Variables

The project uses environment variables for configuration. See the [Prerequisites](#3-j-quants-api-token) section for details on setting up your J-Quants API token.

Create a `.env` file in the project root with the following variables:

```bash
# Required: J-Quants API access
JQ_REFRESH_TOKEN=your_token_here

# Optional: Storage backend configuration
# Switch the demo namespace (bronze/silver/gold) to MinIO instead of filesystem
JQSYS_DEMO_BACKEND=minio  # Defaults to filesystem when unset

# Override the filesystem storage root (defaults to ./var/blob_storage/)
BLOB_STORAGE_PATH=/absolute/path/to/blob_storage

# Optional: MinIO credentials (only needed when JQSYS_DEMO_BACKEND=minio)
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=jq-data
```

### Storage Backend Configuration

The project supports two storage backend modes:

#### Option 1: Filesystem Storage (No Docker Required) - DEFAULT

Use local filesystem storage for all data layers. This is the **default** and simplest setup - no Docker required!

**Default storage location**: `./var/blob_storage/`
- Bronze layer: `./var/blob_storage/bronze/`
- Silver layer: `./var/blob_storage/silver/`
- Gold layer: `./var/blob_storage/gold/`

**No configuration needed** - the storage layers automatically use filesystem backends by default:

```python
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.silver import SilverStorage
from jqsys.data.layers.gold import GoldStorage
from jqsys.fin import Stock

# Just instantiate - uses filesystem storage by default
bronze = BronzeStorage()
silver = SilverStorage()
gold = GoldStorage()

# Stock API also works out of the box
stock = Stock("7203")  # Toyota Motor
```

**Customizing storage**:

You can adjust the demo namespace or override individual layers with environment variables:

```bash
# Switch the demo namespace (bronze/silver/gold) to MinIO
export JQSYS_DEMO_BACKEND=minio

# Use a custom filesystem location for demo storage
export BLOB_STORAGE_PATH=$PWD/.local/blob_storage

# Point a single layer at a different backend name (advanced)
export BRONZE_BACKEND=demo.bronze
export SILVER_BACKEND=demo.silver
export GOLD_BACKEND=demo.gold

# You can also modify configs/blob_backends.py to register additional backends
```

#### Option 2: MinIO Backend with Docker Compose

Use MinIO for object storage (similar to AWS S3). This requires Docker.

**Step 1: Start MinIO services**:

```bash
# Start MinIO (and MongoDB) services
docker compose up -d

# Verify MinIO is running
docker compose ps

# Access MinIO console at http://localhost:9001
# Login: minioadmin / minioadmin
```

**Step 2: Configure your application to use MinIO**:

Switch the demo namespace to MinIO by setting the following environment variables
(the defaults are shown for reference):

```bash
export JQSYS_DEMO_BACKEND=minio
export MINIO_ENDPOINT=${MINIO_ENDPOINT:-localhost:9000}
export MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
export MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}
export MINIO_BUCKET=${MINIO_BUCKET:-jq-data}
```

Add the same entries to your `.env` file if you prefer a persistent setup. When
`JQSYS_DEMO_BACKEND=minio`, the `demo.bronze`, `demo.silver`, and `demo.gold`
backends automatically use MinIO with prefixes for each layer—no additional
per-layer configuration is required.

**Stop MinIO services**:

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
