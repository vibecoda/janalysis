# Janalysis

Lightweight J-Quants utilities for CLI demos and financial analysis with automated testing.

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd janalysis

# Install git hooks (IMPORTANT: Run this after cloning)
./setup-hooks.sh

# Install dependencies
pip install -e ".[dev]"
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
- **Bronze Layer**: Raw data storage with partitioning
- **Silver Layer**: Normalized and validated data  
- **Query Engine**: SQL interface for financial analysis
- **Stock & Portfolio APIs**: High-level analysis tools
- **Comprehensive Tests**: 112+ tests ensuring reliability

### Project Structure

```
jqsys/
├── auth.py          # Authentication utilities
├── client.py        # J-Quants API client
├── stock.py         # Stock analysis API
├── portfolio.py     # Portfolio management API
└── storage/
    ├── bronze.py    # Raw data storage
    ├── silver.py    # Normalized data storage
    └── query.py     # SQL query engine
```

### Environment Variables

Configure data directory location:

```bash
# Optional: Set custom data directory
export JQSYS_DATA_ROOT=/path/to/your/data

# Required for API access
export JQUANTS_REFRESH_TOKEN=your_token_here
```

## Testing

The test suite includes 112+ comprehensive tests covering:
- Authentication and API client functionality
- Data storage and retrieval (Bronze/Silver layers)
- SQL query engine with security testing
- Integration tests with realistic data
- Environment variable configuration

Run tests with detailed output:

```bash
python run_tests.py
```

## Contributing

1. Clone the repository
2. **Run `./setup-hooks.sh`** (essential for development)
3. Install dependencies: `pip install -e ".[dev]"`
4. Make your changes
5. Tests will run automatically on commit
6. Create a pull request

The pre-commit hook ensures all tests pass before allowing commits, maintaining code quality and preventing regressions.