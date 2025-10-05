# J-Quants CLI Demos

Simple terminal programs to explore the J-Quants API using a refresh token in `.env` as `JQ_REFRESH_TOKEN`.

## Setup
- Activate venv: `source .venv/bin/activate`
- Install deps: `pip install -U pip pandas requests`
- Ensure `.env` contains: `JQ_REFRESH_TOKEN=...`

## Commands

### API Scripts (`scripts/api/`)
- Auth: `python scripts/api/jq_auth.py`
- Listings: `python scripts/api/jq_listed_info.py --date 20250530`
- Daily quotes: `python scripts/api/jq_daily_quotes.py --code 6331 --from 20250301 --to 20250501`
- Statements: `python scripts/api/jq_fins_statements.py --code 6331`
- Announcements: `python scripts/api/jq_fins_announcement.py`
- Trading calendar: `python scripts/api/jq_trading_calendar.py --from 20250101 --to 20251231`

### Batch Processing Scripts (`scripts/batch/`)
- Ingest daily quotes: `python scripts/batch/ingest_daily_quotes.py --date 20250530`
- Normalize quotes: `python scripts/batch/normalize_daily_quotes.py`
- Transform prices: `python scripts/batch/transform_daily_prices.py`
- Ingest listings: `python scripts/batch/ingest_listed_info.py --date 20250530`

### Demo Scripts (`scripts/demo/`)
- Storage API demo: `python scripts/demo/demo_storage_apis.py`
- Stock API demo: `python scripts/demo/demo_stock_api.py`
- Backend registry: `python scripts/demo/demo_registry.py`
- Filesystem storage: `python scripts/demo/demo_filesystem_storage.py`

Each command prints a preview to stdout. Use `--limit 0` to print all rows where applicable. `jq_daily_quotes.py` supports `--save path.csv|path.parquet`.

## Notes
- These scripts implement automatic pagination and basic retries.
- Tokens are refreshed per run; no token caching is persisted.
- Batch scripts handle bronze/silver layer data ingestion and transformation.
