# J-Quants CLI Demos

Simple terminal programs to explore the J-Quants API using a refresh token in `.env` as `JQ_REFRESH_TOKEN`.

## Setup
- Activate venv: `source .venv/bin/activate`
- Install deps: `pip install -U pip pandas requests`
- Ensure `.env` contains: `JQ_REFRESH_TOKEN=...`

## Commands
- Auth: `python scripts/jq_auth.py`
- Listings: `python scripts/jq_listed_info.py --date 20250530`
- Daily quotes: `python scripts/jq_daily_quotes.py --code 6331 --from 20250301 --to 20250501`
- Statements: `python scripts/jq_fins_statements.py --code 6331`
- Announcements: `python scripts/jq_fins_announcement.py`
- Trading calendar: `python scripts/jq_trading_calendar.py --from 20250101 --to 20251231`

Each command prints a preview to stdout. Use `--limit 0` to print all rows. `jq_daily_quotes.py` supports `--save path.csv|path.parquet`.

## Notes
- These scripts implement automatic pagination and basic retries.
- Tokens are refreshed per run; no token caching is persisted.
- See `jquants_api_quick_start_en.py` for the canonical tutorial reference.
