# J-Quants Data Platform & Recommender — Project Plan

## Objectives
- Data foundation: Store clean, complete daily prices and fundamentals for all JP stocks via J-Quants.
- Feature pipeline: Compute robust factors (value, quality, momentum, size, volatility).
- Recommender: Rank stocks and construct investable portfolios with constraints.
- Backtesting: Evaluate with realistic frictions; report risk/return and turnover.
- Operations: Reliable daily updates, observability, and reproducibility.

## Scope
- Covered: Daily prices, corporate actions (splits/dividends), listings/universe, sector/industry, fundamentals/indicators.
- Out of scope (initial): Real-time intraday data, options/derivatives, live trading execution.

## High-Level Architecture
- Ingestion: `JQuantsClient` (auth + REST), endpoint modules, incremental fetch, backfill.
- Storage: Bronze (raw JSONL), Silver (normalized tables), Gold (feature tables, signals).
- Processing: Transformations, corporate action adjustment, factor computation, sector neutralization.
- Modeling: Ranker + portfolio construction; parameterized configs.
- Evaluation: Backtest engine with trading calendar and costs.
- Ops: Scheduler, logging, data quality checks, idempotent runs.

## Data Model (Core Tables)
- `securities`: code, name, market, sector, list_date, delist_date, status.
- `daily_prices`: date, code, open, high, low, close, adj_close, volume, turnover, adj_factor, source_ts.
- `dividends`: ex_date, pay_date, code, amount, currency, type.
- `splits`: ex_date, code, split_ratio (new/old).
- `fundamentals_quarterly`: code, period_end, fiscal_year, items (rev, op_income, net_income, assets, equity, cf, shares, etc.).
- `indicators_daily` (if API provides): code, date, PER, PBR, dividend_yield, market_cap.
- `universe_snapshots`: date, code, include_reason, tradable_flag.
- `features_daily`: date, code, feature_name(s) or wide schema with columns.
- `signals_daily`: date, code, composite_score, rank, decile, metadata.
- `trades_daily`: date, code, weight, target_shares, exec_price, notes.
- `backtest_runs`: run_id, config_hash, start, end, metrics_json, version_info.

## Storage Strategy
✅ **DECIDED** - DuckDB + Polars hybrid approach selected based on comprehensive research:

**Architecture:**
- **Bronze Layer**: Raw J-Quants API responses stored as Parquet files, partitioned by `endpoint/date=YYYY-MM-DD`
- **Silver Layer**: Normalized timeseries tables (daily_prices, fundamentals, etc.) in Parquet format
- **Gold Layer**: Feature-engineered datasets and signals optimized for backtesting
- **Query Engine**: DuckDB for complex SQL analytics, Polars for DataFrame operations and ETL
- **Corporate Actions**: Custom adjustment factor logic in Polars with versioned corrections

**Benefits of chosen approach:**
- Zero operational overhead (embedded solutions)
- Exceptional performance (Polars used by major quant firms)
- Excellent Python ecosystem integration
- Cost-effective (fully open source)
- Migration path to TimescaleDB available if scaling needs change

**File Organization:**
```
data/
  bronze/daily_quotes/date=2024-01-15/data.parquet
  bronze/financials/date=2024-01-15/data.parquet
  silver/daily_prices/year=2024/month=01/data.parquet
  gold/features_daily/year=2024/month=01/data.parquet
```

## J-Quants Integration
- Auth: Use `.env` `JQ_REFRESH_TOKEN` → exchange for access token; auto-refresh; exponential backoff; retry on 429/5xx.
- Endpoints (typical): daily quotes, listed/universe info, sectors/industry, dividends, splits, fundamentals/financials, indicators. Map each to an adapter.
- Incremental: Use `dateFrom/dateTo` style params where available; persist last successful date per endpoint.
- Idempotency: Write temp file, validate row counts and schema, then move/commit.

## Ingestion Workflow
- Backfill: Endpoint-by-endpoint historical pull with rate-limit awareness and checkpointing.
- Daily update: After market close + data availability SLA; run date T update; recheck corporate actions/fundamentals lag.
- Data quality: Unique (date, code), non-null essentials, price and volume sanity ranges, calendar completeness, sector coverage.
- Corporate actions: Maintain cumulative adjustment factor; create `adj_close`/`adj_ohlc`; re-adjust history on late CA.

## Transformations
- Universe: Filter tradable (listed, non-stopped), minimum price/volume, remove recent IPOs if needed.
- Returns: 1D/5D/21D/63D; use adjusted prices; handle missing via forward-fill within trading calendar (limited).
- Standardization: Winsorize (e.g., 1st/99th pct), z-score within sector, optionally market-cap neutralization.
- Fundamentals alignment: Point-in-time handling; lag financials to public date to avoid lookahead.
- Feature table: Wide Parquet with columns like `mom_3m`, `val_pbr_inv`, `quality_roe`, `size_ln_mcap`, `vol_20d`.

## Recommender & Portfolio
- Ranking: Composite score via weighted sum of standardized factors, or simple rank aggregation.
- Portfolio rules: Long-only top-N or top-X% per sector; max weight per stock; sector caps; turnover limit; min trade notional.
- Rebalance: Monthly/weekly; with daily drift tolerance.
- Costs: Commission + slippage model based on liquidity/volatility.

## Backtesting & Evaluation
- Engine: Time-series split; purged rolling windows; position book; orders with costs; end-of-day execution assumption.
- Metrics: CAGR, vol, Sharpe, max drawdown, Calmar, hit rate, turnover, sector exposures.
- Reports: Equity curve, drawdown, IC/RankIC by factor, performance by decile, attribution by factor/sector.

## Scheduling & Ops
- Scheduler: `cron`/`APScheduler` for local; CI job for nightly checks.
- Logging: Structured logs per run; summary artifact with row counts and durations.
- Monitoring: Alerts on missing data, unusual gaps, schema drift, API errors.
- Reproducibility: Config-driven (YAML/JSON) for dates/universe/factors; version the config in results.

## Security & Compliance
- Secrets: `.env` for tokens; never commit; rotate refresh token periodically.
- PII: None expected; ensure logs don’t include tokens or full headers.
- Rate limits: Central throttle; respect per-endpoint quotas.

## Repository Structure
```
jqsys/                       # Main installable package
  __init__.py
  core/                      # Core infrastructure
    storage/                 # Storage abstraction layer
      blob.py                # Blob storage interface
      object.py              # Object storage interface
      registry.py            # Backend registry
      backends/              # Storage backend implementations
        filesystem_backend.py
        minio_backend.py
        mongodb_backend.py
        prefixed_backend.py
    utils/                   # Core utilities
      config.py              # Configuration management
      env.py                 # Environment file utilities
  data/                      # Data ingestion and processing
    auth.py                  # J-Quants authentication
    client.py                # J-Quants API client with retries
    ingest.py                # Data ingestion pipelines
    layers/                  # Data lake layers
      bronze.py              # Raw data storage (bronze layer)
      silver.py              # Normalized timeseries (silver layer)
      gold.py                # Feature-engineered data (gold layer)
      query.py               # DuckDB analytical queries
  fin/                       # Financial analysis modules
    stock.py                 # Stock and portfolio analysis
  (future modules...)
    transform/               # Prices, CA, fundamentals transforms
    features/                # Momentum, value, quality, size, volatility
    recommend/               # Ranker and portfolio construction
    backtest/                # Engine, metrics, reports
scripts/                     # CLI and automation scripts
  api/                       # J-Quants API scripts
    jq_auth.py               # Authentication demo
    jq_daily_quotes.py       # Daily price fetching
    jq_fins_announcement.py  # Financial announcements
    jq_fins_statements.py    # Financial statements
    jq_listed_info.py        # Listed company info
    jq_trading_calendar.py   # Trading calendar
  batch/                     # Batch processing scripts
    ingest_daily_quotes.py   # Ingest daily quotes to bronze
    normalize_daily_quotes.py # Normalize to silver layer
    transform_daily_prices.py # Transform to gold layer
    ingest_listed_info.py    # Ingest company listings
  demo/                      # Demo and example scripts
    demo_storage_apis.py     # Storage layer demonstrations
    demo_stock_api.py        # Stock API demonstrations
    demo_registry.py         # Backend registry examples
    demo_filesystem_storage.py # Filesystem backend demo
  (future scripts...)
    backfill.py
    daily_update.py
    run_backtest.py
pyproject.toml               # Package configuration
configs/                     # Configuration files
  (future config files...)
  ingest.yml
  features.yml
  portfolio.yml
  backtest.yml
notebooks/                   # Jupyter notebooks
  (example notebooks...)
docs/                        # Documentation
  AGENTS.md                  # Repository guidelines
  project_plan.md            # This file
  (other docs...)
tests/                       # Pytest test suite
  test_auth.py
  test_client.py
  test_*_storage.py
  (more tests...)
```

## Milestones & Timeline
- M1: Foundations (Week 1) ✅ **COMPLETED**
  - Env setup, repo skeleton, auth client, calendar util, logging.
  - Package structure: Moved jqsys to installable package with proper pyproject.toml
  - Working CLI scripts for auth, daily quotes, financial data, trading calendar
  - Deliverable: Auth works; dry-run connectivity to 1–2 endpoints.
- M1.5: Timeseries Storage Research (Week 1.5) ✅ **COMPLETED**
  - Research and evaluate opensource timeseries databases and libraries
  - Evaluated: InfluxDB, TimescaleDB, QuestDB, Arctic/ArcticDB, TileDB, DuckDB, Polars
  - Created comprehensive decision matrix with scores across 5 criteria
  - **DECISION**: DuckDB + Polars hybrid approach for optimal performance with minimal operational overhead
  - Rationale: Best fit for single-developer environment with proven financial industry usage
  - Deliverable: Complete research document with implementation strategy
- M2: Ingestion Core (Weeks 2–3) ✅ **COMPLETED**
  - ✅ **COMPLETED**: DuckDB + Polars storage foundation implemented
  - ✅ **COMPLETED**: Bronze layer raw data storage with partitioning
  - ✅ **COMPLETED**: Silver layer normalization with data quality validation  
  - ✅ **COMPLETED**: DuckDB query engine with analytical capabilities
  - ✅ **COMPLETED**: Daily quotes ingestion pipeline with real J-Quants data (4,206 records processed)
  - ✅ **COMPLETED**: Stock and Portfolio API modules for high-level analysis
  - ✅ **COMPLETED**: Environment variable configuration and comprehensive testing (112+ tests)
  - ✅ **COMPLETED**: Git hooks setup for automated test enforcement
  - **REMAINING**: Endpoint modules for sectors, dividends, splits, listed companies
  - **REMAINING**: Backfill job for historical data
  - Deliverable: Full historical prices + CA normalized; daily job runnable.
- M2.5: Corporate Actions & Price Adjustments (Week 3) 🚀 **NEXT PRIORITY**
  - Corporate action adjustment system using J-Quants AdjustmentFactor data
  - Historical price series reconstruction with split/merger handling
  - Validation framework for adjustment accuracy and consistency
  - Integration with existing Silver layer for seamless adjusted price access
  - Deliverable: Robust price adjustment system for accurate financial analysis
- M3: Fundamentals & Indicators (Week 4)
  - Financials and daily indicators ingestion; point-in-time alignment.
  - Deliverable: `fundamentals_quarterly`, `indicators_daily` populated.
- M4: Features (Week 5)
  - Factor calculations, standardization, sector-neutral features.
  - Deliverable: `features_daily` table with configs and QA plots.
- M5: Recommender & Portfolio (Week 6)
  - Composite ranking + portfolio constraints; turnover-aware rebalancing.
  - Deliverable: `signals_daily`, portfolio weights generator.
- M6: Backtest & Reports (Weeks 7–8)
  - Backtest engine, costs, metrics; reporting.
  - Deliverable: Benchmark vs TOPIX report; configs and run artifacts.
- M7: Ops Hardening (Week 9)
  - Scheduler, DQ alerts, idempotency, documentation.
  - Deliverable: One-click backfill and daily update scripts.

## Acceptance Criteria
- Data: 99.9% completeness for trading days; no duplicate `(date, code)`; corporate actions correctly applied.
- Features: Deterministic outputs for fixed config and data snapshot.
- Model: Reproducible rankings; constraints respected; turnover within target.
- Backtest: Metrics computed; no lookahead/leakage checks passing.
- Ops: Daily job succeeds unattended; clear logs; reruns are idempotent.

## Risks & Mitigations
- API limits/outages: Centralized throttling; cached bronze; resumable backfills.
- Schema changes: Versioned parsers; validation with fast fail and alerts.
- Corporate action drift: Recompute adj factors on CA updates; maintain tests.
- Lookahead bias: Enforce publish-date lags; purged CV; PIT joins.

## Next Steps
- **PRIORITY**: Corporate Actions & Price Adjustments (M2.5 start)
  - Implement corporate action detection using J-Quants AdjustmentFactor data
  - Create adjustment factor analysis and historical price reconstruction system
  - Enhance Silver layer to store both raw and adjusted prices with tracking
  - Build validation framework for adjustment accuracy and consistency
  - Update Stock and Portfolio APIs with corporate action awareness
  - See detailed implementation plan: `docs/corporate_actions_plan.md`
- Complete remaining M2 endpoints: sectors, dividends, splits, listed companies
- Approve factor set and portfolio constraints for M4 features phase
