"""Query interface using DuckDB for analytical queries on stored data.

Provides SQL-based analytical capabilities over the bronze/silver/gold layers
with optimized performance for financial timeseries analysis.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class QueryEngine:
    """DuckDB-based query engine for financial data analysis."""

    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """Singleton pattern to reuse connections, except in test environments."""
        # Allow multiple instances in test environments (when pytest is running)
        import sys

        if "pytest" in sys.modules:
            return super().__new__(cls)

        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        db_path: str | Path | None = None,
        bronze_path: str | Path | None = None,
        silver_path: str | Path | None = None,
        gold_path: str | Path | None = None,
    ):
        """Initialize query engine.

        Args:
            db_path: Path to DuckDB database file (None for in-memory)
            bronze_path: Path to bronze layer data. If None, checks JQSYS_DATA_ROOT
                        environment variable, otherwise defaults to "data/bronze"
            silver_path: Path to silver layer data. If None, checks JQSYS_DATA_ROOT
                        environment variable, otherwise defaults to "data/silver"
            gold_path: Path to gold layer data. If None, checks JQSYS_DATA_ROOT
                      environment variable, otherwise defaults to "data/gold"
        """
        # Only initialize once (singleton pattern), except in tests
        import sys

        if "pytest" not in sys.modules and self._initialized:
            return

        self.db_path = db_path

        # Get data root from environment or default
        data_root = os.getenv("JQSYS_DATA_ROOT", "data")
        data_root_path = Path(data_root)

        # Set paths with environment variable support
        self.bronze_path = (
            Path(bronze_path) if bronze_path is not None else data_root_path / "bronze"
        )
        self.silver_path = (
            Path(silver_path) if silver_path is not None else data_root_path / "silver"
        )
        self.gold_path = Path(gold_path) if gold_path is not None else data_root_path / "gold"

        # Initialize connection
        self.conn = duckdb.connect(str(db_path) if db_path else ":memory:")

        # Install and load required extensions
        self._setup_extensions()

        # Create views for data layers
        self._create_data_views()

        # Mark as initialized
        self._initialized = True

    def _setup_extensions(self) -> None:
        """Install and load required DuckDB extensions."""
        try:
            # Install Parquet support (should be built-in)
            self.conn.execute("INSTALL parquet")
            self.conn.execute("LOAD parquet")

            logger.info("DuckDB extensions loaded successfully")
        except Exception as e:
            logger.warning(f"Extension setup warning: {e}")

    def _create_data_views(self) -> None:
        """Create views for easy access to data layers."""
        try:
            # Create view for daily prices (silver layer)
            daily_prices_pattern = str(self.silver_path / "daily_prices" / "**" / "*.parquet")
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW daily_prices AS
                SELECT * FROM read_parquet('{daily_prices_pattern}')
                WHERE date IS NOT NULL AND code IS NOT NULL
            """)

            # Create view for raw daily quotes (bronze layer)
            daily_quotes_pattern = str(self.bronze_path / "daily_quotes" / "**" / "*.parquet")
            if self.bronze_path.exists():
                try:
                    self.conn.execute(f"""
                        CREATE OR REPLACE VIEW raw_daily_quotes AS
                        SELECT * FROM read_parquet('{daily_quotes_pattern}')
                        WHERE _endpoint = 'daily_quotes'
                    """)
                except Exception as bronze_e:
                    logger.warning(f"Bronze view creation warning: {bronze_e}")

            logger.info("Data views created successfully")

        except Exception as e:
            logger.warning(f"View creation warning: {e}")

    def execute_sql(self, query: str) -> pl.DataFrame:
        """Execute SQL query and return results as Polars DataFrame.

        Args:
            query: SQL query string

        Returns:
            Query results as Polars DataFrame
        """
        try:
            result = self.conn.execute(query).fetchdf()
            return pl.from_pandas(result)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def execute_sql_with_params(self, query: str, params: list[Any]) -> pl.DataFrame:
        """Execute parameterized SQL query and return results as Polars DataFrame.

        Args:
            query: SQL query string with placeholders
            params: Parameters to bind to placeholders

        Returns:
            Query results as Polars DataFrame
        """
        try:
            result = self.conn.execute(query, params).fetchdf()
            return pl.from_pandas(result)
        except Exception as e:
            logger.error(f"Parameterized query execution failed: {e}")
            raise

    def get_daily_prices(
        self,
        codes: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Get daily prices with optional filters.

        Args:
            codes: Stock codes to filter
            start_date: Start date filter
            end_date: End date filter
            limit: Maximum number of records

        Returns:
            Filtered daily prices data
        """
        query = "SELECT * FROM daily_prices WHERE 1=1"
        params = []

        if codes:
            placeholders = ",".join("?" * len(codes))
            query += f" AND code IN ({placeholders})"
            params.extend(codes)

        if start_date:
            query += " AND date >= ?"
            params.append(start_date.strftime("%Y-%m-%d"))

        if end_date:
            query += " AND date <= ?"
            params.append(end_date.strftime("%Y-%m-%d"))

        query += " ORDER BY date, code"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        result = self.execute_sql_with_params(query, params)

        # Convert datetime objects to date objects for date column
        if "date" in result.columns:
            result = result.with_columns(pl.col("date").dt.date())

        return result

    def get_price_summary_stats(
        self,
        codes: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pl.DataFrame:
        """Get summary statistics for price data.

        Args:
            codes: Stock codes to analyze
            start_date: Start date filter
            end_date: End date filter

        Returns:
            Summary statistics by stock code
        """
        query = """
        SELECT
            code,
            COUNT(*) as record_count,
            MIN(date) as first_date,
            MAX(date) as last_date,
            MIN(close) as min_close,
            MAX(close) as max_close,
            AVG(close) as avg_close,
            STDDEV(close) as std_close,
            SUM(volume) as total_volume
        FROM daily_prices
        WHERE 1=1
        """
        params = []

        if codes:
            placeholders = ",".join("?" * len(codes))
            query += f" AND code IN ({placeholders})"
            params.extend(codes)

        if start_date:
            query += " AND date >= ?"
            params.append(start_date.strftime("%Y-%m-%d"))

        if end_date:
            query += " AND date <= ?"
            params.append(end_date.strftime("%Y-%m-%d"))

        query += " GROUP BY code ORDER BY code"

        return self.execute_sql_with_params(query, params)

    def calculate_returns(
        self,
        codes: list[str] | None = None,
        periods: list[int] = None,  # 1D, 1W, 1M
    ) -> pl.DataFrame:
        """Calculate returns over multiple periods.

        Args:
            codes: Stock codes to analyze
            periods: List of periods (in days) to calculate returns

        Returns:
            DataFrame with returns calculations
        """
        # Build the LAG expressions for each period
        if periods is None:
            periods = [1, 5, 21]
        lag_expressions = []
        return_expressions = []

        for period in periods:
            lag_col = f"close_lag_{period}"
            return_col = f"return_{period}d"

            lag_expressions.append(
                f"LAG(close, {period}) OVER (PARTITION BY code ORDER BY date) as {lag_col}"
            )
            return_expressions.append(f"(close - {lag_col}) / {lag_col} as {return_col}")

        lag_sql = ", ".join(lag_expressions)
        return_sql = ", ".join(return_expressions)

        query = f"""
        WITH price_lags AS (
            SELECT
                code, date, close,
                {lag_sql}
            FROM daily_prices
        )
        SELECT
            code, date, close,
            {return_sql}
        FROM price_lags
        WHERE 1=1
        """

        if codes:
            placeholders = ",".join("?" * len(codes))
            query += f" AND code IN ({placeholders})"
            return self.execute_sql_with_params(query, codes)

        query += " ORDER BY code, date"

        return self.execute_sql(query)

    def get_market_data_coverage(self) -> pl.DataFrame:
        """Get data coverage statistics.

        Returns:
            DataFrame with coverage statistics by date and endpoint
        """
        query = """
        SELECT
            date,
            COUNT(DISTINCT code) as unique_codes,
            COUNT(*) as total_records,
            MIN(close) as min_close,
            MAX(close) as max_close,
            AVG(volume) as avg_volume
        FROM daily_prices
        GROUP BY date
        ORDER BY date DESC
        """

        return self.execute_sql(query)

    def find_missing_data(
        self,
        codes: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pl.DataFrame:
        """Identify missing data points in timeseries.

        Args:
            codes: Stock codes to check
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            DataFrame with missing data analysis
        """
        start_date_str = start_date.strftime("%Y-%m-%d") if start_date else "2020-01-01"
        end_date_str = end_date.strftime("%Y-%m-%d") if end_date else "2024-12-31"

        query = f"""
        WITH date_range AS (
            SELECT UNNEST(generate_series(
                DATE '{start_date_str}',
                DATE '{end_date_str}',
                INTERVAL 1 DAY
            )) as expected_date
        ),
        code_dates AS (
            SELECT DISTINCT code FROM daily_prices
        ),
        expected_records AS (
            SELECT
                c.code,
                d.expected_date as date
            FROM code_dates c
            CROSS JOIN date_range d
        ),
        actual_records AS (
            SELECT code, date
            FROM daily_prices
            WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}'
        )
        SELECT
            e.code,
            e.date as missing_date
        FROM expected_records e
        LEFT JOIN actual_records a ON e.code = a.code AND e.date = a.date
        WHERE a.code IS NULL
        """

        if codes:
            placeholders = ",".join("?" * len(codes))
            query += f" AND e.code IN ({placeholders})"
            query += " ORDER BY e.code, e.date"
            result = self.execute_sql_with_params(query, codes)
        else:
            query += " ORDER BY e.code, e.date"
            result = self.execute_sql(query)

        # Convert datetime objects to date objects for missing_date column
        if "missing_date" in result.columns:
            result = result.with_columns(pl.col("missing_date").dt.date())

        return result

    def close(self) -> None:
        """Close the database connection."""
        import sys

        # In tests, close normally. In production, keep singleton alive
        if "pytest" in sys.modules and hasattr(self, "conn") and self.conn:
            self.conn.close()
            logger.info("Query engine connection closed")
        # Don't close singleton connection in production - it persists for reuse

    def force_close(self) -> None:
        """Force close the connection (for cleanup)."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            logger.info("Query engine connection closed")
            self._initialized = False
            self.__class__._instance = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Parameters are required by context manager protocol but not used
        self.close()
