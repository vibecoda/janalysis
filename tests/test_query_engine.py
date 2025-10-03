from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import polars as pl
import pytest

from jqsys.storage.query import QueryEngine


class TestQueryEngine:
    @pytest.fixture
    def sample_daily_prices_data(self):
        return pl.DataFrame(
            {
                "code": ["1301", "1301", "1332", "1332"],
                "date": [
                    date(2024, 1, 15),
                    date(2024, 1, 16),
                    date(2024, 1, 15),
                    date(2024, 1, 16),
                ],
                "open": [100.0, 102.0, 200.0, 205.0],
                "high": [105.0, 108.0, 210.0, 215.0],
                "low": [98.0, 100.0, 198.0, 203.0],
                "close": [102.0, 106.0, 205.0, 210.0],
                "volume": [100000, 120000, 50000, 60000],
                "turnover_value": [10200000.0, 12720000.0, 10250000.0, 12600000.0],
                "adjustment_factor": [1.0, 1.0, 1.1, 1.1],
                "adj_close": [102.0, 106.0, 225.5, 231.0],
                "processed_at": ["2024-01-15T10:00:00"] * 4,
            }
        )

    def test_init_in_memory_database(self, tmp_path):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        assert query_engine.db_path is None
        assert query_engine.bronze_path == tmp_path / "bronze"
        assert query_engine.silver_path == tmp_path / "silver"
        assert query_engine.conn is not None

    def test_init_with_database_file(self, tmp_path):
        db_path = tmp_path / "test.db"
        query_engine = QueryEngine(
            db_path=db_path, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        assert query_engine.db_path == db_path
        query_engine.close()

    def test_setup_extensions(self, tmp_path):
        with patch("jqsys.storage.query.logger") as mock_logger:
            query_engine = QueryEngine(
                db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
            )

            # Should log successful extension loading
            mock_logger.info.assert_called_with("DuckDB extensions loaded successfully")
            query_engine.close()

    def test_setup_extensions_warning(self, tmp_path):
        with patch.object(duckdb.DuckDBPyConnection, "execute") as mock_execute:
            mock_execute.side_effect = Exception("Extension error")

            with patch("jqsys.storage.query.logger") as mock_logger:
                query_engine = QueryEngine(
                    db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
                )

                # Should log warning on extension setup failure
                mock_logger.warning.assert_called()
                query_engine.close()

    def test_create_data_views_success(self, tmp_path):
        # Create some dummy parquet files
        silver_dir = tmp_path / "silver" / "daily_prices" / "year=2024" / "month=01"
        silver_dir.mkdir(parents=True)

        # Create a minimal valid parquet file
        import polars as pl

        dummy_df = pl.DataFrame({"code": ["1301"], "date": ["2024-01-15"], "close": [100.0]})
        dummy_df.write_parquet(silver_dir / "day=15.parquet")

        bronze_dir = tmp_path / "bronze" / "daily_quotes" / "date=2024-01-15"
        bronze_dir.mkdir(parents=True)
        dummy_df.write_parquet(bronze_dir / "data.parquet")

        with patch("jqsys.storage.query.logger") as mock_logger:
            query_engine = QueryEngine(
                db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
            )

            # Should log successful view creation
            mock_logger.info.assert_called_with("Data views created successfully")
            query_engine.close()

    def test_create_data_views_warning(self, tmp_path):
        # Test view creation with invalid paths (should warn but not fail)
        with patch("jqsys.storage.query.logger") as mock_logger:
            query_engine = QueryEngine(
                db_path=None,
                bronze_path=tmp_path / "nonexistent_bronze",
                silver_path=tmp_path / "nonexistent_silver",
            )

            # Should log warning on view creation issues
            mock_logger.warning.assert_called()
            query_engine.close()

    def test_execute_sql_basic(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        # Create a temporary table for testing
        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE test_data AS SELECT * FROM temp_df")

        # Execute a simple query
        result = query_engine.execute_sql("SELECT COUNT(*) as count FROM test_data")

        assert isinstance(result, pl.DataFrame)
        assert result["count"][0] == 4

        query_engine.close()

    def test_execute_sql_exception_handling(self, tmp_path):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        with pytest.raises(Exception):
            query_engine.execute_sql("SELECT * FROM nonexistent_table")

        query_engine.close()

    def test_get_daily_prices_no_filters(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        # Mock the daily_prices view
        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_daily_prices()

        assert len(result) == 4
        assert "code" in result.columns
        assert "date" in result.columns
        assert "close" in result.columns

        query_engine.close()

    def test_get_daily_prices_with_code_filter(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_daily_prices(codes=["1301"])

        assert len(result) == 2  # Only 1301 records
        assert all(code == "1301" for code in result["code"])

        query_engine.close()

    def test_get_daily_prices_with_date_filters(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_daily_prices(
            start_date=datetime(2024, 1, 15), end_date=datetime(2024, 1, 15)
        )

        assert len(result) == 2  # Only 2024-01-15 records
        assert all(str(d) == "2024-01-15" for d in result["date"])

        query_engine.close()

    def test_get_daily_prices_with_limit(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_daily_prices(limit=2)

        assert len(result) <= 2

        query_engine.close()

    def test_get_price_summary_stats(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_price_summary_stats()

        assert len(result) == 2  # Two unique codes
        assert "code" in result.columns
        assert "record_count" in result.columns
        assert "min_close" in result.columns
        assert "max_close" in result.columns
        assert "avg_close" in result.columns

        # Check that we have stats for both codes
        codes = result["code"].to_list()
        assert "1301" in codes
        assert "1332" in codes

        query_engine.close()

    def test_get_price_summary_stats_with_filters(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_price_summary_stats(
            codes=["1301"], start_date=datetime(2024, 1, 15), end_date=datetime(2024, 1, 15)
        )

        assert len(result) == 1
        assert result["code"][0] == "1301"
        assert result["record_count"][0] == 1  # Only one record for that date

        query_engine.close()

    def test_calculate_returns_basic(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.calculate_returns(periods=[1])

        assert len(result) == 4
        assert "code" in result.columns
        assert "date" in result.columns
        assert "close" in result.columns
        assert "return_1d" in result.columns

        # Check that returns are calculated correctly
        # For code 1301: (106 - 102) / 102 ≈ 0.039
        returns_1301 = result.filter(
            (pl.col("code") == "1301") & (pl.col("date") == date(2024, 1, 16))
        )["return_1d"]

        if len(returns_1301) > 0:
            expected_return = (106.0 - 102.0) / 102.0
            assert returns_1301[0] == pytest.approx(expected_return, rel=1e-3)

        query_engine.close()

    def test_calculate_returns_multiple_periods(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.calculate_returns(periods=[1, 5])

        assert "return_1d" in result.columns
        assert "return_5d" in result.columns

        query_engine.close()

    def test_calculate_returns_with_code_filter(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.calculate_returns(codes=["1301"], periods=[1])

        # Should only have 1301 records
        unique_codes = result["code"].unique()
        assert len(unique_codes) == 1
        assert unique_codes[0] == "1301"

        query_engine.close()

    def test_get_market_data_coverage(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.get_market_data_coverage()

        assert len(result) == 2  # Two unique dates
        assert "date" in result.columns
        assert "unique_codes" in result.columns
        assert "total_records" in result.columns
        assert "min_close" in result.columns
        assert "max_close" in result.columns
        assert "avg_volume" in result.columns

        # Check coverage stats
        for row in result.iter_rows(named=True):
            assert row["unique_codes"] == 2  # Both codes present each date
            assert row["total_records"] == 2  # Two records per date

        query_engine.close()

    def test_find_missing_data_basic(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        # Test finding missing data in a range where we have gaps
        result = query_engine.find_missing_data(
            start_date=datetime(2024, 1, 14),  # Day before our data
            end_date=datetime(2024, 1, 17),  # Day after our data
        )

        assert "code" in result.columns
        assert "missing_date" in result.columns

        # Should find missing dates for both codes
        missing_dates = result["missing_date"].unique()
        assert date(2024, 1, 14) in missing_dates.to_list()
        assert date(2024, 1, 17) in missing_dates.to_list()

        query_engine.close()

    def test_find_missing_data_with_code_filter(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        result = query_engine.find_missing_data(
            codes=["1301"], start_date=datetime(2024, 1, 14), end_date=datetime(2024, 1, 17)
        )

        # Should only find missing data for code 1301
        unique_codes = result["code"].unique()
        assert len(unique_codes) == 1
        assert unique_codes[0] == "1301"

        query_engine.close()

    def test_context_manager(self, tmp_path):
        with QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        ) as query_engine:
            # Should be able to use the query engine
            result = query_engine.execute_sql("SELECT 1 as test")
            assert result["test"][0] == 1

        # Connection should be closed after context manager exits
        # Note: We can't easily test this without accessing private attributes

    def test_close_method(self, tmp_path):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        with patch("jqsys.storage.query.logger") as mock_logger:
            query_engine.close()
            mock_logger.info.assert_called_with("Query engine connection closed")

    def test_close_with_none_connection(self, tmp_path):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        # Simulate connection being None
        query_engine.conn = None

        # Should not raise an exception
        query_engine.close()

    def test_pathlib_path_initialization(self, tmp_path):
        bronze_path = Path(tmp_path) / "bronze"
        silver_path = Path(tmp_path) / "silver"
        gold_path = Path(tmp_path) / "gold"

        query_engine = QueryEngine(
            db_path=None, bronze_path=bronze_path, silver_path=silver_path, gold_path=gold_path
        )

        assert query_engine.bronze_path == bronze_path
        assert query_engine.silver_path == silver_path
        assert query_engine.gold_path == gold_path

        query_engine.close()

    def test_string_path_initialization(self, tmp_path):
        bronze_str = str(tmp_path / "bronze")
        silver_str = str(tmp_path / "silver")

        query_engine = QueryEngine(db_path=None, bronze_path=bronze_str, silver_path=silver_str)

        assert query_engine.bronze_path == Path(bronze_str)
        assert query_engine.silver_path == Path(silver_str)

        query_engine.close()

    def test_sql_injection_prevention(self, tmp_path, sample_daily_prices_data):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        query_engine.conn.register("temp_df", sample_daily_prices_data.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        # Test that code filtering uses proper SQL quoting
        # This should not cause SQL injection
        malicious_codes = ["1301'; DROP TABLE daily_prices; --"]

        # The method should handle this gracefully (no results, but no crash)
        result = query_engine.get_daily_prices(codes=malicious_codes)

        # Should return empty result since the malicious code doesn't exist
        assert len(result) == 0

        # Verify table still exists (not dropped)
        test_result = query_engine.execute_sql("SELECT COUNT(*) as count FROM daily_prices")
        assert test_result["count"][0] == 4

        query_engine.close()

    def test_large_dataset_handling(self, tmp_path):
        query_engine = QueryEngine(
            db_path=None, bronze_path=tmp_path / "bronze", silver_path=tmp_path / "silver"
        )

        # Create a larger dataset for testing
        large_data = []
        for i in range(10000):
            large_data.append(
                {
                    "code": f"130{i % 10}",
                    "date": date(2024, 1, 15),
                    "close": 100.0 + i,
                    "volume": 1000 + i,
                    "open": 99.0 + i,
                    "high": 105.0 + i,
                    "low": 98.0 + i,
                }
            )

        large_df = pl.DataFrame(large_data)
        query_engine.conn.register("temp_df", large_df.to_pandas())
        query_engine.conn.execute("CREATE TABLE daily_prices AS SELECT * FROM temp_df")

        # Test that large queries work
        result = query_engine.get_daily_prices(limit=1000)
        assert len(result) == 1000

        # Test summary stats on large dataset
        stats = query_engine.get_price_summary_stats()
        assert len(stats) == 10  # 10 unique codes

        query_engine.close()
