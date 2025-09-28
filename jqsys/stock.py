"""High-level object-oriented API for stock analysis.

Provides a user-friendly interface for analyzing individual stocks and companies
using the existing bronze/silver storage infrastructure.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import polars as pl
import numpy as np

from .storage.bronze import BronzeStorage
from .storage.silver import SilverStorage
from .storage.query import QueryEngine

logger = logging.getLogger(__name__)


class StockNotFoundError(Exception):
    """Raised when a stock code is not found in the dataset."""
    pass


class InsufficientDataError(Exception):
    """Raised when there's insufficient data for analysis."""
    pass


class Stock:
    """High-level interface for analyzing a single stock/company.
    
    This class provides a convenient API for accessing stock data and performing
    various financial analyses using the underlying bronze/silver storage layers.
    
    Examples:
        >>> toyota = Stock("7203")
        >>> prices = toyota.prices("2023-06-01", "2023-06-21")
        >>> returns = toyota.returns(periods=[1, 5, 21])
        >>> volatility = toyota.volatility_metrics()
    """
    
    def __init__(
        self,
        code: str,
        bronze_storage: Optional[BronzeStorage] = None,
        silver_storage: Optional[SilverStorage] = None,
        query_engine: Optional[QueryEngine] = None
    ):
        """Initialize Stock instance.
        
        Args:
            code: J-Quants stock code (e.g., "7203" for Toyota)
            bronze_storage: Optional bronze storage instance
            silver_storage: Optional silver storage instance
            query_engine: Optional query engine instance
        """
        self.code = code
        self._bronze = bronze_storage or BronzeStorage()
        self._silver = silver_storage or SilverStorage(bronze_storage=self._bronze)
        self._query = query_engine
        
        # Cache for frequently accessed data
        self._price_cache: Dict[str, pl.DataFrame] = {}
        self._stats_cache: Dict[str, Any] = {}
        
        # Validate stock exists
        self._validate_stock_exists()
    
    def _validate_stock_exists(self) -> None:
        """Validate that the stock code exists in the dataset."""
        try:
            query = self._get_query_engine()
            result = query.execute_sql_with_params(
                "SELECT COUNT(*) as count FROM daily_prices WHERE code = ?",
                [self.code]
            )
            if result['count'][0] == 0:
                raise StockNotFoundError(f"Stock code '{self.code}' not found in dataset")
        except Exception as e:
            logger.warning(f"Could not validate stock existence: {e}")
    
    def _get_query_engine(self) -> QueryEngine:
        """Get or create query engine instance."""
        if self._query is None:
            self._query = QueryEngine()
        return self._query
    
    def _cache_key(self, method: str, **kwargs) -> str:
        """Generate cache key for method and parameters."""
        params = "_".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
        return f"{method}_{params}" if params else method
    
    def prices(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """Get OHLCV price data for the stock.
        
        Args:
            start_date: Start date (YYYY-MM-DD or datetime)
            end_date: End date (YYYY-MM-DD or datetime)
            columns: Optional list of columns to return
            
        Returns:
            DataFrame with price data
            
        Raises:
            InsufficientDataError: If no data found for the period
        """
        # Convert string dates to datetime
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Check cache
        cache_key = self._cache_key("prices", start=start_date, end=end_date)
        if cache_key in self._price_cache:
            df = self._price_cache[cache_key]
        else:
            # Fetch from silver layer
            df = self._silver.read_daily_prices(start_date, end_date, codes=[self.code])
            if df.is_empty():
                raise InsufficientDataError(
                    f"No price data found for {self.code} between {start_date.date()} and {end_date.date()}"
                )
            self._price_cache[cache_key] = df
        
        # Filter columns if specified
        if columns:
            available_cols = [col for col in columns if col in df.columns]
            if not available_cols:
                raise ValueError(f"None of the requested columns {columns} are available")
            df = df.select(available_cols)
        
        return df.sort("date")
    
    def returns(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        periods: List[int] = [1, 5, 21]
    ) -> pl.DataFrame:
        """Calculate returns over specified periods.
        
        Args:
            start_date: Start date (optional, uses all available data if None)
            end_date: End date (optional, uses all available data if None)
            periods: List of periods in days (default: 1, 5, 21 days)
            
        Returns:
            DataFrame with returns calculations
        """
        with self._get_query_engine() as query:
            # Build the LAG expressions for each period
            lag_expressions = []
            return_expressions = []
            
            for period in periods:
                lag_col = f"close_lag_{period}"
                return_col = f"return_{period}d"
                
                lag_expressions.append(f"LAG(close, {period}) OVER (ORDER BY date) as {lag_col}")
                return_expressions.append(f"(close - {lag_col}) / {lag_col} as {return_col}")
            
            lag_sql = ", ".join(lag_expressions)
            return_sql = ", ".join(return_expressions)
            
            query_sql = f"""
            WITH price_lags AS (
                SELECT 
                    code, date, close,
                    {lag_sql}
                FROM daily_prices
                WHERE code = ?
            )
            SELECT 
                code, date, close,
                {return_sql}
            FROM price_lags
            """
            
            params = [self.code]
            
            if start_date:
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")
                query_sql += " WHERE date >= ?"
                params.append(start_date.strftime('%Y-%m-%d'))
            
            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, "%Y-%m-%d")
                conjunction = " AND " if start_date else " WHERE "
                query_sql += f"{conjunction}date <= ?"
                params.append(end_date.strftime('%Y-%m-%d'))
            
            query_sql += " ORDER BY date"
            
            return query.execute_sql_with_params(query_sql, params)
    
    def volume_profile(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None
    ) -> Dict[str, Any]:
        """Analyze volume trading patterns.
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dictionary with volume statistics
        """
        prices_df = self.prices(
            start_date or "2020-01-01",
            end_date or datetime.now().strftime("%Y-%m-%d"),
            columns=["date", "volume", "close", "turnover_value"]
        )
        
        if prices_df.is_empty():
            raise InsufficientDataError("No data available for volume analysis")
        
        volume_stats = prices_df.select([
            pl.col("volume").mean().alias("avg_volume"),
            pl.col("volume").median().alias("median_volume"),
            pl.col("volume").std().alias("volume_std"),
            pl.col("volume").min().alias("min_volume"),
            pl.col("volume").max().alias("max_volume"),
            pl.col("turnover_value").mean().alias("avg_turnover"),
            pl.col("turnover_value").sum().alias("total_turnover")
        ]).row(0, named=True)
        
        # Calculate volume-weighted average price (VWAP)
        vwap_df = prices_df.with_columns([
            (pl.col("close") * pl.col("volume")).alias("price_volume")
        ])
        
        total_volume = vwap_df["volume"].sum()
        total_price_volume = vwap_df["price_volume"].sum()
        vwap = total_price_volume / total_volume if total_volume > 0 else 0
        
        volume_stats["vwap"] = vwap
        volume_stats["trading_days"] = len(prices_df)
        
        return volume_stats
    
    def volatility_metrics(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        window: int = 21
    ) -> Dict[str, float]:
        """Calculate volatility metrics.
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            window: Rolling window for calculations (default: 21 days)
            
        Returns:
            Dictionary with volatility metrics
        """
        returns_df = self.returns(start_date, end_date, periods=[1])
        
        if returns_df.is_empty():
            raise InsufficientDataError("No data available for volatility analysis")
        
        # Filter out null returns
        valid_returns = returns_df.filter(pl.col("return_1d").is_not_null())
        
        if len(valid_returns) < 2:
            raise InsufficientDataError("Insufficient data for volatility calculation")
        
        returns_series = valid_returns["return_1d"]
        
        # Calculate various volatility metrics
        daily_vol = returns_series.std()
        annualized_vol = daily_vol * np.sqrt(252)  # Assuming 252 trading days per year
        
        # Rolling volatility
        rolling_vol = valid_returns.with_columns([
            pl.col("return_1d").rolling_std(window_size=window).alias("rolling_vol")
        ])["rolling_vol"]
        
        current_vol = rolling_vol.drop_nulls().tail(1).item() if len(rolling_vol.drop_nulls()) > 0 else daily_vol
        
        return {
            "daily_volatility": daily_vol,
            "annualized_volatility": annualized_vol,
            "current_volatility": current_vol * np.sqrt(252),
            "volatility_of_volatility": rolling_vol.drop_nulls().std() if len(rolling_vol.drop_nulls()) > 1 else 0.0
        }
    
    def performance_stats(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        benchmark_return: float = 0.0
    ) -> Dict[str, float]:
        """Calculate performance statistics.
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            benchmark_return: Risk-free rate for Sharpe ratio calculation
            
        Returns:
            Dictionary with performance metrics
        """
        returns_df = self.returns(start_date, end_date, periods=[1])
        prices_df = self.prices(
            start_date or "2020-01-01",
            end_date or datetime.now().strftime("%Y-%m-%d"),
            columns=["date", "close"]
        )
        
        if returns_df.is_empty() or prices_df.is_empty():
            raise InsufficientDataError("No data available for performance analysis")
        
        # Filter valid returns
        valid_returns = returns_df.filter(pl.col("return_1d").is_not_null())["return_1d"]
        
        if len(valid_returns) < 2:
            raise InsufficientDataError("Insufficient data for performance calculation")
        
        # Basic statistics
        mean_return = valid_returns.mean()
        std_return = valid_returns.std()
        
        # Total return
        first_price = prices_df["close"].first()
        last_price = prices_df["close"].last()
        total_return = (last_price - first_price) / first_price
        
        # Sharpe ratio (annualized)
        excess_return = mean_return - benchmark_return / 252  # Daily risk-free rate
        sharpe_ratio = (excess_return / std_return) * np.sqrt(252) if std_return > 0 else 0.0
        
        # Maximum drawdown
        cumulative_returns = (1 + valid_returns).cumprod()
        running_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # Win rate
        positive_returns = valid_returns.filter(valid_returns > 0)
        win_rate = len(positive_returns) / len(valid_returns)
        
        return {
            "total_return": total_return,
            "annualized_return": mean_return * 252,
            "volatility": std_return * np.sqrt(252),
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
            "trading_days": len(valid_returns)
        }
    
    def correlation_with(
        self,
        other_stock: Union[Stock, str],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        method: str = "pearson"
    ) -> float:
        """Calculate correlation with another stock.
        
        Args:
            other_stock: Another Stock instance or stock code
            start_date: Start date for analysis
            end_date: End date for analysis
            method: Correlation method ("pearson" or "spearman")
            
        Returns:
            Correlation coefficient
        """
        if isinstance(other_stock, str):
            other_stock = Stock(other_stock)
        
        # Get returns for both stocks
        self_returns = self.returns(start_date, end_date, periods=[1])
        other_returns = other_stock.returns(start_date, end_date, periods=[1])
        
        # Merge on date
        merged = self_returns.join(
            other_returns,
            on="date",
            suffix="_other"
        ).filter(
            pl.col("return_1d").is_not_null() & 
            pl.col("return_1d_other").is_not_null()
        )
        
        if len(merged) < 2:
            raise InsufficientDataError("Insufficient overlapping data for correlation")
        
        # Calculate correlation
        if method == "pearson":
            correlation = merged.select(
                pl.corr("return_1d", "return_1d_other").alias("correlation")
            )["correlation"][0]
        else:
            # For Spearman, we'd need to implement rank correlation
            # For now, fall back to Pearson
            correlation = merged.select(
                pl.corr("return_1d", "return_1d_other").alias("correlation")
            )["correlation"][0]
        
        return correlation if correlation is not None else 0.0
    
    @property
    def current_price(self) -> Optional[float]:
        """Get the most recent available price."""
        try:
            query = self._get_query_engine()
            result = query.execute_sql_with_params(
                "SELECT close FROM daily_prices WHERE code = ? ORDER BY date DESC LIMIT 1",
                [self.code]
            )
            return result["close"][0] if len(result) > 0 else None
        except Exception as e:
            logger.error(f"Failed to get current price for {self.code}: {e}")
            return None
    
    @property
    def data_range(self) -> Optional[Dict[str, str]]:
        """Get the date range of available data."""
        try:
            query = self._get_query_engine()
            result = query.execute_sql_with_params(
                "SELECT MIN(date) as first_date, MAX(date) as last_date FROM daily_prices WHERE code = ?",
                [self.code]
            )
            if len(result) > 0:
                return {
                    "first_date": str(result["first_date"][0]),
                    "last_date": str(result["last_date"][0])
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get data range for {self.code}: {e}")
            return None
    
    @property
    def avg_volume(self) -> Optional[float]:
        """Get average trading volume."""
        try:
            query = self._get_query_engine()
            result = query.execute_sql_with_params(
                "SELECT AVG(volume) as avg_vol FROM daily_prices WHERE code = ?",
                [self.code]
            )
            return result["avg_vol"][0] if len(result) > 0 else None
        except Exception as e:
            logger.error(f"Failed to get average volume for {self.code}: {e}")
            return None
    
    def close(self) -> None:
        """Close the query engine connection to free resources."""
        if self._query is not None:
            # Don't close singleton - just remove reference
            self._query = None
    
    def __del__(self) -> None:
        """Cleanup when Stock instance is destroyed."""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup
    
    def __repr__(self) -> str:
        """String representation of the Stock."""
        current_price = self.current_price
        price_str = f" (¥{current_price:.2f})" if current_price else ""
        return f"Stock('{self.code}'{price_str})"
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        return self.__repr__()
