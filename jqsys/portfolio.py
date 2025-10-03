"""Portfolio and Market analysis classes.

Provides high-level APIs for analyzing multiple stocks and market-wide data
using the existing storage infrastructure.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import numpy as np
import polars as pl

from .stock import InsufficientDataError, Stock
from .storage.query import QueryEngine

logger = logging.getLogger(__name__)


class Portfolio:
    """Collection of stocks for comparative analysis.

    This class provides portfolio-level analytics and comparative analysis
    across multiple stocks.

    Examples:
        >>> portfolio = Portfolio(["7203", "6758", "9984"])
        >>> returns = portfolio.calculate_returns()
        >>> correlation_matrix = portfolio.correlation_matrix()
        >>> performance = portfolio.performance_summary()
    """

    def __init__(self, stocks: list[str | Stock], weights: list[float] | None = None):
        """Initialize Portfolio.

        Args:
            stocks: List of stock codes or Stock instances
            weights: Optional portfolio weights (default: equal weights)
        """
        # Convert stock codes to Stock instances
        self.stocks = []
        for stock in stocks:
            if isinstance(stock, str):
                self.stocks.append(Stock(stock))
            else:
                self.stocks.append(stock)

        # Set weights
        if weights is None:
            self.weights = [1.0 / len(self.stocks)] * len(self.stocks)
        else:
            if len(weights) != len(self.stocks):
                raise ValueError("Number of weights must match number of stocks")
            if abs(sum(weights) - 1.0) > 1e-6:
                raise ValueError("Weights must sum to 1.0")
            self.weights = weights

        self.codes = [stock.code for stock in self.stocks]

    def calculate_returns(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        periods: list[int] = None,
    ) -> pl.DataFrame:
        """Calculate portfolio returns.

        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            periods: List of periods in days

        Returns:
            DataFrame with portfolio returns
        """
        # Get returns for all stocks
        if periods is None:
            periods = [1, 5, 21]
        all_returns = []

        for stock in self.stocks:
            stock_returns = stock.returns(start_date, end_date, periods)
            if not stock_returns.is_empty():
                all_returns.append(stock_returns)

        if not all_returns:
            raise InsufficientDataError("No return data available for any stocks in portfolio")

        # Merge all returns on date
        merged_returns = all_returns[0]
        for i, returns_df in enumerate(all_returns[1:], 1):
            merged_returns = merged_returns.join(
                returns_df.select(
                    ["date"] + [col for col in returns_df.columns if col.startswith("return_")]
                ),
                on="date",
                suffix=f"_{self.codes[i]}",
            )

        # Calculate weighted portfolio returns
        portfolio_returns = merged_returns.select("date")

        for period in periods:
            return_cols = [
                col
                for col in merged_returns.columns
                if col == f"return_{period}d" or col.endswith(f"_{period}d")
            ]

            if return_cols:
                # Calculate weighted average return
                weighted_return_expr = pl.lit(0.0)

                for i, col in enumerate(return_cols):
                    if i < len(self.weights):
                        weighted_return_expr = weighted_return_expr + (
                            pl.col(col).fill_null(0) * self.weights[i]
                        )

                portfolio_returns = portfolio_returns.with_columns(
                    [weighted_return_expr.alias(f"portfolio_return_{period}d")]
                )

        return portfolio_returns.sort("date")

    def correlation_matrix(
        self, start_date: str | datetime | None = None, end_date: str | datetime | None = None
    ) -> pl.DataFrame:
        """Calculate correlation matrix between stocks.

        Args:
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            DataFrame with correlation matrix
        """
        # Get returns for all stocks
        all_returns = []

        for stock in self.stocks:
            stock_returns = stock.returns(start_date, end_date, periods=[1])
            if not stock_returns.is_empty():
                stock_returns = stock_returns.select(["date", "return_1d"]).rename(
                    {"return_1d": stock.code}
                )
                all_returns.append(stock_returns)

        if len(all_returns) < 2:
            raise InsufficientDataError("Need at least 2 stocks with data for correlation matrix")

        # Merge all returns
        merged_returns = all_returns[0]
        for returns_df in all_returns[1:]:
            merged_returns = merged_returns.join(returns_df, on="date", how="inner")

        # Calculate correlation matrix
        stock_codes = [col for col in merged_returns.columns if col != "date"]
        correlations = []

        for code1 in stock_codes:
            row = {"stock": code1}
            for code2 in stock_codes:
                if code1 == code2:
                    row[code2] = 1.0
                else:
                    corr = merged_returns.select(pl.corr(code1, code2).alias("correlation"))[
                        "correlation"
                    ][0]
                    row[code2] = corr if corr is not None else 0.0
            correlations.append(row)

        return pl.DataFrame(correlations)

    def performance_summary(
        self, start_date: str | datetime | None = None, end_date: str | datetime | None = None
    ) -> pl.DataFrame:
        """Get performance summary for all stocks in portfolio.

        Args:
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            DataFrame with performance metrics for each stock
        """
        performance_data = []

        for i, stock in enumerate(self.stocks):
            try:
                perf = stock.performance_stats(start_date, end_date)
                perf["code"] = stock.code
                perf["weight"] = self.weights[i]
                perf["current_price"] = stock.current_price
                performance_data.append(perf)
            except Exception as e:
                logger.warning(f"Failed to get performance for {stock.code}: {e}")

        if not performance_data:
            raise InsufficientDataError("No performance data available for any stocks")

        return pl.DataFrame(performance_data).select(
            [
                "code",
                "weight",
                "current_price",
                "total_return",
                "annualized_return",
                "volatility",
                "sharpe_ratio",
                "max_drawdown",
                "win_rate",
                "trading_days",
            ]
        )

    def risk_metrics(
        self, start_date: str | datetime | None = None, end_date: str | datetime | None = None
    ) -> dict[str, float]:
        """Calculate portfolio risk metrics.

        Args:
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            Dictionary with portfolio risk metrics
        """
        portfolio_returns = self.calculate_returns(start_date, end_date, periods=[1])

        if portfolio_returns.is_empty():
            raise InsufficientDataError("No portfolio return data available")

        returns_series = portfolio_returns.filter(pl.col("portfolio_return_1d").is_not_null())[
            "portfolio_return_1d"
        ]

        if len(returns_series) < 2:
            raise InsufficientDataError("Insufficient data for risk calculation")

        # Portfolio volatility
        portfolio_vol = returns_series.std() * np.sqrt(252)

        # Value at Risk (95% confidence)
        var_95 = returns_series.quantile(0.05)

        # Expected Shortfall (Conditional VaR)
        es_95 = returns_series.filter(returns_series <= var_95).mean()

        # Maximum drawdown
        cumulative_returns = (1 + returns_series).cumprod()
        running_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()

        return {
            "portfolio_volatility": portfolio_vol,
            "value_at_risk_95": var_95,
            "expected_shortfall_95": es_95,
            "max_drawdown": max_drawdown,
            "tracking_error": portfolio_vol,  # Simplified - would need benchmark for true tracking error
        }

    def __repr__(self) -> str:
        """String representation of the Portfolio."""
        return f"Portfolio({self.codes})"

    def __str__(self) -> str:
        """Human-readable string representation."""
        return self.__repr__()


class Market:
    """Market-wide analysis and screening capabilities.

    This class provides market-level analytics and stock screening functionality.

    Examples:
        >>> market = Market()
        >>> top_performers = market.top_performers(period="1d", limit=10)
        >>> market_stats = market.market_statistics()
        >>> volatile_stocks = market.screen_by_volatility(min_vol=0.3)
    """

    def __init__(self):
        """Initialize Market analyzer."""
        pass

    def top_performers(
        self,
        period: str = "1d",
        limit: int = 10,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
    ) -> pl.DataFrame:
        """Get top performing stocks.

        Args:
            period: Period for performance calculation ("1d", "5d", "21d")
            limit: Number of top performers to return
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            DataFrame with top performing stocks
        """
        period_days = {"1d": 1, "5d": 5, "21d": 21}.get(period, 1)

        with QueryEngine() as query:
            # Get latest returns for all stocks
            query_sql = f"""
            WITH latest_returns AS (
                SELECT
                    code,
                    date,
                    close,
                    LAG(close, {period_days}) OVER (PARTITION BY code ORDER BY date) as prev_close,
                    (close - LAG(close, {period_days}) OVER (PARTITION BY code ORDER BY date)) /
                    LAG(close, {period_days}) OVER (PARTITION BY code ORDER BY date) as return_{period}
                FROM daily_prices
                WHERE 1=1
            """

            params = []
            if start_date:
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")
                query_sql += " AND date >= ?"
                params.append(start_date.strftime("%Y-%m-%d"))

            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, "%Y-%m-%d")
                query_sql += " AND date <= ?"
                params.append(end_date.strftime("%Y-%m-%d"))

            query_sql += f"""
            ),
            ranked_returns AS (
                SELECT
                    code,
                    date,
                    close,
                    return_{period},
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
                FROM latest_returns
                WHERE return_{period} IS NOT NULL
            )
            SELECT
                code,
                date,
                close,
                return_{period}
            FROM ranked_returns
            WHERE rn = 1
            ORDER BY return_{period} DESC
            LIMIT ?
            """

            params.append(limit)

            return query.execute_sql_with_params(query_sql, params)

    def market_statistics(self, date: str | datetime | None = None) -> dict[str, Any]:
        """Get market-wide statistics.

        Args:
            date: Specific date for analysis (default: latest available)

        Returns:
            Dictionary with market statistics
        """
        with QueryEngine() as query:
            if date is None:
                # Get latest date
                latest_date_result = query.execute_sql(
                    "SELECT MAX(date) as latest_date FROM daily_prices"
                )
                if len(latest_date_result) == 0:
                    raise InsufficientDataError("No market data available")
                date = latest_date_result["latest_date"][0]

            if isinstance(date, str):
                date = datetime.strptime(date, "%Y-%m-%d")

            # Market statistics for the date
            market_stats = query.execute_sql(
                """
                SELECT
                    COUNT(DISTINCT code) as total_stocks,
                    AVG(close) as avg_price,
                    MEDIAN(close) as median_price,
                    MIN(close) as min_price,
                    MAX(close) as max_price,
                    SUM(volume) as total_volume,
                    AVG(volume) as avg_volume,
                    SUM(turnover_value) as total_turnover
                FROM daily_prices
                WHERE date = ?
                """,
                [date.strftime("%Y-%m-%d")],
            )

            if len(market_stats) == 0:
                raise InsufficientDataError(
                    f"No market data available for {date.strftime('%Y-%m-%d')}"
                )

            stats = market_stats.row(0, named=True)
            stats["date"] = date.strftime("%Y-%m-%d")

            return stats

    def screen_by_volatility(
        self,
        min_vol: float | None = None,
        max_vol: float | None = None,
        period_days: int = 21,
        limit: int = 50,
    ) -> pl.DataFrame:
        """Screen stocks by volatility.

        Args:
            min_vol: Minimum annualized volatility
            max_vol: Maximum annualized volatility
            period_days: Period for volatility calculation
            limit: Maximum number of results

        Returns:
            DataFrame with stocks matching volatility criteria
        """
        with QueryEngine() as query:
            query_sql = """
            WITH stock_volatility AS (
                SELECT
                    code,
                    COUNT(*) as trading_days,
                    AVG(close) as avg_price,
                    STDDEV((close - LAG(close, 1) OVER (PARTITION BY code ORDER BY date)) /
                           LAG(close, 1) OVER (PARTITION BY code ORDER BY date)) * SQRT(252) as annualized_volatility
                FROM daily_prices
                GROUP BY code
                HAVING COUNT(*) >= ?
            )
            SELECT
                code,
                trading_days,
                avg_price,
                annualized_volatility
            FROM stock_volatility
            WHERE annualized_volatility IS NOT NULL
            """

            params = [period_days]

            if min_vol is not None:
                query_sql += " AND annualized_volatility >= ?"
                params.append(min_vol)

            if max_vol is not None:
                query_sql += " AND annualized_volatility <= ?"
                params.append(max_vol)

            query_sql += " ORDER BY annualized_volatility DESC LIMIT ?"
            params.append(limit)

            return query.execute_sql_with_params(query_sql, params)

    def screen_by_volume(
        self, min_volume: int | None = None, min_turnover: float | None = None, limit: int = 50
    ) -> pl.DataFrame:
        """Screen stocks by trading volume.

        Args:
            min_volume: Minimum average daily volume
            min_turnover: Minimum average daily turnover value
            limit: Maximum number of results

        Returns:
            DataFrame with stocks matching volume criteria
        """
        with QueryEngine() as query:
            query_sql = """
            SELECT
                code,
                COUNT(*) as trading_days,
                AVG(volume) as avg_volume,
                AVG(turnover_value) as avg_turnover,
                AVG(close) as avg_price
            FROM daily_prices
            GROUP BY code
            HAVING COUNT(*) >= 5
            """

            params = []

            if min_volume is not None:
                query_sql += " AND AVG(volume) >= ?"
                params.append(min_volume)

            if min_turnover is not None:
                query_sql += " AND AVG(turnover_value) >= ?"
                params.append(min_turnover)

            query_sql += " ORDER BY avg_volume DESC LIMIT ?"
            params.append(limit)

            return query.execute_sql_with_params(query_sql, params)

    def get_sector_performance(self, period: str = "1d") -> pl.DataFrame:
        """Get sector performance (simplified - based on stock code patterns).

        Args:
            period: Period for performance calculation

        Returns:
            DataFrame with sector performance
        """
        # This is a simplified implementation
        # In a real system, you'd have sector mapping data
        period_days = {"1d": 1, "5d": 5, "21d": 21}.get(period, 1)

        with QueryEngine() as query:
            query_sql = f"""
            WITH sector_mapping AS (
                SELECT
                    code,
                    CASE
                        WHEN code LIKE '1%' THEN 'Foods'
                        WHEN code LIKE '2%' THEN 'Textiles'
                        WHEN code LIKE '3%' THEN 'Pulp & Paper'
                        WHEN code LIKE '4%' THEN 'Chemicals'
                        WHEN code LIKE '5%' THEN 'Pharmaceuticals'
                        WHEN code LIKE '6%' THEN 'Oil & Coal'
                        WHEN code LIKE '7%' THEN 'Rubber'
                        WHEN code LIKE '8%' THEN 'Glass & Ceramics'
                        WHEN code LIKE '9%' THEN 'Iron & Steel'
                        ELSE 'Other'
                    END as sector
                FROM daily_prices
            ),
            sector_returns AS (
                SELECT
                    sm.sector,
                    dp.code,
                    dp.date,
                    dp.close,
                    LAG(dp.close, {period_days}) OVER (PARTITION BY dp.code ORDER BY dp.date) as prev_close
                FROM daily_prices dp
                JOIN sector_mapping sm ON dp.code = sm.code
            ),
            latest_sector_returns AS (
                SELECT
                    sector,
                    code,
                    date,
                    (close - prev_close) / prev_close as stock_return,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) as rn
                FROM sector_returns
                WHERE prev_close IS NOT NULL
            )
            SELECT
                sector,
                COUNT(DISTINCT code) as stock_count,
                AVG(stock_return) as avg_return,
                MEDIAN(stock_return) as median_return,
                STDDEV(stock_return) as return_std
            FROM latest_sector_returns
            WHERE rn = 1
            GROUP BY sector
            ORDER BY avg_return DESC
            """

            return query.execute_sql(query_sql)

    def __repr__(self) -> str:
        """String representation of the Market."""
        return "Market()"

    def __str__(self) -> str:
        """Human-readable string representation."""
        return self.__repr__()
