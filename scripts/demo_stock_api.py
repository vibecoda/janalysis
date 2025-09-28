#!/usr/bin/env python
"""Demonstration of the high-level Stock API.

This script showcases the object-oriented Stock, Portfolio, and Market APIs
that provide a user-friendly interface for stock analysis using the existing
bronze/silver storage infrastructure.
"""

from __future__ import annotations

import argparse
import logging

from jqsys.stock import Stock, StockNotFoundError, InsufficientDataError
from jqsys.portfolio import Portfolio, Market
from jqsys.utils.env import load_env_file_if_present

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_stock_api():
    """Demonstrate Stock class capabilities."""
    print("\n" + "="*60)
    print("📈 STOCK API DEMONSTRATION")
    print("="*60)
    
    try:
        # Example with a stock that should exist in our dataset
        print("\n🏢 Creating Stock instance:")
        toyota = Stock("13010")  # Using a code from our demo data
        print(f"Stock: {toyota}")
        print(f"Current price: ¥{toyota.current_price}")
        print(f"Data range: {toyota.data_range}")
        print(f"Average volume: {toyota.avg_volume:,.0f}")
        
        # Get price data
        print("\n📊 Price Data:")
        prices = toyota.prices("2023-06-01", "2023-06-21")
        print(f"Retrieved {len(prices)} price records")
        print("Sample prices:")
        print(prices.select(["date", "open", "high", "low", "close", "volume"]).head(5))
        
        # Calculate returns
        print("\n📈 Returns Analysis:")
        returns = toyota.returns("2023-06-01", "2023-06-21", periods=[1, 5])
        valid_returns = returns.filter(returns["return_1d"].is_not_null())
        print(f"Calculated returns for {len(valid_returns)} trading days")
        print("Sample returns:")
        print(valid_returns.select(["date", "close", "return_1d", "return_5d"]).head(5))
        
        # Volume profile
        print("\n📊 Volume Profile:")
        volume_profile = toyota.volume_profile("2023-06-01", "2023-06-21")
        print(f"Average volume: {volume_profile['avg_volume']:,.0f}")
        print(f"VWAP: ¥{volume_profile['vwap']:.2f}")
        print(f"Trading days: {volume_profile['trading_days']}")
        
        # Volatility metrics
        print("\n📉 Volatility Metrics:")
        volatility = toyota.volatility_metrics("2023-06-01", "2023-06-21")
        print(f"Daily volatility: {volatility['daily_volatility']:.4f}")
        print(f"Annualized volatility: {volatility['annualized_volatility']:.2%}")
        
        # Performance statistics
        print("\n🎯 Performance Statistics:")
        performance = toyota.performance_stats("2023-06-01", "2023-06-21")
        print(f"Total return: {performance['total_return']:.2%}")
        print(f"Annualized return: {performance['annualized_return']:.2%}")
        print(f"Sharpe ratio: {performance['sharpe_ratio']:.3f}")
        print(f"Max drawdown: {performance['max_drawdown']:.2%}")
        print(f"Win rate: {performance['win_rate']:.2%}")
        
    except StockNotFoundError as e:
        print(f"Stock not found: {e}")
    except InsufficientDataError as e:
        print(f"Insufficient data: {e}")
    except Exception as e:
        print(f"Error in stock demo: {e}")


def demo_portfolio_api():
    """Demonstrate Portfolio class capabilities."""
    print("\n" + "="*60)
    print("📊 PORTFOLIO API DEMONSTRATION")
    print("="*60)
    
    try:
        # Create portfolio with multiple stocks from our dataset
        print("\n🏢 Creating Portfolio:")
        stock_codes = ["13010", "13050", "13060"]  # Using codes from our demo data
        portfolio = Portfolio(stock_codes)
        print(f"Portfolio: {portfolio}")
        print(f"Weights: {portfolio.weights}")
        
        # Portfolio returns
        print("\n📈 Portfolio Returns:")
        portfolio_returns = portfolio.calculate_returns("2023-06-01", "2023-06-21", periods=[1, 5])
        valid_returns = portfolio_returns.filter(
            portfolio_returns["portfolio_return_1d"].is_not_null()
        )
        print(f"Calculated portfolio returns for {len(valid_returns)} trading days")
        print("Sample portfolio returns:")
        print(valid_returns.select(["date", "portfolio_return_1d", "portfolio_return_5d"]).head(5))
        
        # Correlation matrix
        print("\n🔗 Correlation Matrix:")
        correlation_matrix = portfolio.correlation_matrix("2023-06-01", "2023-06-21")
        print("Stock correlations:")
        print(correlation_matrix)
        
        # Performance summary
        print("\n🎯 Performance Summary:")
        performance_summary = portfolio.performance_summary("2023-06-01", "2023-06-21")
        print("Individual stock performance:")
        print(performance_summary.select([
            "code", "weight", "total_return", "annualized_return", "volatility", "sharpe_ratio"
        ]))
        
        # Risk metrics
        print("\n⚠️ Portfolio Risk Metrics:")
        risk_metrics = portfolio.risk_metrics("2023-06-01", "2023-06-21")
        print(f"Portfolio volatility: {risk_metrics['portfolio_volatility']:.2%}")
        print(f"Value at Risk (95%): {risk_metrics['value_at_risk_95']:.2%}")
        print(f"Expected Shortfall (95%): {risk_metrics['expected_shortfall_95']:.2%}")
        print(f"Max drawdown: {risk_metrics['max_drawdown']:.2%}")
        
    except Exception as e:
        print(f"Error in portfolio demo: {e}")


def demo_market_api():
    """Demonstrate Market class capabilities."""
    print("\n" + "="*60)
    print("🌍 MARKET API DEMONSTRATION")
    print("="*60)
    
    try:
        market = Market()
        print(f"Market analyzer: {market}")
        
        # Market statistics
        print("\n📊 Market Statistics:")
        market_stats = market.market_statistics("2023-06-21")  # Latest date in our data
        print(f"Date: {market_stats['date']}")
        print(f"Total stocks: {market_stats['total_stocks']:,}")
        print(f"Average price: ¥{market_stats['avg_price']:.2f}")
        print(f"Median price: ¥{market_stats['median_price']:.2f}")
        print(f"Price range: ¥{market_stats['min_price']:.2f} - ¥{market_stats['max_price']:,.2f}")
        print(f"Total volume: {market_stats['total_volume']:,.0f}")
        print(f"Total turnover: ¥{market_stats['total_turnover']:,.0f}")
        
        # Top performers
        print("\n🚀 Top Performers (1-day):")
        top_performers = market.top_performers(period="1d", limit=10)
        if len(top_performers) > 0:
            print("Best performing stocks:")
            print(top_performers.select(["code", "date", "close", "return_1d"]))
        else:
            print("No performance data available")
        
        # Volatility screening
        print("\n📈 High Volatility Stocks:")
        volatile_stocks = market.screen_by_volatility(min_vol=0.2, limit=10)
        if len(volatile_stocks) > 0:
            print("Most volatile stocks (>20% annualized volatility):")
            print(volatile_stocks.select(["code", "avg_price", "annualized_volatility", "trading_days"]))
        else:
            print("No high volatility stocks found")
        
        # Volume screening
        print("\n📊 High Volume Stocks:")
        high_volume_stocks = market.screen_by_volume(min_volume=1000000, limit=10)
        if len(high_volume_stocks) > 0:
            print("Most actively traded stocks (>1M avg volume):")
            print(high_volume_stocks.select(["code", "avg_volume", "avg_turnover", "avg_price"]))
        else:
            print("No high volume stocks found")
        
        # Sector performance
        print("\n🏭 Sector Performance:")
        sector_performance = market.get_sector_performance(period="1d")
        if len(sector_performance) > 0:
            print("Sector returns (1-day):")
            print(sector_performance.select(["sector", "stock_count", "avg_return", "return_std"]))
        else:
            print("No sector performance data available")
        
    except Exception as e:
        print(f"Error in market demo: {e}")


def demo_stock_comparison():
    """Demonstrate stock comparison capabilities."""
    print("\n" + "="*60)
    print("🔄 STOCK COMPARISON DEMONSTRATION")
    print("="*60)
    
    try:
        # Compare two stocks
        print("\n🏢 Comparing Two Stocks:")
        stock1 = Stock("13010")
        stock2 = Stock("13050")
        
        print(f"Stock 1: {stock1}")
        print(f"Stock 2: {stock2}")
        
        # Calculate correlation
        correlation = stock1.correlation_with(stock2, "2023-06-01", "2023-06-21")
        print(f"Correlation: {correlation:.3f}")
        
        # Compare performance
        perf1 = stock1.performance_stats("2023-06-01", "2023-06-21")
        perf2 = stock2.performance_stats("2023-06-01", "2023-06-21")
        
        print("\nPerformance Comparison:")
        print(f"Stock 1 ({stock1.code}) - Total Return: {perf1['total_return']:.2%}, Volatility: {perf1['volatility']:.2%}")
        print(f"Stock 2 ({stock2.code}) - Total Return: {perf2['total_return']:.2%}, Volatility: {perf2['volatility']:.2%}")
        
        # Compare volume profiles
        vol1 = stock1.volume_profile("2023-06-01", "2023-06-21")
        vol2 = stock2.volume_profile("2023-06-01", "2023-06-21")
        
        print("\nVolume Comparison:")
        print(f"Stock 1 ({stock1.code}) - Avg Volume: {vol1['avg_volume']:,.0f}, VWAP: ¥{vol1['vwap']:.2f}")
        print(f"Stock 2 ({stock2.code}) - Avg Volume: {vol2['avg_volume']:,.0f}, VWAP: ¥{vol2['vwap']:.2f}")
        
    except Exception as e:
        print(f"Error in stock comparison demo: {e}")


def demo_advanced_usage():
    """Demonstrate advanced usage patterns."""
    print("\n" + "="*60)
    print("🔬 ADVANCED USAGE DEMONSTRATION")
    print("="*60)
    
    try:
        # Custom date ranges and periods
        print("\n📅 Custom Analysis Periods:")
        stock = Stock("13010")
        
        # Different return periods
        returns = stock.returns("2023-06-01", "2023-06-21", periods=[1, 3, 5, 10])
        print(f"Multi-period returns calculated for {len(returns)} days")
        
        # Rolling volatility analysis
        vol_metrics = stock.volatility_metrics("2023-06-01", "2023-06-21", window=5)
        print(f"5-day rolling volatility: {vol_metrics['current_volatility']:.2%}")
        
        # Custom portfolio weights
        print("\n⚖️ Custom Portfolio Weights:")
        custom_portfolio = Portfolio(
            ["13010", "13050", "13060"],
            weights=[0.5, 0.3, 0.2]  # Custom weights
        )
        print(f"Custom weighted portfolio: {custom_portfolio}")
        
        # Portfolio risk analysis
        risk = custom_portfolio.risk_metrics("2023-06-01", "2023-06-21")
        print(f"Custom portfolio volatility: {risk['portfolio_volatility']:.2%}")
        
        # Market screening with multiple criteria
        print("\n🔍 Advanced Market Screening:")
        market = Market()
        
        # Screen for moderate volatility, high volume stocks
        screened = market.screen_by_volatility(min_vol=0.1, max_vol=0.5, limit=5)
        if len(screened) > 0:
            print("Moderate volatility stocks (10-50% annualized):")
            print(screened.select(["code", "avg_price", "annualized_volatility"]))
        
    except Exception as e:
        print(f"Error in advanced usage demo: {e}")


def main() -> int:
    """Main demonstration workflow."""
    parser = argparse.ArgumentParser(description="Demonstrate Stock API capabilities")
    parser.add_argument(
        "--demo",
        choices=["stock", "portfolio", "market", "comparison", "advanced", "all"],
        default="all",
        help="Which demo to run"
    )
    
    args = parser.parse_args()
    
    # Load environment
    load_env_file_if_present()
    
    print("🚀 HIGH-LEVEL STOCK API DEMONSTRATION")
    print("This demo showcases the object-oriented Stock, Portfolio, and Market APIs")
    print("that provide a user-friendly interface for stock analysis.")
    
    try:
        if args.demo in ["stock", "all"]:
            demo_stock_api()
        
        if args.demo in ["portfolio", "all"]:
            demo_portfolio_api()
        
        if args.demo in ["market", "all"]:
            demo_market_api()
        
        if args.demo in ["comparison", "all"]:
            demo_stock_comparison()
        
        if args.demo in ["advanced", "all"]:
            demo_advanced_usage()
        
        print("\n" + "="*60)
        print("✅ STOCK API DEMONSTRATION COMPLETED")
        print("="*60)
        print("\nKey Features Demonstrated:")
        print("• Stock class: Individual stock analysis with caching")
        print("• Portfolio class: Multi-stock portfolio analytics")
        print("• Market class: Market-wide screening and statistics")
        print("• Correlation analysis and stock comparison")
        print("• Risk metrics and performance statistics")
        print("• Integration with existing bronze/silver storage")
        
        print("\nUsage Examples:")
        print("• toyota = Stock('7203')")
        print("• portfolio = Portfolio(['7203', '6758', '9984'])")
        print("• market = Market()")
        print("• correlation = toyota.correlation_with('6758')")
        
        return 0
        
    except Exception as e:
        logger.error(f"Demonstration failed: {e}")
        return 1


if __name__ == "__main__":
    main()
