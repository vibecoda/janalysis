# Corporate Actions & Price Adjustment Implementation Plan

## Overview

Implement a robust corporate action adjustment system that ensures accurate historical price analysis by properly handling stock splits, mergers, dividend adjustments, and other corporate actions using J-Quants API data.

## J-Quants Data Analysis

Based on actual J-Quants API daily quotes data, we have access to:

### Available Fields
- **Raw Prices**: `Open`, `High`, `Low`, `Close`, `Volume`, `TurnoverValue`
- **Adjustment Factor**: `AdjustmentFactor` (cumulative adjustment multiplier)
- **Adjusted Prices**: `AdjustmentOpen`, `AdjustmentHigh`, `AdjustmentLow`, `AdjustmentClose`, `AdjustmentVolume`

### Key Observations
1. **J-Quants provides pre-calculated adjusted prices** - this significantly simplifies implementation
2. **AdjustmentFactor field** shows the cumulative adjustment (observed 1.0 for most stocks, but different ratios for splits)
3. **Volume adjustments** are provided alongside price adjustments
4. **Example**: Code 13290 shows 10:1 split adjustment (raw prices ~37,000 yen, adjusted ~3,700 yen)

## Implementation Strategy

### Phase 1: Data Model Enhancement

**1. Update Silver Layer Schema**
```sql
-- Enhance daily_prices table with adjustment tracking
ALTER TABLE daily_prices ADD COLUMN adjustment_factor REAL;
ALTER TABLE daily_prices ADD COLUMN raw_open REAL;
ALTER TABLE daily_prices ADD COLUMN raw_high REAL; 
ALTER TABLE daily_prices ADD COLUMN raw_low REAL;
ALTER TABLE daily_prices ADD COLUMN raw_close REAL;
ALTER TABLE daily_prices ADD COLUMN raw_volume BIGINT;
-- Keep existing open, high, low, close, volume as adjusted values
```

**2. Create Corporate Actions Tracking Table**
```sql
CREATE TABLE corporate_actions (
    date DATE NOT NULL,
    code TEXT NOT NULL,
    action_type TEXT NOT NULL, -- 'split', 'merger', 'dividend', 'other'
    adjustment_factor REAL NOT NULL,
    split_ratio_old INTEGER, -- e.g., 1 in 1:10 split
    split_ratio_new INTEGER, -- e.g., 10 in 1:10 split  
    description TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, code, action_type)
);
```

### Phase 2: Adjustment Factor Analysis System

**1. Corporate Action Detection**
```python
class CorporateActionDetector:
    """Detect corporate actions from adjustment factor changes."""
    
    def detect_adjustments(self, df: pl.DataFrame) -> pl.DataFrame:
        """Detect adjustment factor changes indicating corporate actions."""
        # Calculate day-over-day adjustment factor changes
        # Identify significant changes (>1% threshold)
        # Classify action types based on magnitude and patterns
    
    def classify_action_type(self, factor_change: float) -> str:
        """Classify corporate action based on adjustment factor change."""
        # Split: factor_change significantly < 1.0 (e.g., 0.1 for 10:1 split)
        # Dividend: small factor_change < 1.0 (e.g., 0.98 for 2% dividend)
        # Merger: varies significantly based on exchange ratio
```

**2. Historical Reconstruction**
```python
class PriceAdjustmentEngine:
    """Reconstruct consistent historical price series."""
    
    def build_adjustment_chain(self, code: str) -> pl.DataFrame:
        """Build complete adjustment factor chain for a stock."""
        # Get all adjustment factors chronologically
        # Calculate cumulative adjustments from any reference point
        # Handle edge cases (delisting, relisting, etc.)
    
    def apply_adjustments(self, prices_df: pl.DataFrame, 
                         method: str = "forward") -> pl.DataFrame:
        """Apply adjustments forward or backward in time."""
        # Forward: adjust historical prices to current basis
        # Backward: adjust current prices to historical basis
```

### Phase 3: Integration with Existing System

**1. Update SilverStorage Class**
```python
# In jqsys/storage/silver.py
class SilverStorage:
    def normalize_daily_quotes(self, date: datetime, 
                              adjustment_method: str = "j_quants") -> Optional[Path]:
        """Enhanced normalization with corporate action handling."""
        # Extract both raw and adjusted prices from J-Quants data
        # Store adjustment factors and detect corporate actions
        # Validate adjustment consistency
        # Apply additional adjustments if needed
```

**2. Enhance QueryEngine**
```python
# In jqsys/storage/query.py  
class QueryEngine:
    def get_adjusted_prices(self, codes: List[str], 
                          start_date: date, end_date: date,
                          adjustment_reference: str = "latest") -> pl.DataFrame:
        """Get consistently adjusted price series."""
        # Reference options: 'latest', 'date_specific', 'unadjusted'
        # Return prices adjusted to consistent reference point
    
    def get_corporate_actions(self, codes: List[str] = None,
                            start_date: date = None,
                            end_date: date = None) -> pl.DataFrame:
        """Query corporate actions with filtering."""
```

### Phase 4: Validation & Quality Assurance

**1. Adjustment Validation Framework**
```python
class AdjustmentValidator:
    """Validate corporate action adjustments for accuracy."""
    
    def validate_split_consistency(self, code: str, split_date: date) -> bool:
        """Validate that split adjustments are mathematically consistent."""
        # Check price ratios before/after split
        # Verify volume adjustments
        # Validate market cap conservation
    
    def validate_total_return_consistency(self, code: str, 
                                        period_start: date, 
                                        period_end: date) -> float:
        """Validate total return calculation across corporate actions."""
        # Calculate total return including all adjustments
        # Compare with alternative calculation methods
```

**2. Data Quality Monitoring**
```python
class CorporateActionMonitor:
    """Monitor corporate action data quality."""
    
    def detect_missing_adjustments(self) -> List[Dict]:
        """Detect stocks with potential missing corporate actions."""
        # Identify unusual price gaps without adjustment factors
        # Flag stocks with inconsistent adjustment patterns
    
    def validate_j_quants_adjustments(self) -> Dict[str, bool]:
        """Validate J-Quants pre-calculated adjustments."""
        # Cross-check adjustment factors with price ratios
        # Identify potential data quality issues
```

### Phase 5: Enhanced APIs

**1. Update Stock Class**
```python
# In jqsys/stock.py
class Stock:
    def get_adjusted_returns(self, period: str = "1D", 
                           adjustment_method: str = "total_return") -> pl.DataFrame:
        """Get returns with proper corporate action adjustments."""
        # Methods: 'price_only', 'total_return', 'reinvested_dividends'
    
    def get_corporate_action_history(self) -> pl.DataFrame:
        """Get complete corporate action history."""
    
    def plot_price_adjustment_impact(self, start_date: date, end_date: date):
        """Visualize impact of corporate actions on price series."""
```

**2. Portfolio Analysis Enhancement**  
```python
# In jqsys/portfolio.py
class Portfolio:
    def calculate_attribution(self, benchmark_codes: List[str]) -> Dict:
        """Calculate performance attribution with proper adjustments."""
        # Account for corporate actions in both portfolio and benchmark
        # Provide attribution by security and corporate action impact
```

## Implementation Timeline

### Week 1: Foundation
- [ ] Update data model with adjustment tracking fields
- [ ] Enhance SilverStorage to capture adjustment factors  
- [ ] Create basic corporate action detection logic
- [ ] Write comprehensive tests for adjustment calculations

### Week 2: Core Engine
- [ ] Implement PriceAdjustmentEngine with historical reconstruction
- [ ] Build CorporateActionDetector with classification logic
- [ ] Update QueryEngine with adjustment-aware queries
- [ ] Create validation framework for adjustment accuracy

### Week 3: Integration & Testing
- [ ] Integrate with existing Stock and Portfolio APIs
- [ ] Implement comprehensive validation tests
- [ ] Create monitoring and alerting for data quality
- [ ] Performance optimization for large datasets

### Week 4: Documentation & Polish
- [ ] Create user documentation with examples
- [ ] Write technical documentation for maintenance
- [ ] Performance benchmarks and optimization
- [ ] Final integration testing and bug fixes

## Success Metrics

1. **Data Accuracy**: 99.9% consistency in adjustment factor calculations
2. **Performance**: Sub-second query response for 5-year price series
3. **Coverage**: Handle 100% of corporate actions in J-Quants data
4. **Reliability**: Zero false positives in corporate action detection
5. **Usability**: Simple API for users to get properly adjusted prices

## Risk Mitigation

1. **J-Quants Data Quality**: Implement validation against alternative sources
2. **Complex Corporate Actions**: Handle edge cases with manual review process
3. **Performance Impact**: Use efficient Polars operations and DuckDB indexing
4. **Backward Compatibility**: Maintain existing API while adding new features
5. **Data Migration**: Provide tools to reprocess historical data with new adjustments

## Deliverables

1. **Enhanced Storage Layer**: Updated schemas and ingestion pipeline
2. **Adjustment Engine**: Complete corporate action processing system
3. **Validation Framework**: Comprehensive testing and monitoring tools
4. **Updated APIs**: Enhanced Stock and Portfolio classes with adjustment features
5. **Documentation**: User guides and technical documentation
6. **Test Suite**: Comprehensive tests covering all corporate action scenarios

This implementation leverages J-Quants' pre-calculated adjustment factors while adding robust validation, monitoring, and enhanced analytical capabilities for accurate financial analysis.