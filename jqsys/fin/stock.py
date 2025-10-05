"""Stock-level access built on top of bronze and gold storage layers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any, Literal
from weakref import WeakKeyDictionary

import polars as pl

from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.gold import GoldStorage

LISTED_INFO_ENDPOINT = "listed_info"
DEFAULT_CODE_COLUMN = "Code"


class Stock:
    """Represents a single security, backed by bronze and gold storage layers.

    The class exposes convenient helpers for retrieving master data from the
    bronze `listed_info` snapshots and price history from the gold layer.
    """

    _listed_info_cache: WeakKeyDictionary[BronzeStorage, tuple[datetime, pl.DataFrame]] = (  # type: ignore[assignment]
        WeakKeyDictionary()
    )

    def __init__(
        self,
        code: str,
        bronze_storage: BronzeStorage | None = None,
        gold_storage: GoldStorage | None = None,
        listed_info: Mapping[str, Any] | None = None,
    ) -> None:
        self._bronze = bronze_storage or BronzeStorage()
        self._gold = gold_storage or GoldStorage()
        self.code = self._resolve_code(str(code), self._bronze, self._gold)
        self._listed_info_cache = dict(listed_info) if listed_info is not None else None

    # ------------------------------------------------------------------
    # Class helpers
    # ------------------------------------------------------------------
    @classmethod
    def search(
        cls,
        field: str,
        value: str,
        *,
        bronze_storage: BronzeStorage | None = None,
        gold_storage: GoldStorage | None = None,
        match: Literal["exact", "icontains"] = "exact",
    ) -> list[Stock]:
        """Search listed securities using bronze `listed_info` data.

        Args:
            field: Column name in the listed_info snapshot to search.
            value: Target string value.
            bronze_storage: Optional bronze storage instance (defaults to configured backend).
            gold_storage: Optional gold storage instance reused for each result.
            match: Either ``"exact"`` or ``"icontains"`` for case-insensitive containment.

        Returns:
            List of :class:`Stock` instances matching the criteria.
        """
        bronze = bronze_storage or BronzeStorage()
        table = cls._load_latest_listed_info(bronze)

        if table.is_empty():
            return []
        if field not in table.columns:
            raise ValueError(f"Field '{field}' not found in listed info columns: {table.columns}")

        column = table.get_column(field).cast(pl.Utf8, strict=False)
        if match == "exact":
            mask = column == value
        elif match == "icontains":
            lower_value = value.lower()
            mask = column.str.to_lowercase().str.contains(lower_value, literal=True)
        else:
            raise ValueError("match must be 'exact' or 'icontains'")

        filtered = table.filter(mask)
        results: list[Stock] = []
        for row in filtered.iter_rows(named=True):
            code_value = str(row.get(DEFAULT_CODE_COLUMN, ""))
            if not code_value:
                continue
            results.append(
                cls(
                    code=code_value,
                    bronze_storage=bronze,
                    gold_storage=gold_storage,
                    listed_info=row,
                )
            )
        return results

    @classmethod
    def _load_latest_listed_info(cls, bronze: BronzeStorage) -> pl.DataFrame:
        """Return the latest listed info snapshot for the given bronze storage."""
        cached = cls._listed_info_cache.get(bronze)
        dates = bronze.list_available_dates(LISTED_INFO_ENDPOINT)
        if not dates:
            return pl.DataFrame()

        latest = max(dates)
        if cached and cached[0] == latest:
            return cached[1]

        df = bronze.read_raw_data(LISTED_INFO_ENDPOINT, date=latest)
        cls._listed_info_cache[bronze] = (latest, df)
        return df

    # ------------------------------------------------------------------
    # Instance helpers
    # ------------------------------------------------------------------
    @property
    def bronze_storage(self) -> BronzeStorage:
        return self._bronze

    @property
    def gold_storage(self) -> GoldStorage:
        return self._gold

    @property
    def base_code(self) -> str:
        """Return the 4-digit base code (without market suffix)."""
        return self.code[:4]

    def get_listed_info(self) -> Mapping[str, Any]:
        """Return master data for the security from bronze storage."""
        if self._listed_info_cache is not None:
            return self._listed_info_cache

        table = self._load_latest_listed_info(self._bronze)
        if table.is_empty():
            raise LookupError("No listed info data available in bronze storage")
        if DEFAULT_CODE_COLUMN not in table.columns:
            raise LookupError("Listed info snapshot missing 'Code' column")

        subset = table.filter(pl.col(DEFAULT_CODE_COLUMN) == self.code)
        if subset.is_empty():
            raise LookupError(f"No listed info found for code {self.code}")

        self._listed_info_cache = subset.row(0, named=True)
        return self._listed_info_cache

    # Convenience accessors ------------------------------------------------
    @property
    def company_name(self) -> str | None:
        return self.get_listed_info().get("CompanyName")

    @property
    def company_name_english(self) -> str | None:
        return self.get_listed_info().get("CompanyNameEnglish")

    @property
    def sector17_code(self) -> str | None:
        return self.get_listed_info().get("Sector17Code")

    @property
    def sector33_code(self) -> str | None:
        return self.get_listed_info().get("Sector33Code")

    @property
    def market_code(self) -> str | None:
        return self.get_listed_info().get("MarketCode")

    # Price access ---------------------------------------------------------
    def get_price_history(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        columns: Iterable[str] | None = None,
        adjust: Literal["none", "add", "replace"] = "none",
        adjust_volume: bool = True,
        adjust_turnover: bool = False,
    ) -> pl.DataFrame:
        """Fetch price history for the stock from the gold layer.

        Args:
            start_date: Optional inclusive start boundary.
            end_date: Optional inclusive end boundary.
            columns: Optional subset of columns to return.
            adjust: Control how adjusted price columns are produced:
                - ``"none"``: return raw values as stored.
                - ``"add"``: add ``adj_<column>`` alongside the raw columns.
                - ``"replace"``: overwrite price (and optionally volume/turnover)
                  columns with adjusted values.
            adjust_volume: When adjustments are applied, scale volume columns
                using the inverse adjustment factor (default True).
            adjust_turnover: When True and adjustments are applied, also scale
                turnover values using the adjustment factor. Disabled by
                default as turnover is typically invariant.
        """
        start = self._normalise_date(start_date) if start_date else None
        end = self._normalise_date(end_date) if end_date else None
        df = self._gold.read_stock_prices(
            code=self.code,
            start_date=start,
            end_date=end,
            columns=None,
        )

        if df.is_empty():
            return df.select(columns) if columns else df

        if "date" in df.columns:
            df = df.sort("date")

        if adjust != "none":
            df = self._apply_adjustments(
                df,
                mode=adjust,
                adjust_volume=adjust_volume,
                adjust_turnover=adjust_turnover,
            )

        if columns:
            cols = list(columns)
            missing = [col for col in cols if col not in df.columns]
            if missing:
                raise ValueError(f"Requested columns not present: {missing}")
            df = df.select(cols)

        return df

    def open_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        adjusted: bool = True,
    ) -> pl.Series:
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        df = self.get_price_history(start_date, end_date, columns=["open"], adjust=mode)
        return df.get_column("open")

    def high_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        adjusted: bool = True,
    ) -> pl.Series:
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        df = self.get_price_history(start_date, end_date, columns=["high"], adjust=mode)
        return df.get_column("high")

    def low_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        adjusted: bool = True,
    ) -> pl.Series:
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        df = self.get_price_history(start_date, end_date, columns=["low"], adjust=mode)
        return df.get_column("low")

    def close_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        adjusted: bool = True,
    ) -> pl.Series:
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        df = self.get_price_history(start_date, end_date, columns=["close"], adjust=mode)
        return df.get_column("close")

    def volume_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        adjusted: bool = True,
    ) -> pl.Series:
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        df = self.get_price_history(start_date, end_date, columns=["volume"], adjust=mode)
        return df.get_column("volume")

    def turnover_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        *,
        adjusted: bool = False,
    ) -> pl.Series:
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        df = self.get_price_history(
            start_date,
            end_date,
            columns=["turnover_value"],
            adjust=mode,
            adjust_turnover=adjusted,
        )
        return df.get_column("turnover_value")

    def adjustment_factor_series(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
    ) -> pl.Series:
        df = self.get_price_history(start_date, end_date, columns=["adjustment_factor"])
        return df.get_column("adjustment_factor")

    def adjustment_events(
        self,
        *,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
        tolerance: float = 1e-9,
    ) -> pl.DataFrame:
        """Return dates where the adjustment factor differs from 1.0.

        Args:
            start_date: Optional inclusive start boundary to inspect.
            end_date: Optional inclusive end boundary to inspect.
            tolerance: Absolute tolerance when comparing against 1.0.

        Returns:
            DataFrame containing ``date`` and ``adjustment_factor`` rows with
            material adjustments.
        """
        df = self.get_price_history(
            start_date,
            end_date,
            columns=["date", "adjustment_factor"],
        )

        if df.is_empty():
            return df

        return df.filter(
            pl.col("adjustment_factor").fill_null(1.0).cast(pl.Float64).sub(1.0).abs() > tolerance
        )

    def get_latest_price(
        self,
        *,
        columns: Iterable[str] | None = None,
        adjusted: bool = True,
    ) -> dict[str, Any] | None:
        """Return the latest price record as a mapping, if available."""
        mode: Literal["none", "add", "replace"] = "replace" if adjusted else "none"
        history = self.get_price_history(columns=columns, adjust=mode)
        if history.is_empty():
            return None
        return history.sort("date").row(-1, named=True)

    # Utilities ------------------------------------------------------------
    def __repr__(self) -> str:
        info = []
        name = self.company_name
        if name:
            info.append(name)
        market = self.market_code
        if market:
            info.append(market)
        info_str = f" ({', '.join(info)})" if info else ""
        return f"Stock(code='{self.code}'{info_str})"

    @staticmethod
    def _normalise_date(value: date | datetime | str | None) -> date:
        if value is None:
            raise ValueError("Date value cannot be None")
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d").date()
        raise TypeError(f"Unsupported date type: {type(value)!r}")

    @staticmethod
    def _resolve_code(code: str, bronze: BronzeStorage, gold: GoldStorage) -> str:
        cleaned = code.strip()
        if not cleaned.isdigit():
            raise ValueError(f"Stock code must be numeric: {code!r}")

        if len(cleaned) == 5:
            return cleaned

        if len(cleaned) != 4:
            raise ValueError("Stock code must be 4 or 5 digits")

        candidates: list[str] = []

        try:
            table = Stock._load_latest_listed_info(bronze)
        except Exception:  # pragma: no cover - fallback for misconfigured bronze
            table = pl.DataFrame()

        if not table.is_empty() and DEFAULT_CODE_COLUMN in table.columns:
            codes_series = table.get_column(DEFAULT_CODE_COLUMN).cast(pl.Utf8, strict=False)
            if cleaned in codes_series.to_list():
                return cleaned
            prefix_matches = table.select(pl.col(DEFAULT_CODE_COLUMN)).filter(
                pl.col(DEFAULT_CODE_COLUMN).str.starts_with(cleaned)
            )
            candidates.extend(prefix_matches.get_column(DEFAULT_CODE_COLUMN).to_list())

        if not candidates:
            try:
                stocks = gold.list_available_stocks()
            except Exception:  # pragma: no cover - e.g. missing backend
                stocks = []
            candidates.extend([c for c in stocks if c.startswith(cleaned)])

        if not candidates:
            return f"{cleaned}0"

        unique_candidates = sorted(set(map(str, candidates)))
        for candidate in unique_candidates:
            if candidate.endswith("0"):
                return candidate
        return unique_candidates[0]

    @staticmethod
    def _apply_adjustments(
        df: pl.DataFrame,
        *,
        mode: Literal["add", "replace"],
        adjust_volume: bool,
        adjust_turnover: bool,
    ) -> pl.DataFrame:
        if "adjustment_factor" not in df.columns:
            return df

        factor = pl.col("adjustment_factor").cast(pl.Float64).fill_null(1.0)
        safe_divisor = pl.when(factor == 0).then(1.0).otherwise(factor)

        exprs: list[pl.Expr] = []
        for col in ("open", "high", "low", "close"):
            if col in df.columns:
                exprs.append(
                    (pl.col(col).cast(pl.Float64) * factor).alias(
                        f"adj_{col}" if mode == "add" else col
                    )
                )

        if adjust_volume and "volume" in df.columns:
            exprs.append(
                (pl.col("volume").cast(pl.Float64) / safe_divisor).alias(
                    "adj_volume" if mode == "add" else "volume"
                )
            )

        if adjust_turnover and "turnover_value" in df.columns:
            exprs.append(
                (pl.col("turnover_value").cast(pl.Float64) * factor).alias(
                    "adj_turnover_value" if mode == "add" else "turnover_value"
                )
            )

        if not exprs:
            return df

        return df.with_columns(exprs)
