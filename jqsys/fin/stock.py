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
    ) -> pl.DataFrame:
        """Fetch price history for the stock from the gold layer."""
        start = self._normalise_date(start_date) if start_date else None
        end = self._normalise_date(end_date) if end_date else None
        cols = list(columns) if columns is not None else None

        return self._gold.read_stock_prices(
            code=self.code,
            start_date=start,
            end_date=end,
            columns=cols,
        )

    def get_latest_price(self, *, columns: Iterable[str] | None = None) -> dict[str, Any] | None:
        """Return the latest price record as a mapping, if available."""
        history = self.get_price_history(columns=columns)
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
