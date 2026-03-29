"""
Silver layer — cleaning, normalisation, and incremental merge.

Responsibilities (per table)
-----------------------------
1. Read today's Bronze partition for the table.
2. Read the existing Silver file (if any) and union with the new data.
3. Apply table-specific transformations:
   - Parse timestamps (handles DD/MM/YYYY and YYYY-MM-DD mixed formats)
   - Normalise strings (strip whitespace, consistent casing)
   - Fix Brazilian decimal commas in numeric fields ("0,00" → 0.0)
   - Replace invalid placeholders ("N/A", "n/a") with NaN/NaT
   - Compute derived columns (order_date, item_net_amount)
4. Deduplicate by primary key (keep='last' — most recent ingest wins).
5. Validate against the Silver schema (strict mode).
6. Overwrite data/silver/<table_name>.csv.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

from pipeline.base import LayerProcessor
from config.settings import BRONZE_DIR, SILVER_DIR, PRIMARY_KEYS
from config.schemas import SILVER_SCHEMAS

logger = logging.getLogger(__name__)

# Regex that matches common "not available" placeholders (case-insensitive)
_NA_PATTERN = r"(?i)^\s*(n/?a|null|none|nan|-)\s*$"


class SilverCleaner(LayerProcessor):
    """Cleans and normalises Bronze data into the Silver layer.

    Parameters
    ----------
    ingest_date:
        Date of the Bronze partition to process (``YYYY-MM-DD``).
        Defaults to today.
    """

    layer_name = "SILVER"

    def __init__(self, ingest_date: str | None = None):
        self.ingest_date = ingest_date or date.today().strftime("%Y-%m-%d")

    def run(self, table_name: str, **kwargs) -> pd.DataFrame:
        """
        Full Silver processing for one table.

        Returns
        -------
        pd.DataFrame
            The clean, deduplicated DataFrame written to Silver.
            Returns an empty DataFrame (and skips writing) if no Bronze
            data is found for the given date.
        """
        self._log(f"Processing '{table_name}' [{self.ingest_date}]")

        # 1. Read today's Bronze data
        bronze_path = BRONZE_DIR / f"ingest_date={self.ingest_date}" / f"{table_name}.csv"
        if not bronze_path.exists():
            self._log(
                f"'{table_name}': Bronze file not found for {self.ingest_date} — skipping",
                "warning",
            )
            return pd.DataFrame()

        new_df = pd.read_csv(bronze_path, dtype=str, keep_default_na=False)
        self._log(f"'{table_name}': {len(new_df)} rows read from Bronze")

        # 2. Union with existing Silver
        merged_df = self._merge_with_existing(new_df, table_name)

        # 3. Apply transformations
        transformed_df = self._apply_transforms(merged_df, table_name)

        # 4. Drop rows with null primary key (cannot be keyed — discard with warning)
        transformed_df = self._drop_null_pks(transformed_df, table_name)

        # 5. Deduplicate
        deduped_df = self._dedup(transformed_df, table_name)

        # 6. Strict schema validation
        if table_name in SILVER_SCHEMAS:
            deduped_df = self.validate(
                deduped_df, SILVER_SCHEMAS[table_name], table_name, mode="strict"
            )
        else:
            self._log(f"No Silver schema for '{table_name}' — skipping validation", "warning")

        # 7. Write
        output_path = SILVER_DIR / f"{table_name}.csv"
        self.write(deduped_df, output_path)

        self._log(f"'{table_name}' done → {len(deduped_df)} rows")
        return deduped_df

    # ── Merge ─────────────────────────────────────────────────────────────────

    def _merge_with_existing(self, new_df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        silver_path = SILVER_DIR / f"{table_name}.csv"
        if silver_path.exists():
            existing_df = pd.read_csv(silver_path, dtype=str, keep_default_na=False)
            self._log(
                f"'{table_name}': merging {len(new_df)} new + {len(existing_df)} existing rows"
            )
            return pd.concat([existing_df, new_df], ignore_index=True)

        self._log(f"'{table_name}': no existing Silver — first load")
        return new_df.copy()

    # ── Null PK filter ────────────────────────────────────────────────────────

    def _drop_null_pks(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Drop rows where any primary key column is null (they cannot be keyed)."""
        pks = PRIMARY_KEYS.get(table_name, [])
        if not pks:
            return df
        mask = df[pks].isnull().any(axis=1)
        n_dropped = mask.sum()
        if n_dropped:
            self._log(
                f"'{table_name}': dropped {n_dropped} row(s) with null PK {pks}",
                "warning",
            )
        return df[~mask].reset_index(drop=True)

    # ── Dedup ─────────────────────────────────────────────────────────────────

    def _dedup(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        pks = PRIMARY_KEYS.get(table_name, [])
        if not pks:
            return df
        before = len(df)
        df = df.drop_duplicates(subset=pks, keep="last").reset_index(drop=True)
        removed = before - len(df)
        if removed:
            self._log(f"'{table_name}': removed {removed} duplicate row(s)")
        return df

    # ── Transformation dispatcher ─────────────────────────────────────────────

    def _apply_transforms(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        dispatch: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
            "orders":      self._transform_orders,
            "order_items": self._transform_order_items,
            "shipments":   self._transform_shipments,
            "customers":   self._transform_customers,
            "products":    self._transform_products,
        }
        fn = dispatch.get(table_name)
        if fn is None:
            self._log(f"No transform defined for '{table_name}' — passing through", "warning")
            return df
        return fn(df)

    # ── NA-string sanitiser ───────────────────────────────────────────────────

    @staticmethod
    def _parse_datetime_mixed(series: pd.Series) -> pd.Series:
        """Parse a Series that mixes DD/MM/YYYY HH:MM (Brazilian) and
        YYYY-MM-DD HH:MM:SS (ISO) datetime strings.

        Multi-pass strategy
        -------------------
        Passes 1-3 — explicit Brazilian (DD/MM/YYYY) formats to unambiguously
                     resolve day-first dates before pandas inference kicks in.
        Pass 4    — generic ``pd.to_datetime`` for remaining rows (ISO strings,
                    etc.).

        This avoids the pandas 3.x ``dayfirst=True`` inference bug where
        "01/12/2025" is misread as January 12 instead of December 1.
        """
        _FORMATS = [
            "%d/%m/%Y %H:%M",        # "01/12/2025 05:57"
            "%d/%m/%Y %H:%M:%S",     # "01/12/2025 05:57:00"
            "%d/%m/%Y",              # "18/03/2024"
        ]
        result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        remaining = series.copy()

        for fmt in _FORMATS:
            parsed = pd.to_datetime(remaining, format=fmt, errors="coerce")
            filled = parsed.notna()
            result.loc[filled] = parsed.loc[filled]
            remaining = remaining.where(~filled)   # blank out what was parsed

        # Final pass: ISO and anything left.
        # format="mixed" is required here because after incrementally filling
        # matched rows, the remaining values can be a heterogeneous mix of
        # "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS", etc.  Without format="mixed",
        # pandas 3.x infers ONE format for the whole Series and silently returns
        # NaT for rows that don't fit — format="mixed" parses each value
        # independently, which is exactly what we need.
        fallback_mask = result.isna() & series.notna()
        if fallback_mask.any():
            result.loc[fallback_mask] = pd.to_datetime(
                series.loc[fallback_mask], format="mixed", errors="coerce"
            )
        return result

    @staticmethod
    def _clean_na_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Replace "N/A", "n/a", "null", "none", "nan", "-" with pd.NA
        across all columns.  Called in every transform after the initial
        ``df.replace("", pd.NA)`` so that placeholders never leak into
        analytics as literal strings.

        Note: at the point this is called all columns are still raw strings,
        so applying replace() to the whole DataFrame is safe and avoids the
        select_dtypes() StringDtype deprecation warning in pandas 3.x.
        """
        return df.replace(_NA_PATTERN, pd.NA, regex=True)

    # ── Table-level transformations ───────────────────────────────────────────

    def _transform_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Replace blank strings and N/A-like placeholders before type coercion
        df.replace("", pd.NA, inplace=True)
        df = self._clean_na_strings(df)

        df["order_ts"] = self._parse_datetime_mixed(df["order_ts"])
        # order_date = calendar date only (time set to 00:00:00)
        df["order_date"] = df["order_ts"].dt.normalize()

        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

        df["status"]         = df["status"].str.strip().str.lower()
        df["currency"]       = df["currency"].str.strip().str.upper()
        df["payment_method"] = df["payment_method"].str.strip().str.lower()
        df["customer_id"]    = df["customer_id"].str.strip()
        df["order_id"]       = df["order_id"].str.strip()
        return df

    def _transform_order_items(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.replace("", pd.NA, inplace=True)
        df = self._clean_na_strings(df)

        df["quantity"]        = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        df["unit_price"]      = pd.to_numeric(df["unit_price"], errors="coerce")
        df["discount_amount"] = pd.to_numeric(df["discount_amount"], errors="coerce").fillna(0.0)

        # Negative quantity or unit_price are data errors — set to NaN
        neg_qty   = df["quantity"] < 0
        neg_price = df["unit_price"] < 0
        if neg_qty.any():
            self._log(f"order_items: {neg_qty.sum()} negative quantity value(s) set to NaN", "warning")
            df.loc[neg_qty, "quantity"] = 0  # keep as 0 so row is preserved
        if neg_price.any():
            self._log(f"order_items: {neg_price.sum()} negative unit_price value(s) set to NaN", "warning")
            df.loc[neg_price, "unit_price"] = float("nan")

        # Derived column: item-level net revenue
        df["item_net_amount"] = (df["quantity"] * df["unit_price"]) - df["discount_amount"]

        df["order_id"]   = df["order_id"].str.strip()
        df["product_id"] = df["product_id"].str.strip()
        return df

    def _transform_shipments(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.replace("", pd.NA, inplace=True)
        # Replace "N/A", "n/a", "null", "none", "-" etc. across all string columns
        # (covers shipped_ts, delivered_ts, carrier, delivery_status, …)
        df = self._clean_na_strings(df)

        # Normalise carrier to uppercase for consistent grouping
        # ("CORREIOS", "Correios", "Correios  " → "CORREIOS")
        df["carrier"] = df["carrier"].str.strip().str.upper()

        # Brazilian decimal comma: "0,00" → 0.0; non-numeric ("free") → NaN; negative → NaN
        df["shipping_cost"] = (
            df["shipping_cost"]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )
        negative_mask = df["shipping_cost"] < 0
        if negative_mask.any():
            self._log(
                f"shipments: {negative_mask.sum()} negative shipping_cost value(s) set to NaN",
                "warning",
            )
            df.loc[negative_mask, "shipping_cost"] = pd.NA

        df["shipped_ts"]   = self._parse_datetime_mixed(df["shipped_ts"])
        df["delivered_ts"] = self._parse_datetime_mixed(df["delivered_ts"])

        df["delivery_status"] = df["delivery_status"].str.strip().str.lower()
        df["order_id"]        = df["order_id"].str.strip()
        return df

    def _transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.replace("", pd.NA, inplace=True)
        df = self._clean_na_strings(df)

        # Standardise state abbreviation to uppercase (e.g. "Rj" → "RJ")
        df["state"] = df["state"].str.strip().str.upper()
        df["city"]  = df["city"].str.strip().str.title()

        df["created_ts"]  = self._parse_datetime_mixed(df["created_ts"])
        df["customer_id"] = df["customer_id"].str.strip()
        return df

    def _transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.replace("", pd.NA, inplace=True)
        df = self._clean_na_strings(df)

        # Normalise to title case for consistent grouping
        # ("Eletrônicos", "ELETRÔNICOS" → "Eletrônicos"; "ACME", "Acme" → "Acme")
        df["category"]   = df["category"].str.strip().str.title()
        df["brand"]      = df["brand"].str.strip().str.title()
        df["created_ts"] = self._parse_datetime_mixed(df["created_ts"])
        df["product_id"] = df["product_id"].str.strip()
        return df
