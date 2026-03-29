"""
Gold layer — analytics-ready fact and dimension tables.

Tables produced
---------------
fact_orders       One row per order — full revenue, shipping, and delivery KPIs.
fact_order_items  One row per (order, product) — item-level revenue + discount %.
dim_customers     Deduped customer dimension.
dim_products      Deduped product dimension.

All tables are read from Silver, joined/aggregated here, validated against
the Gold schema (strict mode), and written to data/gold/.
"""

import logging

import pandas as pd

from pipeline.base import LayerProcessor
from config.settings import SILVER_DIR, GOLD_DIR, LATE_DELIVERY_HOURS
from config.schemas import GOLD_SCHEMAS

logger = logging.getLogger(__name__)


class GoldBuilder(LayerProcessor):
    """Builds Gold analytics tables from Silver data."""

    layer_name = "GOLD"

    def run(self, table_name: str, **kwargs) -> pd.DataFrame:
        """Build, validate, and write one Gold table.

        Parameters
        ----------
        table_name:
            One of ``fact_orders | fact_order_items | dim_customers | dim_products``.
        """
        builders = {
            "fact_orders":      self._build_fact_orders,
            "fact_order_items": self._build_fact_order_items,
            "dim_customers":    self._build_dim_customers,
            "dim_products":     self._build_dim_products,
        }

        fn = builders.get(table_name)
        if fn is None:
            raise ValueError(
                f"Unknown Gold table '{table_name}'. "
                f"Valid options: {list(builders)}"
            )

        self._log(f"Building '{table_name}'…")
        df = fn()

        if table_name in GOLD_SCHEMAS:
            df = self.validate(df, GOLD_SCHEMAS[table_name], table_name, mode="strict")

        output_path = GOLD_DIR / f"{table_name}.csv"
        self.write(df, output_path)

        self._log(f"'{table_name}' done → {len(df)} rows")
        return df

    # ── Silver readers ────────────────────────────────────────────────────────

    def _read_silver(self, table_name: str) -> pd.DataFrame:
        path = SILVER_DIR / f"{table_name}.csv"
        if not path.exists():
            raise FileNotFoundError(
                f"GoldBuilder: Silver table not found → {path}\n"
                "Run the Silver layer first."
            )
        return pd.read_csv(path)

    # ── fact_orders ───────────────────────────────────────────────────────────

    def _build_fact_orders(self) -> pd.DataFrame:
        orders    = self._read_silver("orders")
        shipments = self._read_silver("shipments")
        items     = self._read_silver("order_items")

        # ── Aggregate order_items to order level ─────────────────────────────
        items_agg = (
            items
            .groupby("order_id", as_index=False)
            .agg(
                discount_total=("discount_amount", "sum"),
                net_amount    =("item_net_amount",  "sum"),
                items_count   =("product_id",       "nunique"),   # distinct SKUs
                total_units   =("quantity",          "sum"),       # total units sold
            )
        )

        # ── Joins (left — orders is the source of truth) ──────────────────────
        fact = orders.merge(
            shipments[[
                "order_id", "carrier", "shipping_cost",
                "shipped_ts", "delivered_ts", "delivery_status",
            ]],
            on="order_id", how="left",
        )
        fact = fact.merge(items_agg, on="order_id", how="left")

        # ── Parse timestamps (Silver stores datetime as string in CSV) ────────
        fact["order_ts"]     = pd.to_datetime(fact["order_ts"],     errors="coerce")
        fact["order_date"]   = pd.to_datetime(fact["order_date"],   errors="coerce")
        fact["shipped_ts"]   = pd.to_datetime(fact["shipped_ts"],   errors="coerce")
        fact["delivered_ts"] = pd.to_datetime(fact["delivered_ts"], errors="coerce")

        # ── Base revenue ──────────────────────────────────────────────────────
        fact["gross_amount"]   = pd.to_numeric(fact["total_amount"], errors="coerce")
        fact["shipping_cost"]  = pd.to_numeric(fact["shipping_cost"], errors="coerce")
        fact["discount_total"] = fact["discount_total"].fillna(0.0)
        # net_amount = sum of item_net_amount; fallback to gross when items absent
        fact["net_amount"]     = fact["net_amount"].fillna(fact["gross_amount"])
        fact["items_count"]    = fact["items_count"].fillna(0).astype(int)
        fact["total_units"]    = fact["total_units"].fillna(0).astype(int)

        # ── Revenue KPIs ──────────────────────────────────────────────────────
        # Use float NaN (not pd.NA) so .round() works in pandas >= 2.2 / 3.x
        _NaN = float("nan")

        # Discount rate relative to gross amount
        fact["discount_pct"] = (
            fact["discount_total"] / fact["gross_amount"].replace(0, _NaN)
        ).fillna(0.0).round(4)

        # Freight as a share of net revenue (useful for margin analysis)
        fact["shipping_pct_net"] = (
            fact["shipping_cost"] / fact["net_amount"].replace(0, _NaN)
        ).round(4)

        # Average net value per unit in the order
        fact["avg_unit_price"] = (
            fact["net_amount"] / fact["total_units"].replace(0, _NaN)
        ).round(2)

        # ── Delivery KPIs ─────────────────────────────────────────────────────
        # pd.to_numeric prevents NAType.__round__ errors (pandas >= 2.2 / 3.x)
        def _td_hours(a: pd.Series, b: pd.Series) -> pd.Series:
            return (
                pd.to_numeric((a - b).dt.total_seconds(), errors="coerce")
                .div(3600).round(2)
            )

        def _td_days(a: pd.Series, b: pd.Series) -> pd.Series:
            return (
                pd.to_numeric((a - b).dt.total_seconds(), errors="coerce")
                .div(86400).round(2)
            )

        fact["delivery_time_hours"] = _td_hours(fact["delivered_ts"], fact["shipped_ts"])
        fact["is_late"] = fact["delivery_time_hours"] > LATE_DELIVERY_HOURS

        # Days from order placement to dispatch (processing speed)
        fact["days_to_ship"] = _td_days(fact["shipped_ts"], fact["order_ts"])

        # ── Delivery status flags ─────────────────────────────────────────────
        _status = fact["delivery_status"].fillna("").str.lower()
        fact["is_delivered"] = _status == "delivered"
        fact["is_returned"]  = _status == "returned"

        # ── Temporal dimensions (for report slicing) ──────────────────────────
        fact["order_year"]       = fact["order_date"].dt.year
        fact["order_month"]      = fact["order_date"].dt.month
        fact["order_year_month"] = fact["order_date"].dt.strftime("%Y-%m")
        fact["order_weekday"]    = fact["order_date"].dt.day_name()

        # Date-only versions of timestamps (for daily reporting)
        fact["shipped_date"]   = fact["shipped_ts"].dt.normalize()
        fact["delivered_date"] = fact["delivered_ts"].dt.normalize()

        # ── Rename status for clarity ─────────────────────────────────────────
        fact = fact.rename(columns={"status": "status_final"})

        # ── Select and order final columns ────────────────────────────────────
        final_columns = [
            # ── Keys
            "order_id", "customer_id",
            # ── Temporal
            "order_date", "order_ts",
            "order_year", "order_month", "order_year_month", "order_weekday",
            # ── Revenue
            "gross_amount", "discount_total", "discount_pct", "net_amount",
            "items_count", "total_units", "avg_unit_price",
            # ── Payment & Status
            "payment_method", "status_final",
            # ── Shipping
            "carrier", "shipping_cost", "shipping_pct_net",
            "shipped_date", "shipped_ts",
            "delivered_date", "delivered_ts",
            # ── Delivery KPIs
            "days_to_ship", "delivery_time_hours", "is_late",
            "is_delivered", "is_returned", "delivery_status",
        ]
        return fact[final_columns].reset_index(drop=True)

    # ── fact_order_items ──────────────────────────────────────────────────────

    def _build_fact_order_items(self) -> pd.DataFrame:
        items = self._read_silver("order_items")
        df    = items[[
            "order_id", "product_id", "quantity",
            "unit_price", "discount_amount", "item_net_amount",
        ]].copy()

        # Explicit gross before discount (makes discount impact transparent)
        df["gross_item_amount"] = (df["quantity"] * df["unit_price"]).round(2)

        # Discount rate at item level
        _NaN = float("nan")
        df["discount_pct"] = (
            df["discount_amount"] / df["gross_item_amount"].replace(0, _NaN)
        ).fillna(0.0).round(4)

        final_columns = [
            "order_id", "product_id",
            "quantity", "unit_price",
            "gross_item_amount",
            "discount_amount", "discount_pct",
            "item_net_amount",
        ]
        return df[final_columns].reset_index(drop=True)

    # ── dim_customers ─────────────────────────────────────────────────────────

    def _build_dim_customers(self) -> pd.DataFrame:
        customers = self._read_silver("customers")
        return (
            customers[["customer_id", "state", "city", "created_ts"]]
            .drop_duplicates(subset=["customer_id"], keep="last")
            .reset_index(drop=True)
        )

    # ── dim_products ──────────────────────────────────────────────────────────

    def _build_dim_products(self) -> pd.DataFrame:
        products = self._read_silver("products")
        return (
            products[["product_id", "category", "brand", "created_ts"]]
            .drop_duplicates(subset=["product_id"], keep="last")
            .reset_index(drop=True)
        )
