"""
Pandera schemas for all three pipeline layers.

Bronze  — loose validation: columns must be present, types are raw strings.
Silver  — strict types, transformed/derived columns required, basic range checks.
Gold    — business-ready: final types + business rule validations.
"""

import pandera as pa
from pandera import Column, DataFrameSchema, Check

# ─────────────────────────────────────────────────────────────────────────────
# BRONZE SCHEMAS  (data arrives "as-is" — only presence + min structure)
# strict=False allows extra metadata columns (_ingest_date, etc.)
# ─────────────────────────────────────────────────────────────────────────────

BronzeOrdersSchema = DataFrameSchema(
    columns={
        "order_id":       Column(str, nullable=False, coerce=True),
        "customer_id":    Column(str, nullable=False, coerce=True),
        "order_ts":       Column(str, nullable=True,  coerce=True),
        "status":         Column(str, nullable=True,  coerce=True),
        "payment_method": Column(str, nullable=True,  coerce=True),
        "total_amount":   Column(object, nullable=True),
        "currency":       Column(str, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="BronzeOrders",
)

BronzeOrderItemsSchema = DataFrameSchema(
    columns={
        "order_id":       Column(str, nullable=False, coerce=True),
        "product_id":     Column(str, nullable=False, coerce=True),
        "quantity":       Column(object, nullable=True),
        "unit_price":     Column(object, nullable=True),
        "discount_amount":Column(object, nullable=True),
    },
    strict=False,
    coerce=True,
    name="BronzeOrderItems",
)

BronzeShipmentsSchema = DataFrameSchema(
    columns={
        "order_id":       Column(str, nullable=False, coerce=True),
        "carrier":        Column(str, nullable=True,  coerce=True),
        "shipping_cost":  Column(object, nullable=True),
        "shipped_ts":     Column(str, nullable=True,  coerce=True),
        "delivered_ts":   Column(str, nullable=True,  coerce=True),
        "delivery_status":Column(str, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="BronzeShipments",
)

BronzeCustomersSchema = DataFrameSchema(
    columns={
        "customer_id": Column(str, nullable=False, coerce=True),
        "state":       Column(str, nullable=True,  coerce=True),
        "city":        Column(str, nullable=True,  coerce=True),
        "created_ts":  Column(str, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="BronzeCustomers",
)

BronzeProductsSchema = DataFrameSchema(
    columns={
        "product_id": Column(str, nullable=False, coerce=True),
        "category":   Column(str, nullable=True,  coerce=True),
        "brand":      Column(str, nullable=True,  coerce=True),
        "created_ts": Column(str, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="BronzeProducts",
)

# ─────────────────────────────────────────────────────────────────────────────
# SILVER SCHEMAS  (clean types, derived columns present, range checks)
# ─────────────────────────────────────────────────────────────────────────────

SilverOrdersSchema = DataFrameSchema(
    columns={
        "order_id":       Column(str,      nullable=False, coerce=True),
        "customer_id":    Column(str,      nullable=False, coerce=True),
        "order_ts":       Column(pa.DateTime, nullable=True,  coerce=True),
        "order_date":     Column(pa.DateTime, nullable=True,  coerce=True),
        "status":         Column(str,      nullable=True,  coerce=True),
        "payment_method": Column(str,      nullable=True,  coerce=True),
        "total_amount":   Column(float,    nullable=True,  coerce=True,
                                 checks=Check.ge(0, error="total_amount must be >= 0")),
        "currency":       Column(str,      nullable=True,  coerce=True),
        "_ingest_date":   Column(str,      nullable=False, coerce=True),
        "_source_file":   Column(str,      nullable=False, coerce=True),
        "_load_ts":       Column(str,      nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
    name="SilverOrders",
)

SilverOrderItemsSchema = DataFrameSchema(
    columns={
        "order_id":        Column(str,   nullable=False, coerce=True),
        "product_id":      Column(str,   nullable=False, coerce=True),
        "quantity":        Column(int,   nullable=False, coerce=True,
                                  checks=Check.ge(0)),
        "unit_price":      Column(float, nullable=True,  coerce=True,
                                  checks=Check.ge(0)),
        "discount_amount": Column(float, nullable=False, coerce=True,
                                  checks=Check.ge(0)),
        "item_net_amount": Column(float, nullable=True,  coerce=True),
        "_ingest_date":    Column(str,   nullable=False, coerce=True),
        "_source_file":    Column(str,   nullable=False, coerce=True),
        "_load_ts":        Column(str,   nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
    name="SilverOrderItems",
)

SilverShipmentsSchema = DataFrameSchema(
    columns={
        "order_id":        Column(str,         nullable=False, coerce=True),
        "carrier":         Column(str,         nullable=True,  coerce=True),
        "shipping_cost":   Column(float,       nullable=True,  coerce=True,
                                  checks=Check.ge(0)),
        "shipped_ts":      Column(pa.DateTime, nullable=True,  coerce=True),
        "delivered_ts":    Column(pa.DateTime, nullable=True,  coerce=True),
        "delivery_status": Column(str,         nullable=True,  coerce=True),
        "_ingest_date":    Column(str,         nullable=False, coerce=True),
        "_source_file":    Column(str,         nullable=False, coerce=True),
        "_load_ts":        Column(str,         nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
    name="SilverShipments",
)

SilverCustomersSchema = DataFrameSchema(
    columns={
        "customer_id": Column(str,         nullable=False, coerce=True),
        "state":       Column(str,         nullable=True,  coerce=True),
        "city":        Column(str,         nullable=True,  coerce=True),
        "created_ts":  Column(pa.DateTime, nullable=True,  coerce=True),
        "_ingest_date":Column(str,         nullable=False, coerce=True),
        "_source_file":Column(str,         nullable=False, coerce=True),
        "_load_ts":    Column(str,         nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
    name="SilverCustomers",
)

SilverProductsSchema = DataFrameSchema(
    columns={
        "product_id":  Column(str,         nullable=False, coerce=True),
        "category":    Column(str,         nullable=True,  coerce=True),
        "brand":       Column(str,         nullable=True,  coerce=True),
        "created_ts":  Column(pa.DateTime, nullable=True,  coerce=True),
        "_ingest_date":Column(str,         nullable=False, coerce=True),
        "_source_file":Column(str,         nullable=False, coerce=True),
        "_load_ts":    Column(str,         nullable=False, coerce=True),
    },
    strict=False,
    coerce=True,
    name="SilverProducts",
)

# ─────────────────────────────────────────────────────────────────────────────
# GOLD SCHEMAS  (analytics-ready, business rules enforced)
# ─────────────────────────────────────────────────────────────────────────────

GoldFactOrdersSchema = DataFrameSchema(
    columns={
        # ── Keys
        "order_id":             Column(str,         nullable=False, coerce=True),
        "customer_id":          Column(str,         nullable=False, coerce=True),
        # ── Temporal
        "order_date":           Column(pa.DateTime, nullable=True,  coerce=True),
        "order_ts":             Column(pa.DateTime, nullable=True,  coerce=True),
        "order_year":           Column(float,       nullable=True,  coerce=True),
        "order_month":          Column(float,       nullable=True,  coerce=True),
        "order_year_month":     Column(str,         nullable=True,  coerce=True),
        "order_weekday":        Column(str,         nullable=True,  coerce=True),
        # ── Revenue
        "gross_amount":         Column(float,       nullable=True,  coerce=True,
                                        checks=Check.ge(0)),
        "discount_total":       Column(float,       nullable=False, coerce=True,
                                        checks=Check.ge(0)),
        "discount_pct":         Column(float,       nullable=True,  coerce=True),
        "net_amount":           Column(float,       nullable=True,  coerce=True),
        "items_count":          Column(int,         nullable=False, coerce=True,
                                        checks=Check.ge(0)),
        "total_units":          Column(int,         nullable=False, coerce=True,
                                        checks=Check.ge(0)),
        "avg_unit_price":       Column(float,       nullable=True,  coerce=True),
        # ── Payment & Status
        "payment_method":       Column(str,         nullable=True,  coerce=True),
        "status_final":         Column(str,         nullable=True,  coerce=True),
        # ── Shipping
        "carrier":              Column(str,         nullable=True,  coerce=True),
        "shipping_cost":        Column(float,       nullable=True,  coerce=True),
        "shipping_pct_net":     Column(float,       nullable=True,  coerce=True),
        "shipped_date":         Column(pa.DateTime, nullable=True,  coerce=True),
        "shipped_ts":           Column(pa.DateTime, nullable=True,  coerce=True),
        "delivered_date":       Column(pa.DateTime, nullable=True,  coerce=True),
        "delivered_ts":         Column(pa.DateTime, nullable=True,  coerce=True),
        # ── Delivery KPIs
        "days_to_ship":         Column(float,       nullable=True,  coerce=True),
        "delivery_time_hours":  Column(float,       nullable=True,  coerce=True),
        "is_late":              Column(bool,        nullable=True,  coerce=True),
        "is_delivered":         Column(bool,        nullable=False, coerce=True),
        "is_returned":          Column(bool,        nullable=False, coerce=True),
        "delivery_status":      Column(str,         nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="GoldFactOrders",
)

GoldFactOrderItemsSchema = DataFrameSchema(
    columns={
        "order_id":           Column(str,   nullable=False, coerce=True),
        "product_id":         Column(str,   nullable=False, coerce=True),
        "quantity":           Column(int,   nullable=False, coerce=True,
                                     checks=Check.ge(0)),
        "unit_price":         Column(float, nullable=True,  coerce=True,
                                     checks=Check.ge(0)),
        "gross_item_amount":  Column(float, nullable=True,  coerce=True,
                                     checks=Check.ge(0)),
        "discount_amount":    Column(float, nullable=False, coerce=True,
                                     checks=Check.ge(0)),
        "discount_pct":       Column(float, nullable=True,  coerce=True),
        "item_net_amount":    Column(float, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="GoldFactOrderItems",
)

GoldDimCustomersSchema = DataFrameSchema(
    columns={
        "customer_id": Column(str,         nullable=False, coerce=True),
        "state":       Column(str,         nullable=True,  coerce=True),
        "city":        Column(str,         nullable=True,  coerce=True),
        "created_ts":  Column(pa.DateTime, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="GoldDimCustomers",
)

GoldDimProductsSchema = DataFrameSchema(
    columns={
        "product_id": Column(str,         nullable=False, coerce=True),
        "category":   Column(str,         nullable=True,  coerce=True),
        "brand":      Column(str,         nullable=True,  coerce=True),
        "created_ts": Column(pa.DateTime, nullable=True,  coerce=True),
    },
    strict=False,
    coerce=True,
    name="GoldDimProducts",
)

# ─────────────────────────────────────────────────────────────────────────────
# Lookup dictionaries  (used by pipeline layers to resolve schema by table name)
# ─────────────────────────────────────────────────────────────────────────────

BRONZE_SCHEMAS: dict[str, DataFrameSchema] = {
    "orders":      BronzeOrdersSchema,
    "order_items": BronzeOrderItemsSchema,
    "shipments":   BronzeShipmentsSchema,
    "customers":   BronzeCustomersSchema,
    "products":    BronzeProductsSchema,
}

SILVER_SCHEMAS: dict[str, DataFrameSchema] = {
    "orders":      SilverOrdersSchema,
    "order_items": SilverOrderItemsSchema,
    "shipments":   SilverShipmentsSchema,
    "customers":   SilverCustomersSchema,
    "products":    SilverProductsSchema,
}

GOLD_SCHEMAS: dict[str, DataFrameSchema] = {
    "fact_orders":      GoldFactOrdersSchema,
    "fact_order_items": GoldFactOrderItemsSchema,
    "dim_customers":    GoldDimCustomersSchema,
    "dim_products":     GoldDimProductsSchema,
}
