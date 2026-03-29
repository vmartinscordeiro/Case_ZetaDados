from pathlib import Path

BASE_DIR     = Path(__file__).resolve().parent.parent
DATA_DIR     = BASE_DIR / "data"
ARCHIVES_DIR = BASE_DIR / "Archives"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR   = DATA_DIR / "gold"

# Business rule: deliveries exceeding this threshold are considered late
LATE_DELIVERY_HOURS: int = 72

# All raw table names
TABLE_NAMES = ["orders", "order_items", "shipments", "customers", "products"]

# Primary keys used for deduplication in Silver
PRIMARY_KEYS: dict[str, list[str]] = {
    "orders":      ["order_id"],
    "order_items": ["order_id", "product_id"],
    "shipments":   ["order_id"],
    "customers":   ["customer_id"],
    "products":    ["product_id"],
}

# Gold tables to build
GOLD_TABLES = ["fact_orders", "fact_order_items", "dim_customers", "dim_products"]
