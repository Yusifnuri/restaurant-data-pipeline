# src/transform_gold.py

from pathlib import Path
import pandas as pd

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def ensure_gold_dir():
    """Ensure the gold directory exists."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)


def build_orders_with_customers():
    """
    Join orders with customers using UUID keys:
    stg_orders.customer_ref → stg_customers.customer_id
    """
    orders = pd.read_csv(SILVER_DIR / "stg_orders.csv")
    customers = pd.read_csv(SILVER_DIR / "stg_customers.csv")

    merged = orders.merge(
        customers,
        left_on="customer_ref",
        right_on="customer_id",
        how="left"
    )

    return merged


def build_orders_with_totals():
    """
    Compute order totals using items and products.
    Since there is no quantity column, assume quantity = 1.
    """
    items = pd.read_csv(SILVER_DIR / "stg_items.csv")
    products = pd.read_csv(SILVER_DIR / "stg_products.csv")

    merged = items.merge(
        products[["sku", "price", "product_name", "product_type"]],
        on="sku",
        how="left"
    )

    merged["quantity"] = 1
    merged["line_total"] = merged["quantity"] * merged["price"]

    order_totals = (
        merged.groupby("order_id", as_index=False)
        .agg(
            computed_total=("line_total", "sum"),
            number_of_items=("item_id", "count")
        )
    )

    return order_totals


def build_orders_mart():
    """Build the final orders mart table with customer, store, and totals."""
    ensure_gold_dir()

    orders = build_orders_with_customers()
    stores = pd.read_csv(SILVER_DIR / "stg_stores.csv")
    totals = build_orders_with_totals()

    orders = orders.merge(totals, on="order_id", how="left")
    orders = orders.merge(stores, on="store_id", how="left")

    # Placeholder for tickets (future extension)
    orders["num_tickets"] = 0

    orders.to_csv(GOLD_DIR / "orders_mart.csv", index=False)
    print("orders_mart.csv created")


def run_all_gold():
    build_orders_mart()


if __name__ == "__main__":
    run_all_gold()
