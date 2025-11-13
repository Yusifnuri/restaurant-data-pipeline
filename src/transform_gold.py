# src/transform_gold.py

from pathlib import Path
import pandas as pd

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def ensure_dirs():
    """Create gold directory if it does not exist."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)


def build_orders_mart():
    """
    Build the main orders mart by joining orders, customers, stores,
    items, and tickets.

    Input:
        data/silver/stg_orders.csv
        data/silver/stg_customers.csv
        data/silver/stg_stores.csv
        data/silver/stg_items.csv
        data/silver/stg_tickets.csv

    Output:
        data/gold/orders_mart.csv
    """
    # Base tables
    orders = pd.read_csv(SILVER_DIR / "stg_orders.csv")
    customers = pd.read_csv(SILVER_DIR / "stg_customers.csv")
    stores = pd.read_csv(SILVER_DIR / "stg_stores.csv")

    # Join orders → customers → stores
    df = (
        orders
        .merge(customers, left_on="customer_ref", right_on="customer_id", how="left")
        .merge(stores, on="store_id", how="left")
    )

    # Number of items per order
    items_path = SILVER_DIR / "stg_items.csv"
    if items_path.exists():
        items = pd.read_csv(items_path)
        items_counts = (
            items
            .groupby("order_id", as_index=False)
            .agg(number_of_items=("item_id", "count"))
        )
        df = df.merge(items_counts, on="order_id", how="left")
    else:
        df["number_of_items"] = 0

    # Number of tickets per order
    tickets_path = SILVER_DIR / "stg_tickets.csv"
    if tickets_path.exists():
        tickets = pd.read_csv(tickets_path)
        ticket_counts = (
            tickets
            .groupby("order_id", as_index=False)
            .agg(num_tickets=("ticket_id", "count"))
        )
        df = df.merge(ticket_counts, on="order_id", how="left")
    else:
        df["num_tickets"] = 0

    # Fill missing counts with 0 and cast to int
    if "number_of_items" in df.columns:
        df["number_of_items"] = df["number_of_items"].fillna(0).astype("int64")
    if "num_tickets" in df.columns:
        df["num_tickets"] = df["num_tickets"].fillna(0).astype("int64")

    # Computed total as a simple check
    df["computed_total"] = df["subtotal"] + df["tax_paid"]

    # Optional: order columns for readability (you vermişdin bir dəfə)
    column_order = [
        "order_id",
        "customer_ref",
        "customer_id",
        "customer_name",
        "ordered_at",
        "store_id",
        "store_name",
        "opened_at",
        "tax_rate",
        "subtotal",
        "tax_paid",
        "order_total",
        "computed_total",
        "number_of_items",
        "num_tickets",
    ]
    # Keep only columns that actually exist
    column_order = [c for c in column_order if c in df.columns]
    df = df[column_order]

    # Write output
    ensure_dirs()
    out_path = GOLD_DIR / "orders_mart.csv"
    df.to_csv(out_path, index=False)
    print("orders_mart.csv created")


def run_all_gold():
    """Run all gold transformations."""
    ensure_dirs()
    build_orders_mart()


if __name__ == "__main__":
    run_all_gold()
