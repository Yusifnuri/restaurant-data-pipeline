# src/transform_silver.py

from pathlib import Path
import pandas as pd

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")


def ensure_dirs():
    """Create silver directory if it does not exist."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)


def build_stg_customers():
    """Transform raw_customers → stg_customers."""
    df = pd.read_csv(BRONZE_DIR / "raw_customers.csv")

    # Standardize column names
    df = df.rename(columns={
        "id": "customer_id",
        "name": "customer_name"
    })

    df.to_csv(SILVER_DIR / "stg_customers.csv", index=False)
    print("stg_customers.csv created")


def build_stg_orders():
    """Transform raw_orders → stg_orders."""
    df = pd.read_csv(BRONZE_DIR / "raw_orders.csv")

    # Rename columns to standard naming
    df = df.rename(columns={
        "id": "order_id",
        "customer": "customer_ref"
    })

    # Convert order timestamp
    df["ordered_at"] = pd.to_datetime(df["ordered_at"], errors="coerce")

    df.to_csv(SILVER_DIR / "stg_orders.csv", index=False)
    print("stg_orders.csv created")


def build_stg_items():
    """Transform raw_items → stg_items."""
    df = pd.read_csv(BRONZE_DIR / "raw_items.csv")

    df = df.rename(columns={
        "id": "item_id"
    })

    df.to_csv(SILVER_DIR / "stg_items.csv", index=False)
    print("stg_items.csv created")


def build_stg_products():
    """Transform raw_products → stg_products."""
    df = pd.read_csv(BRONZE_DIR / "raw_products.csv")

    df = df.rename(columns={
        "name": "product_name",
        "type": "product_type"
    })

    # Convert price to numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df.to_csv(SILVER_DIR / "stg_products.csv", index=False)
    print("stg_products.csv created")


def build_stg_stores():
    """Transform raw_stores → stg_stores."""
    df = pd.read_csv(BRONZE_DIR / "raw_stores.csv")

    df = df.rename(columns={
        "id": "store_id",
        "name": "store_name"
    })

    df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce")

    df.to_csv(SILVER_DIR / "stg_stores.csv", index=False)
    print("stg_stores.csv created")


def build_stg_supplies():
    """Transform raw_supplies → stg_supplies."""
    df = pd.read_csv(BRONZE_DIR / "raw_supplies.csv")

    df = df.rename(columns={
        "id": "supply_id",
        "name": "supply_name"
    })

    df.to_csv(SILVER_DIR / "stg_supplies.csv", index=False)
    print("stg_supplies.csv created")


def build_stg_tickets():
    """
    Transform support_tickets.jsonl → stg_tickets.

    Input:
        data/bronze/support_tickets.jsonl

    Output:
        data/silver/stg_tickets.csv
    """
    src_path = BRONZE_DIR / "support_tickets.jsonl"
    df = pd.read_json(src_path, lines=True)

    # Keep only relevant columns for joins and basic analytics
    keep_cols = [
        "ticket_id",
        "order_id",
        "customer_external_id",
        "channel",
        "priority",
        "status",
        "category",
        "sentiment",
        "sla_due_at",
        "first_response_at",
        "resolved_at",
        "agent_id",
        "updated_at",
        "ingested_at",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].copy()

    # Drop rows without ticket_id or order_id
    df = df.dropna(subset=["ticket_id", "order_id"])

    # Parse datetime columns
    datetime_cols = [
        "sla_due_at",
        "first_response_at",
        "resolved_at",
        "updated_at",
        "ingested_at",
    ]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df.to_csv(SILVER_DIR / "stg_tickets.csv", index=False)
    print("stg_tickets.csv created")


def run_all_silver():
    """Run all silver transformations in correct order."""
    ensure_dirs()
    build_stg_customers()
    build_stg_orders()
    build_stg_items()
    build_stg_products()
    build_stg_stores()
    build_stg_supplies()
    build_stg_tickets()  # tickets staging added


if __name__ == "__main__":
    run_all_silver()
