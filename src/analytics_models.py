# src/analytics_models.py

from pathlib import Path
import pandas as pd

GOLD_DIR = Path("data/gold")
SILVER_DIR = Path("data/silver")
REPORTS_DIR = Path("reports/tables")


def ensure_dirs():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def build_orders_metrics():
    """
    Build simple metrics for orders:
      - average order value
      - total number of orders
      - total number of tickets
    """
    df = pd.read_csv(GOLD_DIR / "orders_mart.csv")

    metrics = {
        "total_orders": len(df),
        "average_order_value": df["order_total"].mean(),
        "total_tickets": df["num_tickets"].sum(),
    }

    out_path = REPORTS_DIR / "orders_metrics.csv"
    pd.DataFrame([metrics]).to_csv(out_path, index=False)
    print("orders_metrics.csv created")


def build_tickets_per_order():
    """
    Build a table with ticket counts per order.
    Source: silver stg_tickets.
    """
    tickets = pd.read_csv(SILVER_DIR / "stg_tickets.csv")

    ticket_counts = (
        tickets
        .groupby("order_id", as_index=False)
        .agg(num_tickets=("ticket_id", "count"))
    )

    out_path = REPORTS_DIR / "tickets_per_order.csv"
    ticket_counts.to_csv(out_path, index=False)
    print("tickets_per_order.csv created")


def run_all_analytics():
    ensure_dirs()
    build_orders_metrics()
    build_tickets_per_order()


if __name__ == "__main__":
    run_all_analytics()
