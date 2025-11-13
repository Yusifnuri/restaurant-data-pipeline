# src/analytics_models.py

from pathlib import Path
import pandas as pd

GOLD_DIR = Path("data/gold")
REPORTS_DIR = Path("reports")


def build_analytics():
    """Create simple analytics outputs from the orders mart."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(GOLD_DIR / "orders_mart.csv")

    # 1) Average order value (using order_total from source system)
    avg_order_value = orders["order_total"].mean()

    # 2) Number of tickets per order (currently num_tickets column)
    #    Later this can be updated when support tickets are ingested.
    tickets_per_order = orders[["order_id", "num_tickets"]]

    # Save metrics to a small CSV
    metrics_df = pd.DataFrame(
        [
            {
                "avg_order_value": avg_order_value,
                "order_count": len(orders),
            }
        ]
    )
    metrics_df.to_csv(REPORTS_DIR / "orders_metrics.csv", index=False)

    tickets_per_order.to_csv(REPORTS_DIR / "tickets_per_order.csv", index=False)

    print("Analytics reports created: orders_metrics.csv, tickets_per_order.csv")


if __name__ == "__main__":
    build_analytics()
