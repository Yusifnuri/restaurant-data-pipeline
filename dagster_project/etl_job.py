# dagster_project/etl_job.py

from dagster import job, op
import subprocess
import sys


# -------------------------
# Extract Layer
# -------------------------
@op
def extract_layer():
    print("Running Extract Layer...")
    cmd = [sys.executable, "-m", "src.extract_tickets"]
    subprocess.run(cmd, check=True)
    print("Extract Layer Finished")
    return "extract_done"


# -------------------------
# Silver Layer
# -------------------------
@op
def silver_layer(_extract_done):
    print("Running Silver Layer...")
    cmd = [sys.executable, "-m", "src.transform_silver"]
    subprocess.run(cmd, check=True)
    print("Silver Layer Finished")
    return "silver_done"


# -------------------------
# Gold Layer
# -------------------------
@op
def gold_layer(_silver_done):
    print("Running Gold Layer...")
    cmd = [sys.executable, "-m", "src.transform_gold"]
    subprocess.run(cmd, check=True)
    print("Gold Layer Finished")
    return "gold_done"


# -------------------------
# Analytics Layer
# -------------------------
@op
def analytics_layer(_gold_done):
    print("Running Analytics Models...")
    cmd = [sys.executable, "-m", "src.analytics_models"]
    subprocess.run(cmd, check=True)
    print("Analytics Layer Finished")


# -------------------------
# Job DAG
# -------------------------
@job
def restaurant_etl_job():
    """Correct dependency order: Extract → Silver → Gold → Analytics"""
    analytics_layer(
        gold_layer(
            silver_layer(
                extract_layer()
            )
        )
    )


# -------------------------
# Local CLI execution
# -------------------------
if __name__ == "__main__":
    print("Executing Dagster ETL Pipeline...")
    result = restaurant_etl_job.execute_in_process()
    print("Pipeline Success:", result.success)
