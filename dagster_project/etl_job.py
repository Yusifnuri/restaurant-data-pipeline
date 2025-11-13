from dagster import op, job
import subprocess
import sys


@op
def silver_step():
    print("Running Silver Layer...")
    cmd = [sys.executable, "-m", "src.transform_silver"]
    result = subprocess.run(cmd)
    print("Silver Layer Finished")
    return result.returncode


@op
def gold_step(_prev_result):
    print("Running Gold Layer...")
    cmd = [sys.executable, "-m", "src.transform_gold"]
    result = subprocess.run(cmd)
    print("Gold Layer Finished")
    return result.returncode


@op
def analytics_step(_prev_result):
    print("Running Analytics Models...")
    cmd = [sys.executable, "-m", "src.analytics_models"]
    result = subprocess.run(cmd)
    print("Analytics Finished")
    return result.returncode


@job
def etl_pipeline():
    analytics_step(gold_step(silver_step()))


if __name__ == "__main__":
    print("Executing Dagster ETL Pipeline...")
    res = etl_pipeline.execute_in_process()
    print("Pipeline Success:", res.success)
