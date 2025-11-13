# dagster_project/repository.py

from dagster import repository
from .etl_job import restaurant_etl_job


@repository
def restaurant_repo():
    return [restaurant_etl_job]
