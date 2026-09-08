from airflow  import DAG
from datetime import datetime, timedelta

default_arg ={
    "owner": "tradecorp",
    "retries":1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="tradecorp_etl_pipeline",
    description="Pipeline ETL TradeCorp — orchestré via Airflow",
    default_args=default_arg,
    start_date=datetime(2024,1,1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["tradecorp","etl","spark"],
) as dag:
    pass