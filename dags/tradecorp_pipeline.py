import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


PROJECT_PATH = "//c/Users/Utilisateur/Desktop/simplon-project-data/tradecorp-data-platform"
AZURE_ENV = {
    "AZURE_STORAGE_KEY": os.getenv("AZURE_STORAGE_KEY"),
    "AZURE_STORAGE_ACCOUNT": os.getenv("AZURE_STORAGE_ACCOUNT"),
    "AZURE_STORAGE_URL": os.getenv("AZURE_STORAGE_URL"),
    "AZURE_CONTAINER_RAW": os.getenv("AZURE_CONTAINER_RAW"),
    "AZURE_CONTAINER_CLEAN": os.getenv("AZURE_CONTAINER_CLEAN"),
}

mounts = [
    Mount(
        source=f"{PROJECT_PATH}/src",
        target="/home/jovyan/src",
        type="bind",
    ),
    Mount(
        source=f"{PROJECT_PATH}/data",
        target="/home/jovyan/data",
        type="bind",
    )
]


default_args = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="tradecorp_etl_pipeline",
    description="Pipeline ETL TradeCorp — orchestré via Airflow",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["tradecorp-data-platform", "etl", "spark"],
) as dag:

    fetch_exchange_rates = DockerOperator(
    task_id="fetch_exchange_rates",
    image="tradecorp-data-platform",
    docker_url="unix://var/run/docker.sock",
    network_mode="tradecorp-data-platform_default",
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=mounts,
    command="spark-submit /home/jovyan/src/fetch_exchange_rates.py",
    environment=AZURE_ENV,
)

reader = DockerOperator(
    task_id="reader",
    image="tradecorp-data-platform",
    docker_url="unix://var/run/docker.sock",
    network_mode="tradecorp-data-platform_default",
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=mounts,
    command="spark-submit /home/jovyan/src/reader.py",
    environment=AZURE_ENV,
)

transformer = DockerOperator(
    task_id="transformer",
    image="tradecorp-data-platform",
    docker_url="unix://var/run/docker.sock",
    network_mode="tradecorp-data-platform_default",
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=mounts,
    command="spark-submit /home/jovyan/src/transformer.py",
    environment=AZURE_ENV,
)

writer = DockerOperator(
    task_id="writer",
    image="tradecorp-data-platform",
    docker_url="unix://var/run/docker.sock",
    network_mode="tradecorp-data-platform_default",
    auto_remove=True,
    mount_tmp_dir=False,
    mounts=mounts,
    command="spark-submit /home/jovyan/src/writer.py",
    environment=AZURE_ENV,
)

fetch_exchange_rates >> reader >> transformer >> writer

