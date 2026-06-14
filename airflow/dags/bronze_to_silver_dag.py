from __future__ import annotations
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="bronze_to_silver",
    description="Incrementally load new bronze parquet files into silver Iceberg tables",
    schedule="0 22 * * 3-7",
    start_date=datetime(2026, 6, 5),
    catchup=False,
    default_args=default_args,
    tags=["spark", "iceberg", "silver"],
) as dag:

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image="lakehouse_stock_project-spark:latest",  
        api_version="auto",
        auto_remove="success",
        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "--jars /opt/spark/jars/postgresql-42.7.3.jar "
            "/opt/spark/work/bronze_to_silver.py"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="lakehouse_stock_project_my-network",
        environment={
            "AWS_ACCESS_KEY_ID": "minio_admin",
            "AWS_SECRET_ACCESS_KEY": "minio_password",
            "MINIO_ENDPOINT": "http://minio:9000",
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "db",
            "PG_USER": "db_user",
            "PG_PASSWORD": "db_password",
        },
        mounts=[
            Mount(
                source="/home/leduc/repos/lakehouse_stock_project/spark/work/bronze_to_silver.py",  # update to absolute host path containing bronze_to_silver.py
                target="/opt/spark/work",
                type="bind",
            ),
            Mount(
                source="/home/leduc/repos/lakehouse_stock_project/spark/spark-defaults.conf",  # update to absolute host path
                target="/opt/spark/conf/spark-defaults.conf",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
    ) 
    silver_to_gold = DockerOperator(
        task_id="silver_to_gold",
        image="lakehouse_stock_project-spark:latest",  
        api_version="auto",
        auto_remove="success",
        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark:7077 "
            "--jars /opt/spark/jars/postgresql-42.7.3.jar "
            "/opt/spark/work/bronze_to_silver.py"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="lakehouse_stock_project_my-network",
        environment={
            "AWS_ACCESS_KEY_ID": "minio_admin",
            "AWS_SECRET_ACCESS_KEY": "minio_password",
            "MINIO_ENDPOINT": "http://minio:9000",
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "db",
            "PG_USER": "db_user",
            "PG_PASSWORD": "db_password",
        },
        mounts=[
            Mount(
                source="/home/leduc/repos/lakehouse_stock_project/spark/work/silver_to_gold.py",  # update to absolute host path containing bronze_to_silver.py
                target="/opt/spark/work",
                type="bind",
            ),
            Mount(
                source="/home/leduc/repos/lakehouse_stock_project/spark/spark-defaults.conf",  # update to absolute host path
                target="/opt/spark/conf/spark-defaults.conf",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
    ) 
    bronze_to_silver >> silver_to_gold