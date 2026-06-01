import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

PROJECT_ID = "project-86c0fe67-3e32-4439-b05"
REGION = "us-central1"
ZONE = "us-central1-a"
CLUSTER_NAME = "ephemeral-spark-cluster-airflow"
BUCKET_NAME = "tung-dang-data-lake-2026"
PYSPARK_SCRIPT_URI = f"gs://{BUCKET_NAME}/scripts/bronze_to_silver.py"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_size_gb": 30},
    },
    "worker_config": {"num_instances": 0},
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
            "spark:spark.sql.catalog.demo.type": "hadoop",
            "spark:spark.sql.catalog.demo.warehouse": f"gs://{BUCKET_NAME}/silver",
            "spark:spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1"
        }
    }
}

PYSPARK_JOB_CONFIG = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_SCRIPT_URI},
}

default_args = {
    "owner": "TungDang",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="e2e_lakehouse_orchestration",
    default_args=default_args,
    description="End-to-End Pipeline: Gen Data -> Build Cluster -> Run Spark -> Delete Cluster",
    schedule="0 0 * * *", 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data_lakehouse", "end_to_end"],
) as dag:

    # TASK 0: Đẻ dữ liệu
    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command="python ~/airflow/dags/data_generator.py",
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        job=PYSPARK_JOB_CONFIG,
        region=REGION,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
    )

    # Nối 4 khối với nhau
    generate_raw_data >> create_dataproc_cluster >> submit_spark_job >> delete_dataproc_cluster