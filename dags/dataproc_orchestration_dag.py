"""
==============================================================================
E-COMMERCE DATA LAKEHOUSE - DATAPROC ORCHESTRATION DAG
==============================================================================
Description: Airflow DAG to orchestrate the Ephemeral Dataproc cluster, 
             run PySpark Bronze-to-Silver transformation, and auto-terminate.
==============================================================================
"""

import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# ==============================================================================
# 1. CẤU HÌNH BIẾN MÔI TRƯỜNG & THÔNG SỐ GCP
# ==============================================================================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-86c0fe67-3e32-4439-b05")
REGION = os.getenv("GCP_REGION", "us-central1")
CLUSTER_NAME = "ecommerce-spark-cluster-{{ ds_nodash }}"
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "your-data-lake-bucket-name")

# Đường dẫn trỏ tới file PySpark trên GCS (Tương đương thư mục src/ trên local)
PYSPARK_SCRIPT_URI = f"gs://{BUCKET_NAME}/scripts/bronze_to_silver.py"

# Cấu hình cụm Dataproc (Ephemeral Cluster)
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
}

# Cấu hình PySpark Job (Kèm theo thư viện Apache Iceberg)
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_SCRIPT_URI,
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
        ],
        "properties": {
            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hive",
        },
    },
}

# ==============================================================================
# 2. HÀM CẢNH BÁO TELEGRAM KHI CÓ LỖI (ERROR HANDLING)
# ==============================================================================
def telegram_alert_on_failure(context):
    """Bắn thông báo qua Telegram khi DAG thất bại."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")
    
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    
    message = f"""
    🚨 <b>CẢNH BÁO SẬP PIPELINE!</b> 🚨
    - <b>DAG:</b> {dag_id}
    - <b>Task Lỗi:</b> {task_id}
    - <b>Thời gian:</b> {execution_date}
    - <b>Tình trạng:</b> Hệ thống đang gặp sự cố, cần kiểm tra ngay! 🛠️
    """
    
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=5)
        print("Đã gửi tín hiệu SOS lên Telegram!")
    except Exception as e:
        print(f"Không thể gửi Telegram Alert: {e}")

# ==============================================================================
# 3. KHỞI TẠO DAG VÀ CÁC TASKS
# ==============================================================================
default_args = {
    "owner": "TungDang",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": telegram_alert_on_failure, # Gắn hàm cảnh báo tự động
}

with DAG(
    "ecommerce_dataproc_orchestration",
    default_args=default_args,
    description="Orchestrate Ephemeral Dataproc for Bronze-to-Silver ETL",
    schedule_interval="0 2 * * *", # Chạy lúc 2h sáng mỗi ngày
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["ecommerce", "dataproc", "pyspark", "iceberg"],
) as dag:

    # Task 1: Khởi động luồng (Dummy Task)
    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    # Task 2: Tạo Ephemeral Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # Task 3: Đẩy PySpark Job lên Cluster để chạy
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_bronze_to_silver_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task 4: Xóa cụm để tiết kiệm tiền (Luôn chạy kể cả khi Task 3 lỗi)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done", # 🔥 Chìa khóa vàng giải quyết Cost Overrun
    )

    # ==============================================================================
    # 4. ĐỊNH NGHĨA THỨ TỰ CHẠY (PIPELINE DEPENDENCIES)
    # ==============================================================================
    chain(start_pipeline, create_cluster, submit_pyspark_job, delete_cluster)