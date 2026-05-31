import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# 1. Đọc cấu hình từ biến môi trường (Bảo mật & Linh hoạt 100%)
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
REGION = os.getenv("GCP_REGION", "us-central1") # Có giá trị mặc định nếu quên khai báo trong .env
ZONE = os.getenv("GCP_ZONE", "us-central1-a")
CLUSTER_NAME = os.getenv("DATAPROC_CLUSTER_NAME", "ephemeral-spark-cluster-airflow")

# Kiểm tra an toàn: Dừng DAG ngay nếu chưa load được file .env
if not PROJECT_ID or not BUCKET_NAME:
    raise ValueError("LỖI NGHIÊM TRỌNG: Chưa cấu hình GCP_PROJECT_ID hoặc GCS_BUCKET_NAME trong file .env!")

# Đường dẫn file code PySpark gốc anh em mình đã ném lên GCS 
PYSPARK_SCRIPT_URI = f"gs://{BUCKET_NAME}/scripts/bronze_to_silver.py"

# 2. Cấu hình "Cơ bắp phần cứng" chuẩn Sinh Viên (Vừa khít Quota Google)
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2", # Ép xung xuống 2 vCPU, 8GB RAM
        "disk_config": {"boot_disk_size_gb": 30},
    },
    "worker_config": {"num_instances": 0}, # Chơi hệ Single Node để tiết kiệm
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            # Khai báo danh mục Apache Iceberg với con Spark trên Cloud
            "spark:spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
            "spark:spark.sql.catalog.demo.type": "hadoop",
            "spark:spark.sql.catalog.demo.warehouse": f"gs://{BUCKET_NAME}/silver",
            "spark:spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1"
        }
    }
}

# 3. Cấu hình Job PySpark sẽ nạp vào cụm máy để thực thi
PYSPARK_JOB_CONFIG = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_SCRIPT_URI},
}

# 4. Cấu hình cài đặt mặc định (Lưới an toàn cho hệ thống)
default_args = {
    "owner": "TungDang",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1, 
    "retry_delay": timedelta(minutes=5),
}

# 5. Khai sinh con DAG: Lập lịch đúng 12 giờ đêm (0 0 * * *) chạy tự động hàng ngày
with DAG(
    dag_id="e2e_lakehouse_orchestration",
    default_args=default_args,
    description="Tu dong hoa Pipeline Spark Iceberg bang Ephemeral Dataproc Cluster",
    schedule="0 0 * * *", 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data_lakehouse"],
) as dag:

    # TASK 1: Ra lệnh cho GCP tự động dựng cụm máy e2-standard-2
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # TASK 2: Tự động nạp job PySpark tiến hóa cấu trúc bảng Iceberg vào chạy
    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        job=PYSPARK_JOB_CONFIG,
        region=REGION,
    )

    # TASK 3: CHIẾN THUẬT FINOPS - Bất kể Task 2 chạy xong hay lỗi, LẬP TỨC XÓA CỤM
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
    )

    # 🔗 THIẾT LẬP DÒNG CHẢY LOGIC: Dựng máy -> Chạy Job -> Xóa máy
    create_dataproc_cluster >> submit_spark_job >> delete_dataproc_cluster