import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

def main():
    # 1. Khởi tạo Spark Session + Tối ưu hóa bộ nhớ RAM bằng Kryo Serializer
    # Giúp nén các đối tượng trong RAM nhỏ hơn 10 lần, chống lỗi nghẹt bộ nhớ Code 143
    spark = SparkSession.builder \
        .appName("TungDang-BronzeToSilver-Iceberg") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    print("🚀--- Pipeline Khởi Chạy: Bronze sang Silver (Apache Iceberg) ---")

    # 2. Định nghĩa Schema chặt chẽ cho dữ liệu thô ban đầu
    raw_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("discount_code", StringType(), True),    # 🔥 Đòi lại cột giảm giá
        StructField("payment_method", StringType(), True)
    ])

    # 3. Đọc dữ liệu thô (JSON Lines) từ rổ GCS Bronze bằng Wildcard Pattern (*/*/*/*)
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("GCS_BUCKET_NAME not configured!") 
    bronze_path = f"gs://{bucket_name}/bronze/*/*/*/*.json"
    
    print(f"📥 Đang quét và đọc toàn bộ dữ liệu thô từ: {bronze_path}")
    df_raw = spark.read.schema(raw_schema).json(bronze_path)

    # Kiểm tra phòng hờ nếu chưa có dữ liệu thô nào
    if df_raw.isEmpty():
        print("⚠️ Không có dữ liệu mới tại tầng Bronze. Dừng job")
        spark.stop()
        sys.exit(0)

    # 4. Làm sạch và chuẩn hóa dữ liệu (Transform)
    df_silver = df_raw \
        .withColumn("transaction_id", col("transaction_id").cast(StringType())) \
        .withColumn("amount", when(col("amount") >= 0, col("amount")).otherwise(0.0)) \
        .withColumn("event_timestamp", to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("ingested_at", current_timestamp()) \
        .drop("transaction_date")

    # Lọc trùng lặp dựa trên mã giao dịch
    df_silver_cleaned = df_silver.dropDuplicates(["transaction_id"])

    # Sử dụng spark.catalog.tableExists để kiểm tra xem bảng Iceberg đã được tạo trước đó chưa
# 5. Ghi dữ liệu xuống tầng Silver dưới định dạng Apache Iceberg (Hỗ trợ Schema Evolution)
    iceberg_table = "demo.silver_sales.transactions"
    print(f"💾 Đang tiến hành xử lý ghi dữ liệu vào bảng Iceberg: {iceberg_table}")
    
    # [TƯ DUY PRODUCTION CHUẨN]: Dùng try-except để chấp mọi loại Metastore lơ mơ!
    try:
        # Thử ghi dữ liệu theo kiểu APPEND (Bơm thêm) trước
        print(f"🔄 Thử tiến hành APPEND dữ liệu mới và tự động gộp cấu trúc tiến hóa...")
        df_silver_cleaned.write \
            .format("iceberg") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(iceberg_table)
    except Exception as e:
        # Nếu bảng thực sự chưa từng tồn tại trong lịch sử, con Iceberg dưới GCS sẽ báo lỗi, lúc đó mình mới CREATE
        print(f"🆕 Bảng chưa tồn tại thực sự dưới GCS. Tiến hành khởi tạo bảng Iceberg V2 lần đầu tiên...")
        df_silver_cleaned.writeTo(iceberg_table) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("history.expire.max-snapshot-age-ms", "604800000") \
            .create()

    print("✨--- Pipeline Thành Công: Dữ liệu tại tầng Silver Iceberg! ---")
    spark.stop()

if __name__ == "__main__":
    main()