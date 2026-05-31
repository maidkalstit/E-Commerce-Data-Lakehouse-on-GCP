import os
import json
import random
import time
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

# Tự xây dựng hàm sinh UUIDv7bằng Python thuần
# Giúp tối ưu hóa cấu trúc cây B-Tree Index và Z-Order vật lý trên Data Lakehouse
def generate_uuidv7():
    """Sinh chuỗi UUIDv7 dựa trên Timestamp (millisecond) + Random bits"""
    # 48 bits đầu tiên: Thời gian hiện tại tính bằng milliseconds
    ms = int(time.time() * 1000)
    
    # Sinh các chuỗi bytes ngẫu nhiên cho phần còn lại
    rand_bytes = os.urandom(10)
    
    # Bóc tách các phân đoạn theo đặc tả chuẩn UUIDv7
    time_high_and_version = (ms & 0x0FFF) | 0x7000 # Version 7
    clock_seq_and_variant = (rand_bytes[0] & 0x3F) | 0x80 # Variant 1
    
    # Lắp ghép thành định dạng chuỗi UUID tiêu chuẩn 8-4-4-4-12
    uuid_str = (
        f"{(ms >> 16) & 0xFFFFFFFF:08x}-"
        f"{ms & 0xFFFF:04x}-"
        f"{time_high_and_version:04x}-"
        f"{clock_seq_and_variant:02x}{rand_bytes[1]:02x}-"
        f"{rand_bytes[2]:02x}{rand_bytes[3]:02x}{rand_bytes[4]:02x}"
        f"{rand_bytes[5]:02x}{rand_bytes[6]:02x}{rand_bytes[7]:02x}"
    )
    return uuid_str

# Hàm sinh tên sản phẩm giả lập đơn giản thay thế thư viện Faker để chạy siêu tốc
def get_mock_product_name(category):
    prefixes = ['Ultra', 'Smart', 'Eco', 'Premium', 'Pro', 'Neo']
    suffixes = ['X', 'Hub', 'Line', 'Max', 'Plus', 'Wave']
    return f"{random.choice(prefixes)} {category} {random.choice(suffixes)}"

def generate_product_catalog(num_products=1000):
    """Sinh ra danh mục sản phẩm khổng lồ với ID là UUIDv7 (Sắp xếp theo thời gian) 💎"""
    catalog = []
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Beauty']
    for _ in range(num_products):
        category = random.choice(categories)
        catalog.append({
            "product_id": generate_uuidv7(), # 🔥 ĐÃ ĐỔI SANG UUIDv7 CHUẨN TIME-ORDERED
            "product_name": get_mock_product_name(category),
            "category": category,
            "base_price": round(random.uniform(10.0, 1500.0), 2)
        })
    return catalog

def generate_mock_orders(catalog, num_orders=500):
    """Sinh đơn hàng tăng trưởng - Sử dụng UUIDv7 cho ID giao dịch để tối ưu Index 🛒🚀"""
    orders = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    payment_methods = ['Credit Card', 'E-Wallet', 'Bank Transfer', 'COD']
    discounts = ['SUMMER26', 'DE_FRESHER', 'ICEBERG_SUPER', None, None]
    
    for _ in range(num_orders):
        product = random.choice(catalog)
        quantity = random.randint(1, 5)
        amount = round(product["base_price"] * quantity, 2)
        
        # Tạo chuỗi UUIDv7 đầy đủ và cắt 8 ký tự đầu để làm mã giao dịch tăng dần đẹp mắt
        u7 = generate_uuidv7()
        
        order = {
            "transaction_id": f"TXN-{u7[:8].upper()}", # 🔥 Giao dịch giờ có tính chất tăng dần theo thời gian
            "customer_id": f"CUST-{random.randint(10000, 99999)}",
            "product_id": product["product_id"], # Link sang UUIDv7 của Catalog sản phẩm
            "amount": amount,
            "quantity": quantity,
            "transaction_date": current_time,
            "discount_code": random.choice(discounts),       
            "payment_method": random.choice(payment_methods)   
        }
        orders.append(order)
    return orders

def upload_to_gcs(bucket_name, data):
    """Hàm ném file JSON lên tầng GCS Bronze """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    now = datetime.now()
    blob_name = f"bronze/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/orders_uuidv7_{now.strftime('%H%M%S')}.json"
    
    blob = bucket.blob(blob_name)
    json_lines = "\n".join([json.dumps(record) for record in data])
    
    print(f"🚀 [PRODUCTION] Đang tải {len(data)} đơn hàng cấu trúc UUIDv7 tối ưu lên GCS: gs://{bucket_name}/{blob_name}")
    blob.upload_from_string(json_lines, content_type="application/json")

def main():
    load_dotenv()
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("LỖI: Chưa khai báo GCS_BUCKET_NAME trong file .env!") 
    
    catalog = generate_product_catalog(num_products=1000)
    num_orders = random.randint(6700, 10000)
    mock_data = generate_mock_orders(catalog, num_orders=num_orders)
    
    upload_to_gcs(bucket_name, mock_data)
    print("Dữ liệu đã lên GCS Bronze!")

if __name__ == "__main__":
    main()