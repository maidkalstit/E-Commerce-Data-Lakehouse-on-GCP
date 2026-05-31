# ==============================================================================
# INFRASTRUCTURE AS CODE (IaC) - TUNG DANG ENTERPRISE
# Lệnh chạy: terraform init -> terraform plan -> terraform apply
# ==============================================================================

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0" # Sử dụng provider mới nhất
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = "us-central1"
}

variable "gcp_project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "gcs_bucket_name" {
  type        = string
  description = "Data Lake Bucket Name"
}

# 1. Tự động khởi tạo GCS Bucket (Data Lake - Bronze & Silver)
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = "US"
  force_destroy = true
  storage_class = "STANDARD"

  lifecycle_rule {
    condition {
      age = 30 # Tự động dọn dẹp file nháp sau 30 ngày
    }
    action {
      type = "Delete"
    }
  }
}

# 2. Tự động khởi tạo BigQuery Dataset (Gold Layer)
resource "google_bigquery_dataset" "gold_layer" {
  dataset_id                 = "gold_sales"
  location                   = "US"
  delete_contents_on_destroy = true
}