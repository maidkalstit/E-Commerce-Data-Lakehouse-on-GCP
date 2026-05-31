{{ config(materialized='view') }}

SELECT
    transaction_id AS order_id,
    customer_id,
    product_id,
    CAST(amount AS FLOAT64) AS amount,
    CAST(quantity AS INT64) AS quantity,
    -- Đổi tên thành event_timestamp cho khớp 100% với BigQuery
    event_timestamp AS transaction_timestamp,
    DATE(event_timestamp) AS order_date
FROM {{ source('silver_data', 'stg_transactions_silver') }}