{{ config(materialized='table') }}

SELECT
    product_id,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_quantity_sold,
    SUM(amount) AS total_revenue
FROM {{ ref('stg_orders') }}
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 20