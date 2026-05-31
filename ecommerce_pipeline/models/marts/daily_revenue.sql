{{ config(materialized='table') }}

SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(amount) AS total_revenue
FROM {{ ref('stg_orders') }}
GROUP BY 1
ORDER BY 1 DESC