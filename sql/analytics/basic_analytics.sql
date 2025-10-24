-- shopflow-pipeline/sql/analytics/basic_analytics.sql
-- Iteration 1: Basic analytics queries

-- 1) Top 10 customers by total spending
-- Assumes tables: customers, products, transactions
-- Spending = sum(quantity * price) per customer
WITH order_lines AS (
  SELECT
    t.customer_id,
    t.product_id,
    t.quantity
  FROM transactions t
)
SELECT
  c.id AS customer_id,
  c.name,
  c.email,
  SUM(ol.quantity * p.price) AS total_spent
FROM order_lines ol
JOIN products p ON p.id = ol.product_id
JOIN customers c ON c.id = ol.customer_id
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 10;

-- 2) Best-selling products by category (by quantity sold)
SELECT
  p.category,
  p.id AS product_id,
  p.name AS product_name,
  SUM(t.quantity) AS units_sold
FROM transactions t
JOIN products p ON p.id = t.product_id
GROUP BY p.category, p.id, p.name
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(t.quantity) DESC) <= 5; -- if database doesn't support QUALIFY, use subquery

-- Portable alternative using subquery:
/*
WITH cat_sales AS (
  SELECT
    p.category,
    p.id AS product_id,
    p.name AS product_name,
    SUM(t.quantity) AS units_sold,
    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(t.quantity) DESC) AS rn
  FROM transactions t
  JOIN products p ON p.id = t.product_id
  GROUP BY p.category, p.id, p.name
)
SELECT * FROM cat_sales WHERE rn <= 5;
*/

-- 3) Monthly revenue trends (sum of quantity*price per month)
WITH trx_value AS (
  SELECT
    DATE_TRUNC('month', t.timestamp::timestamp) AS month,
    SUM(t.quantity * p.price) AS revenue
  FROM transactions t
  JOIN products p ON p.id = t.product_id
  GROUP BY 1
)
SELECT month, revenue
FROM trx_value
ORDER BY month;

-- 4) Average order value (AOV) by country
-- AOV = average(value per transaction) grouped by customer country
WITH per_tx_value AS (
  SELECT
    t.id AS transaction_id,
    c.country,
    SUM(t.quantity * p.price) AS tx_value
  FROM transactions t
  JOIN customers c ON c.id = t.customer_id
  JOIN products p ON p.id = t.product_id
  GROUP BY t.id, c.country
)
SELECT
  country,
  AVG(tx_value) AS avg_order_value
FROM per_tx_value
GROUP BY country
ORDER BY avg_order_value DESC;
