-- Top customers by total spending
CREATE OR REPLACE VIEW public.v_top_customers AS
WITH order_lines AS (
  SELECT t.customer_id, t.product_id, t.quantity
  FROM public.transactions t
)
SELECT c.id AS customer_id, c.name, c.email,
       SUM(ol.quantity * p.price) AS total_spent
FROM order_lines ol
JOIN public.products  p ON p.id = ol.product_id
JOIN public.customers c ON c.id = ol.customer_id
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC;

-- Best-sellers by category (Postgres-friendly: no QUALIFY)
CREATE OR REPLACE VIEW public.v_best_sellers_by_category AS
WITH cat_sales AS (
  SELECT p.category,
         p.id AS product_id,
         p.name AS product_name,
         SUM(t.quantity) AS units_sold,
         ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(t.quantity) DESC) AS rn
  FROM public.transactions t
  JOIN public.products p ON p.id = t.product_id
  GROUP BY p.category, p.id, p.name
)
SELECT category, product_id, product_name, units_sold
FROM cat_sales
WHERE rn <= 5;

-- Monthly revenue
CREATE OR REPLACE VIEW public.v_monthly_revenue AS
WITH trx_value AS (
  SELECT DATE_TRUNC('month', t."timestamp") AS month,
         SUM(t.quantity * p.price) AS revenue
  FROM public.transactions t
  JOIN public.products p ON p.id = t.product_id
  GROUP BY 1
)
SELECT month, revenue
FROM trx_value
ORDER BY month;

-- Average order value by country
CREATE OR REPLACE VIEW public.v_aov_by_country AS
WITH per_tx_value AS (
  SELECT t.id AS transaction_id, c.country,
         SUM(t.quantity * p.price) AS tx_value
  FROM public.transactions t
  JOIN public.customers c ON c.id = t.customer_id
  JOIN public.products  p ON p.id = t.product_id
  GROUP BY t.id, c.country
)
SELECT country, AVG(tx_value) AS avg_order_value
FROM per_tx_value
GROUP BY country
ORDER BY avg_order_value DESC;
