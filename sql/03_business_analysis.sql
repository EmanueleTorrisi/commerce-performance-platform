-- Targeted queries to answer business questions and power Power BI visuals.

-- -----------------------------------
-- Sales Trends & growth
-- -----------------------------------

-- 1) Monthly sales, with month-over-month (MoM) and year-over-year (YoY) growth
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date)::date AS month,
        SUM(sales) AS revenue,
        SUM(profit) AS profit,
        COUNT(DISTINCT order_id) AS orders_count
    FROM fact_sales
    GROUP BY 1
)
SELECT
    month,
    revenue,
    profit,
    orders_count,
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    CASE WHEN LAG(revenue) OVER (ORDER BY month) IS NULL THEN NULL
         ELSE ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month)) / NULLIF(LAG(revenue) OVER (ORDER BY month),0),2) END AS mom_pct,
    -- compare to same month last year
    LAG(revenue, 12) OVER (ORDER BY month) AS revenue_last_year,
    CASE WHEN LAG(revenue, 12) OVER (ORDER BY month) IS NULL THEN NULL
         ELSE ROUND(100.0 * (revenue - LAG(revenue,12) OVER (ORDER BY month)) / NULLIF(LAG(revenue,12) OVER (ORDER BY month),0),2) END AS yoy_pct
FROM monthly
ORDER BY month;

-- 2) Cumulative sales trend
SELECT month, SUM(revenue) OVER (ORDER BY month) AS cumulative_revenue
FROM (
  SELECT DATE_TRUNC('month', order_date)::date AS month, SUM(sales) AS revenue
  FROM fact_sales
  GROUP BY 1
) t
ORDER BY month;

-- -----------------------------------
-- Product performance
-- -----------------------------------

-- 1) Top 10 products by revenue and profitability (revenue, profit, profit margin, avg discount)
SELECT
  p.product_id,
  p.product_name,
  SUM(f.sales) AS total_revenue,
  SUM(f.profit) AS total_profit,
  ROUND(100.0 * SUM(f.profit) / NULLIF(SUM(f.sales),0),2) AS profit_margin_pct,
  ROUND(AVG(f.discount)::numeric,4) AS avg_discount,
  SUM(f.quantity) AS units_sold
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY 1,2
ORDER BY total_revenue DESC
LIMIT 10;

-- 2) Category contribution to total revenue (and cumulative %)
WITH cat AS (
  SELECT category, SUM(sales) AS cat_sales
  FROM fact_sales f JOIN dim_product p ON f.product_id = p.product_id
  GROUP BY category
)
SELECT
  category,
  cat_sales,
  ROUND(100.0 * cat_sales / SUM(cat_sales) OVER (),2) AS pct_of_total,
  SUM(cat_sales) OVER (ORDER BY cat_sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
    SUM(cat_sales) OVER () AS cumulative_share
FROM cat
ORDER BY cat_sales DESC;

-- -----------------------------------
-- Customer loyalty & retention
-- -----------------------------------

-- 1) Basic RFM scoring components (Recency as days since last order, Frequency (count orders), Monetary)
WITH cust AS (
  SELECT
    customer_id,
    MAX(order_date) AS last_order_date,
    COUNT(DISTINCT order_id) AS frequency,
    SUM(sales) AS monetary
  FROM fact_sales
  GROUP BY customer_id
),
params AS (SELECT MAX(order_date) AS analysis_date FROM fact_sales)
SELECT
  c.customer_id,
  c.last_order_date,
  (SELECT analysis_date FROM params) - c.last_order_date AS recency_days,
  c.frequency,
  c.monetary
FROM cust c
ORDER BY monetary DESC
LIMIT 100;

-- 2) Repeat purchase rate: customers with >1 orders
SELECT
  SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_customers,
  COUNT(*) AS total_customers,
  ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*),2) AS repeat_pct
FROM (
  SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
  FROM fact_sales
  GROUP BY customer_id
) t;

-- 3) Cohort / retention starter (monthly acquisition + next-month retention)
-- Acquire month = month of first order for a customer; measure % that order again next N months.
WITH first_order AS (
  SELECT customer_id, DATE_TRUNC('month', MIN(order_date))::date AS cohort_month
  FROM fact_sales
  GROUP BY customer_id
),
orders_by_month AS (
  SELECT customer_id, DATE_TRUNC('month', order_date)::date AS order_month
  FROM fact_sales
  GROUP BY customer_id, DATE_TRUNC('month', order_date)
)
SELECT
  f.cohort_month,
  COUNT(DISTINCT f.customer_id) AS cohort_size,
  COUNT(DISTINCT CASE WHEN o.order_month = f.cohort_month + INTERVAL '1 month' THEN o.customer_id END) AS retained_next_month,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN o.order_month = f.cohort_month + INTERVAL '1 month' THEN o.customer_id END) / NULLIF(COUNT(DISTINCT f.customer_id),0),2) AS retention_next_month_pct
FROM first_order f
LEFT JOIN orders_by_month o ON f.customer_id = o.customer_id
GROUP BY f.cohort_month
ORDER BY f.cohort_month;

-- -----------------------------------
-- Regional performance
-- -----------------------------------

-- 1) Regional revenue & margin leaderboard
SELECT
  region,
  SUM(sales) AS revenue,
  SUM(profit) AS profit,
  ROUND(100.0 * SUM(profit) / NULLIF(SUM(sales),0),2) AS profit_margin_pct,
  COUNT(DISTINCT customer_id) AS unique_customers
FROM fact_sales
GROUP BY region
ORDER BY revenue DESC;

-- 2) Top/underperforming states in a given region
SELECT state, SUM(sales) AS revenue, SUM(profit) AS profit, ROUND(100.0*SUM(profit)/NULLIF(SUM(sales),0),2) AS margin
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.region = 'Central US'
GROUP BY state
ORDER BY revenue DESC
LIMIT 20;

-- 3) Return rate by region (orders flagged as returned / total orders)
SELECT
  region,
  COUNT(*) FILTER (WHERE returned = TRUE) AS return_count,
  COUNT(*) AS order_rows,
  ROUND(100.0 * COUNT(*) FILTER (WHERE returned = TRUE) / NULLIF(COUNT(*),0),2) AS return_pct
FROM fact_sales
GROUP BY region
ORDER BY return_pct DESC;
