-- A set of SQL checks to run after ingestion into raw_ tables (or before transformations).

-- 1) Basic row counts
SELECT 'orders_raw_count' AS check, COUNT(*) AS cnt FROM raw_orders;
SELECT 'returns_raw_count' AS check, COUNT(*) AS cnt FROM raw_returns;
SELECT 'people_raw_count' AS check, COUNT(*) AS cnt FROM raw_people;

-- 2) Null key checks: order_id / row_id required
SELECT COUNT(*) AS missing_order_id FROM raw_orders WHERE order_id IS NULL;
SELECT COUNT(*) AS missing_row_id FROM raw_orders WHERE row_id IS NULL;

-- 3) Duplicate Row ID check (row_id should uniquely identify rows)
SELECT row_id, COUNT(*) AS cnt
FROM raw_orders
GROUP BY row_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;

-- 4) Duplicate order lines check (detect same order_id + product_id + sales repeated)
SELECT order_id, product_id, COUNT(*) AS cnt
FROM raw_orders
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;

-- 5) Out-of-range values
SELECT COUNT(*) AS negative_quantity FROM raw_orders WHERE quantity < 0;
SELECT COUNT(*) AS negative_sales FROM raw_orders WHERE sales < 0;
SELECT COUNT(*) AS negative_profit FROM raw_orders WHERE profit < 0;

-- invalid discount: outside [0,1]
SELECT COUNT(*) AS invalid_discount FROM raw_orders WHERE discount IS NOT NULL AND (discount < 0 OR discount > 1);

-- 6) Quick data-distribution checks (top categories / markets)
SELECT category, COUNT(*) AS cnt, SUM(sales) AS total_sales, SUM(profit) AS total_profit
FROM raw_orders
GROUP BY category
ORDER BY total_sales DESC
LIMIT 10;

SELECT market, COUNT(*) AS cnt, SUM(sales) AS total_sales
FROM raw_orders
GROUP BY market
ORDER BY total_sales DESC;

-- 7) Returns sanity check: returned values present and map to order ids
SELECT returned, COUNT(*) AS cnt FROM raw_returns GROUP BY returned;

-- 8) Join-level check: how many orders marked in returns vs how many order rows exist
SELECT
  (SELECT COUNT(DISTINCT order_id) FROM raw_orders) AS distinct_orders,
  (SELECT COUNT(DISTINCT order_id) FROM raw_returns) AS distinct_returns;

-- 9) Basic null distribution for key analytic columns
SELECT
    SUM(CASE WHEN sales IS NULL THEN 1 ELSE 0 END) AS sales_nulls,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS quantity_nulls,
    SUM(CASE WHEN profit IS NULL THEN 1 ELSE 0 END) AS profit_nulls
FROM raw_orders;
