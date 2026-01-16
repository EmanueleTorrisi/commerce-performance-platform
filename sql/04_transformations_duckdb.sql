-- Create analytics-ready dimensions and fact table from raw_orders

-- -----------------------------
-- Date Dimension
-- -----------------------------
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    order_date,
    DATE_TRUNC('year', order_date)     AS year,
    DATE_TRUNC('quarter', order_date)  AS quarter,
    DATE_TRUNC('month', order_date)    AS month,
    EXTRACT(year FROM order_date)      AS year_num,
    EXTRACT(month FROM order_date)     AS month_num,
    CURRENT_LOCALTIMESTAMP()            AS created_at
FROM raw_orders
WHERE order_date IS NOT NULL;

-- -----------------------------
-- Customer Dimension
-- -----------------------------
CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    customer_id,
    customer_name,
    segment,
    country,
    region,
    market,
    CURRENT_LOCALTIMESTAMP() AS created_at
FROM raw_orders;

-- -----------------------------
-- Product Dimension
-- -----------------------------
CREATE OR REPLACE TABLE dim_product AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    sub_category,
    CURRENT_LOCALTIMESTAMP() AS created_at
FROM raw_orders;

-- -----------------------------
-- People / Ownership Dimension
-- -----------------------------
CREATE OR REPLACE TABLE dim_people AS
SELECT DISTINCT
    region,
    person,
    CURRENT_LOCALTIMESTAMP() AS created_at
FROM raw_orders
WHERE person IS NOT NULL;

-- -----------------------------
-- Fact Table: Sales
-- -----------------------------
CREATE OR REPLACE TABLE fact_sales AS
SELECT
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    product_id,
    sales,
    quantity,
    discount,
    profit,
    shipping_cost,
    order_priority,
    returned,
    profit_margin,
    CURRENT_LOCALTIMESTAMP() AS created_at
FROM raw_orders;
