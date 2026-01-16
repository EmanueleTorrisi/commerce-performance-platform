-- Creates raw (staging) tables, denormalized staging -> normalized schema (dimensions + fact)

-- 1) Create staging tables
CREATE TABLE IF NOT EXISTS raw_orders (
    row_id            INTEGER,
    order_id          TEXT,
    order_date        DATE,
    ship_date         DATE,
    ship_mode         TEXT,
    customer_id       TEXT,
    customer_name     TEXT,
    segment           TEXT,
    postal_code       TEXT,
    city              TEXT,
    state             TEXT,
    country           TEXT,
    region            TEXT,
    market            TEXT,
    product_id        TEXT,
    category          TEXT,
    sub_category      TEXT,
    product_name      TEXT,
    sales             DOUBLE,
    quantity          INTEGER,
    discount          DOUBLE,
    profit            DOUBLE,
    shipping_cost     DOUBLE,
    order_priority    TEXT
);

CREATE TABLE IF NOT EXISTS raw_returns (
    returned TEXT,
    order_id TEXT,
    region   TEXT
);

CREATE TABLE IF NOT EXISTS raw_people (
    person TEXT,
    region TEXT
);

-- 2) Dimension: date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key       DATE PRIMARY KEY,
    year           INTEGER,
    quarter        INTEGER,
    month          INTEGER,
    day            INTEGER,
    month_name     TEXT,
    day_of_week    INTEGER,
    is_weekend     BOOLEAN
);

-- 3) Dimension: customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id    TEXT PRIMARY KEY,
    customer_name  TEXT,
    segment        TEXT,
    postal_code    TEXT,
    city           TEXT,
    state          TEXT,
    country        TEXT,
    region         TEXT,
    market         TEXT
);

-- 4) Dimension: product
CREATE TABLE IF NOT EXISTS dim_product (
    product_id     TEXT PRIMARY KEY,
    product_name   TEXT,
    category       TEXT,
    sub_category   TEXT
);

-- 5) Dimension: people (sales owners / territory)
CREATE TABLE IF NOT EXISTS dim_people (
    region TEXT PRIMARY KEY,
    person TEXT
);

-- 6) Fact table: sales (one row per original order line)
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id         BIGINT PRIMARY KEY,
    row_id           INTEGER,
    order_id         TEXT,
    order_date       DATE,
    ship_date        DATE,
    customer_id      TEXT,
    product_id       TEXT,
    quantity         INTEGER,
    sales            DOUBLE,
    discount         DOUBLE,
    profit           DOUBLE,
    profit_margin    DOUBLE,
    shipping_cost    DOUBLE,
    order_priority   TEXT,
    ship_mode        TEXT,
    returned         BOOLEAN DEFAULT FALSE,
    region           TEXT,
    market           TEXT,
    created_at       TIMESTAMP DEFAULT current_timestamp
);
