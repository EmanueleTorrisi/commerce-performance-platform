"""
Run business-grade intelligence queries and export curated CSVs for Power BI.

This script produces the following analytics-ready tables:
1. Executive KPIs
2. Sales Trends & Growth
3. Product Performance
4. Customer Loyalty & Retention
5. Regional Performance
6. Discount & Profitability Analysis
"""

import duckdb
import pathlib
import sys
import pandas as pd

DB_FILE = pathlib.Path("commerce.duckdb")
OUT_DIR = pathlib.Path("./data/processed")

# ---------------------------------------------------
# Sql definitions
# ---------------------------------------------------

KPI_SQL = """
SELECT
    SUM(sales)                             AS total_revenue,
    SUM(profit)                            AS total_profit,
    COUNT(DISTINCT order_id)               AS total_orders,
    COUNT(DISTINCT customer_id)            AS total_customers,
    ROUND(SUM(profit) / NULLIF(SUM(sales),0), 4) AS profit_margin
FROM fact_sales;
"""

MONTHLY_TRENDS_SQL = """
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(sales)                      AS revenue,
    SUM(profit)                     AS profit,
    COUNT(DISTINCT order_id)        AS orders,
    COUNT(DISTINCT customer_id)     AS active_customers
FROM fact_sales
GROUP BY month
ORDER BY month;
"""

PRODUCT_PERFORMANCE_SQL = """
SELECT
    f.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    SUM(f.sales)    AS revenue,
    SUM(f.profit)   AS profit,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.profit) / NULLIF(SUM(f.sales),0), 4) AS profit_margin
FROM fact_sales f
JOIN dim_product p USING (product_id)
GROUP BY
    f.product_id,
    p.product_name,
    p.category,
    p.sub_category;
"""

CUSTOMER_LOYALTY_SQL = """
SELECT
    customer_id,
    COUNT(DISTINCT order_id)        AS order_count,
    SUM(sales)                      AS lifetime_revenue,
    SUM(profit)                     AS lifetime_profit,
    MIN(order_date)                 AS first_order_date,
    MAX(order_date)                 AS last_order_date
FROM fact_sales
GROUP BY customer_id;
"""

REGIONAL_PERFORMANCE_SQL = """
SELECT
    c.market,
    c.region,
    SUM(f.sales)    AS revenue,
    SUM(f.profit)   AS profit,
    COUNT(DISTINCT f.order_id) AS orders,
    COUNT(DISTINCT f.customer_id) AS customers
FROM fact_sales f
JOIN dim_customer c USING (customer_id)
GROUP BY c.market, c.region;
"""

DISCOUNT_PROFITABILITY_SQL = """
SELECT
    discount,
    COUNT(DISTINCT order_id) AS orders,
    SUM(sales)              AS revenue,
    SUM(profit)             AS profit,
    ROUND(SUM(profit) / NULLIF(SUM(sales),0), 4) AS profit_margin
FROM fact_sales
GROUP BY discount
ORDER BY discount;
"""

# ---------------------------------------------------
# Execution
# ---------------------------------------------------

def export_query(con, sql, filename):
    df = con.execute(sql).df()
    out_path = OUT_DIR / filename
    df.to_csv(out_path, index=False)
    print(f"Exported {filename:<30} rows: {len(df)}")

def main():
    if not DB_FILE.exists():
        print(f"ERROR: DuckDB file not found at {DB_FILE}", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_FILE))

    try:
        print("\nRunning business exports...\n")

        export_query(con, KPI_SQL, "kpi_overview.csv")
        export_query(con, MONTHLY_TRENDS_SQL, "monthly_sales_trends.csv")
        export_query(con, PRODUCT_PERFORMANCE_SQL, "product_performance.csv")
        export_query(con, CUSTOMER_LOYALTY_SQL, "customer_loyalty.csv")
        export_query(con, REGIONAL_PERFORMANCE_SQL, "regional_performance.csv")
        export_query(con, DISCOUNT_PROFITABILITY_SQL, "discount_profitability.csv")

    except Exception as e:
        print("ERROR during export:", e, file=sys.stderr)
        sys.exit(2)
    finally:
        con.close()

    print("\nAll Power BI datasets exported successfully.")

if __name__ == "__main__":
    main()
