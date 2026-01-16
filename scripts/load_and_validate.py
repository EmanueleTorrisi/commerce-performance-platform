# scripts/load_and_validate.py
import duckdb
import pathlib
import sys

DB_FILE = "commerce.duckdb"
SQL_DIR = pathlib.Path("sql")
PARQUET = "./data/processed/cleaned_orders.parquet"

def main():
    # Connect
    con = duckdb.connect(database=DB_FILE, read_only=False)
    print("Connected to", DB_FILE)

    # Basic sanity: parquet existence and row count (read directly from file)
    p = pathlib.Path(PARQUET)
    if not p.exists():
        print("ERROR: Parquet file not found at", PARQUET)
        con.close()
        sys.exit(1)

    try:
        parquet_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{PARQUET}')").fetchone()[0]
    except Exception as e:
        print("ERROR: Unable to read parquet with DuckDB:", e)
        con.close()
        sys.exit(1)

    print(f"Parquet file found: {PARQUET} | rows: {parquet_rows}")

    # Optional: execute duckdb-compatible schema if present.
    # It's safe to run schema before or after loading raw_orders because we'll replace raw_orders explicitly.
    duck_schema = SQL_DIR / "01_schema_creation_duckdb.sql"
    if duck_schema.exists():
        try:
            schema_sql = duck_schema.read_text()
            con.execute(schema_sql)
            print("Executed DuckDB schema from", duck_schema)
        except Exception as e:
            print("WARNING: Failed to execute duckdb schema:", e)
    else:
        print("No DuckDB schema file found at", duck_schema, "- skipping DDL execution.")

    # 1) Load parquet into raw_orders (force the correct structure and data)
    try:
        con.execute(f"CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM read_parquet('{PARQUET}');")
        loaded_rows = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        print("Loaded parquet into raw_orders. Row count:", loaded_rows)
    except Exception as e:
        print("ERROR loading parquet into raw_orders:", e)
        con.close()
        sys.exit(1)

    # 2) Inspect columns in raw_orders
    try:
        cols_info = con.execute("PRAGMA table_info('raw_orders')").fetchall()
        col_names = [r[1] for r in cols_info]
        print("raw_orders columns:", col_names)
    except Exception as e:
        print("ERROR inspecting raw_orders schema:", e)
        con.close()
        sys.exit(1)

    # 3) Create raw_returns from raw_orders if column exists, otherwise create empty placeholder
    if 'returned' in col_names:
        try:
            con.execute("""
                CREATE OR REPLACE TABLE raw_returns AS
                SELECT DISTINCT order_id, returned FROM raw_orders WHERE returned IS NOT NULL;
            """)
            cnt = con.execute("SELECT COUNT(*) FROM raw_returns").fetchone()[0]
            print("raw_returns created from raw_orders; rows:", cnt)
        except Exception as e:
            print("WARNING: Failed to create raw_returns from raw_orders:", e)
            con.execute("CREATE OR REPLACE TABLE raw_returns (order_id TEXT, returned TEXT, region TEXT);")
            print("Empty raw_returns created as fallback.")
    else:
        print("Column 'returned' not present in raw_orders. Creating empty raw_returns table.")
        con.execute("CREATE OR REPLACE TABLE raw_returns (order_id TEXT, returned TEXT, region TEXT);")
        print("Empty raw_returns created.")

    # 4) Create raw_people from raw_orders if columns exist, otherwise placeholder
    if 'person' in col_names and 'region' in col_names:
        try:
            con.execute("""
                CREATE OR REPLACE TABLE raw_people AS
                SELECT DISTINCT region, person FROM raw_orders WHERE person IS NOT NULL;
            """)
            cntp = con.execute("SELECT COUNT(*) FROM raw_people").fetchone()[0]
            print("raw_people created from raw_orders; rows:", cntp)
        except Exception as e:
            print("WARNING: Failed to create raw_people from raw_orders:", e)
            con.execute("CREATE OR REPLACE TABLE raw_people (region TEXT, person TEXT);")
            print("Empty raw_people created as fallback.")
    else:
        print("Columns 'person' and/or 'region' not present in raw_orders. Creating empty raw_people table.")
        con.execute("CREATE OR REPLACE TABLE raw_people (region TEXT, person TEXT);")
        print("Empty raw_people created.")

    # 5) Run a set of quick programmatic quality checks and print results (clear, senior-level smoke tests)
    try:
        qc = {}
        qc['orders_raw_count'] = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        qc['missing_order_id'] = con.execute("SELECT COUNT(*) FROM raw_orders WHERE order_id IS NULL").fetchone()[0]
        qc['invalid_discount'] = con.execute("SELECT COUNT(*) FROM raw_orders WHERE discount IS NOT NULL AND (discount < 0 OR discount > 1)").fetchone()[0]
        qc['negative_quantity'] = con.execute("SELECT COUNT(*) FROM raw_orders WHERE quantity < 0").fetchone()[0]
        qc['duplicate_row_id'] = con.execute("SELECT COUNT(*) FROM (SELECT row_id FROM raw_orders GROUP BY row_id HAVING COUNT(*) > 1)").fetchone()[0]
        print("\nQuick quality checks:")
        for k, v in qc.items():
            print(f"  {k}: {v}")
    except Exception as e:
        print("WARNING: Quality checks failed:", e)

    # 6) Optionally run the quality checks SQL file (02_quality_checks.sql) and print a small note
    qc_file = SQL_DIR / "02_quality_checks.sql"
    if qc_file.exists():
        try:
            # Execute the file as-is. For complex multi-statement files the last SELECT result may be returned;
            # we run it primarily for side-effect / manual inspection in logs.
            con.execute(qc_file.read_text())
            print("Executed quality checks SQL file:", qc_file)
        except Exception as e:
            print("WARNING: Running 02_quality_checks.sql raised an error:", e)
    else:
        print("No quality checks file found at", qc_file)

    con.close()
    print("Loader finished successfully.")

if __name__ == "__main__":
    main()
