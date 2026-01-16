"""
Run DuckDB transformation SQL to populate dim_date, dim_customer, dim_product, dim_people, and fact_sales.

"""

import duckdb
import pathlib
import sys

DB_FILE = pathlib.Path("commerce.duckdb")
SQL_FILE = pathlib.Path("./sql/04_transformations_duckdb.sql")

def main():
    if not SQL_FILE.exists():
        print(f"ERROR: Transform SQL not found at {SQL_FILE}", file=sys.stderr)
        sys.exit(1)

    # Connect (creates DB file if it doesn't exist)
    con = duckdb.connect(str(DB_FILE))

    try:
        sql_text = SQL_FILE.read_text()
        print(f"Executing transformations from {SQL_FILE} ...")
        con.execute(sql_text)
        print("Transformation SQL executed.")
    except Exception as e:
        print("ERROR executing transformation SQL:", e, file=sys.stderr)
        con.close()
        sys.exit(2)

    # Post-transform validation: print counts for key tables
    try:
        def safe_count(q):
            try:
                return con.execute(q).fetchone()[0]
            except Exception:
                return None

        counts = {
            "fact_sales": safe_count("SELECT COUNT(*) FROM fact_sales"),
            "dim_date": safe_count("SELECT COUNT(*) FROM dim_date"),
            "dim_customer": safe_count("SELECT COUNT(*) FROM dim_customer"),
            "dim_product": safe_count("SELECT COUNT(*) FROM dim_product"),
            "dim_people": safe_count("SELECT COUNT(*) FROM dim_people")
        }

        print("\nPost-transformation row counts:")
        for k, v in counts.items():
            print(f"  {k}: {v}")
    except Exception as e:
        print("WARNING: Could not fetch post-transform counts:", e, file=sys.stderr)

    con.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
