import pandas as pd
import os
import logging
from datetime import datetime
import numpy as np

# Set up logging for better traceability and debugging in production environments
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    """
    Optimized ETL pipeline for the Global Superstore 2016 dataset.

    Key features:

    - Proper handling of Excel serial dates.
    - Explicit dtype specifications during loading to minimize memory usage.
    - Enhanced error handling with try-except blocks to prevent crashes and provide informative logs.
    - Data validation steps to check for inconsistencies (e.g., duplicates, missing keys).
    - Modular functions for better maintainability and reusability.
    - Preservation of all columns for comprehensive analysis, but with options for selective export if needed.
    - Export to both CSV (for accessibility) and Parquet (for efficiency).

    """

    # ----------------------------
    # 1. Define File Paths
    # ----------------------------
    raw_path = './data/raw/global_superstore_2016.xlsx'
    processed_csv = './data/processed/cleaned_orders.csv'
    processed_parquet = './data/processed/cleaned_orders.parquet'

    # Ensure output directory exists
    os.makedirs(os.path.dirname(processed_csv), exist_ok=True)

    # ----------------------------
    # 2. Load Excel Sheets with Optimized Dtypes
    # ----------------------------
    logging.info(f"Loading Excel from {raw_path}...")

    # Dtypes to minimize memory
    orders_dtypes = {
        'Row ID': 'int32',
        'Postal Code': 'str',
        'Sales': 'float32',
        'Quantity': 'int16',
        'Discount': 'float32',
        'Profit': 'float32',
        'Shipping Cost': 'float32'
    }

    try:
        # try to let pandas parse dates normally (more robust with mixed formats)
        orders = pd.read_excel(raw_path, sheet_name='Orders', dtype=orders_dtypes, parse_dates=['Order Date', 'Ship Date'])
        returns = pd.read_excel(raw_path, sheet_name='Returns')
        people = pd.read_excel(raw_path, sheet_name='People')
    except FileNotFoundError:
        logging.error(f"File not found: {raw_path}")
        raise
    except ValueError as e:
        # fallback: try reading without parse_dates then coerce dates manually
        logging.warning(f"Initial sheet load with parse_dates failed: {e}. Attempting fallback read without date parsing.")
        try:
            orders = pd.read_excel(raw_path, sheet_name='Orders', dtype=orders_dtypes)
            returns = pd.read_excel(raw_path, sheet_name='Returns')
            people = pd.read_excel(raw_path, sheet_name='People')
        except Exception as e2:
            logging.error(f"Sheet loading error on fallback: {e2}")
            raise

    # ----------------------------
    # 3. Standardize Column Names
    # ----------------------------
    def clean_cols(df):
        """Clean column names: strip, lowercase, replace spaces/hyphens with underscores."""
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        return df

    orders = clean_cols(orders)
    returns = clean_cols(returns)
    people = clean_cols(people)

    # ----------------------------
    # 4. Data Validation
    # ----------------------------
    logging.info("Validating data...")

    # Drop exact duplicate rows
    initial_rows = len(orders)
    orders = orders.drop_duplicates()
    if len(orders) < initial_rows:
        logging.warning(f"Dropped {initial_rows - len(orders)} exact duplicate rows in Orders.")

    # Check for duplicates in People regions
    if 'region' in people.columns and people['region'].duplicated().any():
        logging.warning("Duplicates in People 'region'. Keeping first occurrence...")
        people = people.drop_duplicates(subset=['region'])

    # Basic data quality checks
    if 'quantity' in orders.columns and (orders['quantity'] < 0).any():
        logging.warning("Negative quantities found in Orders.")
    if 'discount' in orders.columns and ((orders['discount'] > 1).any() or (orders['discount'] < 0).any()):
        logging.warning("Invalid discounts (not in [0,1]) found in Orders.")

    # Check missing keys
    missing_orders = orders['order_id'].isnull().sum() if 'order_id' in orders.columns else 0
    if missing_orders > 0:
        logging.warning(f"{missing_orders} missing order_ids. Dropping those rows.")
        orders = orders.dropna(subset=['order_id'])

    # ----------------------------
    # 5. Merge DataFrames
    # ----------------------------
    logging.info("Merging sheets...")

    # Normalize returns: ensure lower-case column names exist
    if 'order_id' not in returns.columns or 'returned' not in returns.columns:
        logging.error("Returns sheet does not contain expected columns 'Order ID' and 'Returned' after cleaning.")
        raise ValueError("Invalid Returns sheet format.")

    df_merged = pd.merge(
        orders,
        returns[['order_id', 'returned']],
        on='order_id',
        how='left'
    )
    # Normalize returned values to 'Yes'/'No' then cast to category
    df_merged['returned'] = df_merged['returned'].astype('str').str.strip().str.capitalize()
    df_merged.loc[~df_merged['returned'].isin(['Yes','No']), 'returned'] = 'No'
    df_merged['returned'] = df_merged['returned'].astype('category')

    # Canada regional split - keep your logic but guard for missing columns
    canada_mask = (df_merged['country'] == 'Canada') if 'country' in df_merged.columns else pd.Series([False]*len(df_merged))
    eastern_states = ['Ontario', 'Quebec', 'Nova Scotia', 'New Brunswick', 'Prince Edward Island', 'Newfoundland and Labrador']
    if 'state' in df_merged.columns:
        df_merged.loc[canada_mask & df_merged['state'].isin(eastern_states), 'region'] = 'Eastern Canada'
        df_merged.loc[canada_mask & ~df_merged['state'].isin(eastern_states), 'region'] = 'Western Canada'

    # Merge people: people has 'region' and 'person'
    if 'region' in df_merged.columns and 'region' in people.columns:
        df_merged = pd.merge(
            df_merged,
            people[['region', 'person']],
            on='region',
            how='left'
        )
    else:
        # If region missing in people, add person with Unknown
        df_merged['person'] = 'Unknown'

    # Handle unmatched regions
    unmatched = df_merged['person'].isnull()
    unmatched_count = unmatched.sum()
    if unmatched_count > 0:
        logging.warning(f"{unmatched_count} rows have unmatched regions. Filling with 'Unknown'.")
        # Log unique unmatched regions for debugging
        unmatched_regions = df_merged.loc[unmatched, 'region'].unique()
        logging.info(f"Unmatched regions: {unmatched_regions}")
        df_merged['person'] = df_merged['person'].fillna('Unknown')

    # ----------------------------
    # 6. Fix Data Types
    # ----------------------------
    logging.info("Converting data types...")

    # Safe Excel serial to datetime conversion (handles if already converted)
    def excel_to_datetime(serial):
        if pd.isna(serial):
            return pd.NaT
        if isinstance(serial, (int, float)):
            # Excel base date (Windows)
            return pd.Timestamp('1899-12-30') + pd.Timedelta(days=serial)
        elif isinstance(serial, (pd.Timestamp, datetime)):
            return serial
        else:
            # try parsing strings explicitly
            try:
                return pd.to_datetime(serial, errors='coerce')
            except Exception:
                logging.warning(f"Unexpected type in date column: {type(serial)}")
                return pd.NaT

    # If pandas parsed the dates already, keep them; otherwise coerce / fallback
    for col in ['order_date', 'ship_date']:
        if col in df_merged.columns:
            if not pd.api.types.is_datetime64_any_dtype(df_merged[col]):
                # try to parse
                df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce')
                # fallback row-wise for any remaining numeric excel serials
                mask_serial = df_merged[col].isna() & df_merged[col].notnull()
                # above mask is usually False; do a safe apply fallback for non-datetime types
                df_merged[col] = df_merged[col].apply(lambda x: excel_to_datetime(x) if not pd.isna(x) and not isinstance(x, pd.Timestamp) else x)

    # Convert to categorical for memory savings
    categoricals = ['ship_mode', 'segment', 'country', 'region', 'market', 'category', 'sub_category', 'order_priority', 'returned']
    for col in categoricals:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].astype('category')

    # ----------------------------
    # 7. Additional Cleaning
    # ----------------------------
    # Fill missing postal codes
    if 'postal_code' in df_merged.columns:
        # convert to string preserving leading zeros, fill NaN
        df_merged['postal_code'] = df_merged['postal_code'].astype(str).replace('nan', 'Unknown')
        df_merged.loc[df_merged['postal_code'].str.strip() == '', 'postal_code'] = 'Unknown'

    # Normalize person names & regions (strip weird whitespace / NBSP)
    if 'person' in df_merged.columns:
        df_merged['person'] = df_merged['person'].astype(str).str.replace(u'\xa0', ' ').str.strip()

    if 'region' in df_merged.columns:
        df_merged['region'] = df_merged['region'].astype(str).str.replace(u'\xa0', ' ').str.strip()

    # Round floats to 2 decimals
    float_cols = ['sales', 'discount', 'profit', 'shipping_cost']
    for col in float_cols:
        if col in df_merged.columns:
            # coerce to numeric first (avoid object dtype surprises)
            df_merged[col] = pd.to_numeric(df_merged[col], errors='coerce').round(2)

    # Derived: profit_margin (safe)
    if 'profit' in df_merged.columns and 'sales' in df_merged.columns:
        df_merged['profit_margin'] = np.where(df_merged['sales'].fillna(0) == 0, np.nan,
                                              (df_merged['profit'] / df_merged['sales']).round(4))

    # Validate / flag discounts outside expected range (0-1)
    if 'discount' in df_merged.columns:
        invalid_discount_mask = df_merged['discount'].isnull() & df_merged['discount'].notnull()  # default False
        # Explicit check for >1 or <0
        out_of_range = df_merged[(df_merged['discount'] < 0) | (df_merged['discount'] > 1)]
        if len(out_of_range) > 0:
            logging.warning(f"Found {len(out_of_range)} discounts outside [0,1]. Setting them to NaN.")
            df_merged.loc[(df_merged['discount'] < 0) | (df_merged['discount'] > 1), 'discount'] = np.nan

    # ----------------------------
    # 8. Export Cleaned Data
    # ----------------------------
    logging.info(f"Exporting CSV to {processed_csv}...")
    df_merged.to_csv(processed_csv, index=False)

    logging.info(f"Exporting Parquet to {processed_parquet}...")
    df_merged.to_parquet(processed_parquet, index=False, engine='pyarrow')

    logging.info("Pipeline Complete. Dataset is cleaned, validated, and ready for analysis.")
    logging.info(f"Final shape: {df_merged.shape} | Memory usage: {df_merged.memory_usage(deep=True).sum() / (1024 ** 2):.2f} MB")

# Entry point
if __name__ == "__main__":
    run_pipeline()
