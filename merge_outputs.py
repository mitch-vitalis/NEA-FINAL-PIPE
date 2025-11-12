
import pandas as pd
import os
import snowflake.connector
from retail_cleaning import run_retail_cleaning
from wholesale_cleaning import run_wholesale_cleaning
from snowflake.connector.pandas_tools import write_pandas

# --- Environment Variables (GitHub Secrets) ---
SF_USER = os.getenv("MY_SF_USER")
SF_PASS = os.getenv("MY_SF_PASS")
SF_ACCOUNT = os.getenv("MY_SF_ACCT")
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE = "NEA_FORECASTING"
SF_SCHEMA = "PUBLIC"

# --- Output Folder ---
output_folder = r"C:\Users\Mitch\OneDrive\Desktop\Consulting\Vitalis Files\NEA\Merged_Output"
os.makedirs(output_folder, exist_ok=True)

# --- Run Retail and Wholesale Scripts ---
print("🚀 Running retail and wholesale scripts...")
retail_matched, retail_unmatched, _, _ = run_retail_cleaning()
wholesale_matched, wholesale_unmatched, _, _ = run_wholesale_cleaning()

# --- Align Columns ---
def align_columns(df1, df2):
    common_cols = list(set(df1.columns).intersection(df2.columns))
    return df1[common_cols], df2[common_cols]

retail_matched, wholesale_matched = align_columns(retail_matched, wholesale_matched)
retail_unmatched, wholesale_unmatched = align_columns(retail_unmatched, wholesale_unmatched)

# --- Combine Data ---
merged_matched = pd.concat([retail_matched, wholesale_matched], ignore_index=True)
merged_unmatched = pd.concat([retail_unmatched, wholesale_unmatched], ignore_index=True)

# --- Ensure numeric type for TOTAL_QUANTITY ---
for df in [merged_matched, merged_unmatched]:
    if 'TOTAL_QUANTITY' in df.columns:
        df['TOTAL_QUANTITY'] = pd.to_numeric(df['TOTAL_QUANTITY'], errors='coerce').astype(float)

# --- Snowflake Import Function ---
def upload_to_snowflake(df, table_name):
    df = df.copy()
    if 'TOTAL_QUANTITY' in df.columns:
        df['TOTAL_QUANTITY'] = pd.to_numeric(df['TOTAL_QUANTITY'], errors='coerce').astype(float)
        print("⚠️ Rows with NaN TOTAL_QUANTITY:", df['TOTAL_QUANTITY'].isna().sum())

    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASS,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    if "TRANSACTIONDATE" in df.columns:
        conn.cursor().execute(
            f"DELETE FROM {table_name} WHERE TRANSACTIONDATE >= DATEADD(DAY, -90, CURRENT_DATE());"
        )
        print(f"🔄 Cleared {table_name} (last 30 days).")
        print(f"🔄 Cleared {table_name} (last 30 days).")
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
    print(f"✅ Uploaded to {table_name}: {nrows} rows")
    conn.close()

# --- Upload to Snowflake ---
upload_to_snowflake(merged_matched, "matched_sales_with_snop_category")
upload_to_snowflake(merged_unmatched, "unmatched_sales_without_snop_category")
