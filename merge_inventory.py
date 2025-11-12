import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from retail_inventory_cleaning import run_retail_inventory_cleaning
from wholesale_inventory_cleaning import run_wholesale_inventory_cleaning

# --- Snowflake (target) ---
SF_USER = os.getenv("MY_SF_USER")
SF_PASS = os.getenv("MY_SF_PASS")
SF_ACCOUNT = os.getenv("MY_SF_ACCT")
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE = "NEA_FORECASTING"
SF_SCHEMA = "PUBLIC"

# ---------- Helpers ----------
def harmonize_wholesale_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map wholesale inventory columns to the same names used by retail inventory outputs."""
    rename_map = {
        "MATCHED_SNOP_CATEGORY": "Matched S&OP Category",
        "MATCH_SCORE": "Match Score",
        "MATCH_RESULT": "Match Result",
        "MATCHED_REFERENCE": "Matched Reference",
        "LOCATIONNAME": "LOCATIONNAME",
        "INVENTORYDATE": "INVENTORYDATE",
        "PRODUCTNAME": "PRODUCTNAME",
        "BRANDNAME": "BRANDNAME",
        "TOTAL_QUANTITY": "TOTAL_QUANTITY",
    }
    return df.rename(columns=rename_map)

def split_matched_unmatched(df: pd.DataFrame):
    cond = df["Matched S&OP Category"].notna() & (df["Matched S&OP Category"].astype(str).str.strip() != "")
    return df[cond].copy(), df[~cond].copy()

def align_columns(df1: pd.DataFrame, df2: pd.DataFrame):
    """Align columns but ensure TOTAL_QUANTITY is preserved"""
    print(f"🔍 DEBUG align_columns:")
    print(f"   df1 columns: {list(df1.columns)}")
    print(f"   df2 columns: {list(df2.columns)}")
    
    common = list(set(df1.columns).intersection(df2.columns))
    print(f"   Common columns: {common}")
    
    # Ensure TOTAL_QUANTITY is included if it exists in either DataFrame
    if 'TOTAL_QUANTITY' in df1.columns and 'TOTAL_QUANTITY' not in common:
        print("⚠️ TOTAL_QUANTITY missing from df2, adding with NaN")
        df2['TOTAL_QUANTITY'] = None
        common.append('TOTAL_QUANTITY')
    if 'TOTAL_QUANTITY' in df2.columns and 'TOTAL_QUANTITY' not in common:
        print("⚠️ TOTAL_QUANTITY missing from df1, adding with NaN")
        df1['TOTAL_QUANTITY'] = None
        common.append('TOTAL_QUANTITY')
    
    print(f"   Final aligned columns: {common}")
    return df1[common].copy(), df2[common].copy()

def upload_to_snowflake(df: pd.DataFrame, table_name: str):
    print(f"\n🔍 DEBUG - Uploading {table_name}:")
    print(f"   DataFrame shape: {df.shape}")
    print(f"   DataFrame columns: {list(df.columns)}")
    
    df = df.copy()
    if "TOTAL_QUANTITY" in df.columns:
        print(f"✅ TOTAL_QUANTITY found")
        print(f"   Before conversion: sum={df['TOTAL_QUANTITY'].sum()}, nulls={df['TOTAL_QUANTITY'].isna().sum()}")
        df["TOTAL_QUANTITY"] = pd.to_numeric(df["TOTAL_QUANTITY"], errors="coerce").astype(float)
        print(f"   After conversion: sum={df['TOTAL_QUANTITY'].sum()}, nulls={df['TOTAL_QUANTITY'].isna().sum()}")
    else:
        print(f"❌ TOTAL_QUANTITY column missing!")

    conn = snowflake.connector.connect(
        user=SF_USER, password=SF_PASS, account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA
    )
    try:
        # Clear just the snapshot dates we're about to load
        if "INVENTORYDATE" in df.columns and not df.empty:
            dates = sorted(set(pd.to_datetime(df["INVENTORYDATE"]).dt.strftime("%Y-%m-%d")))
            if dates:
                in_list = ",".join([f"TO_DATE('{d}')" for d in dates])
                conn.cursor().execute(f"DELETE FROM {table_name} WHERE INVENTORYDATE IN ({in_list});")
                print(f"🔄 Cleared {table_name} for snapshot(s): {', '.join(dates)}")

        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
        print(f"✅ Uploaded to {table_name}: {nrows} rows")
    finally:
        conn.close()

# ---------- Main ----------
if __name__ == "__main__":
    print("🧩 Running retail & wholesale inventory cleaners...")

    # Debug: Check what the cleaners return
    print("🔍 Running retail inventory cleaner...")
    retail_matched, retail_unmatched = run_retail_inventory_cleaning()
    print(f"   Retail matched columns: {list(retail_matched.columns)}")
    print(f"   Retail matched shape: {retail_matched.shape}")
    if 'TOTAL_QUANTITY' in retail_matched.columns:
        print(f"   Retail TOTAL_QUANTITY: sum={retail_matched['TOTAL_QUANTITY'].sum()}")

    print("🔍 Running wholesale inventory cleaner...")
    wholesale_df = run_wholesale_inventory_cleaning()
    print(f"   Wholesale columns: {list(wholesale_df.columns)}")
    print(f"   Wholesale shape: {wholesale_df.shape}")
    
    wholesale_df = harmonize_wholesale_columns(wholesale_df)
    print(f"   After harmonization: {list(wholesale_df.columns)}")
    if 'TOTAL_QUANTITY' in wholesale_df.columns:
        print(f"   Wholesale TOTAL_QUANTITY: sum={wholesale_df['TOTAL_QUANTITY'].sum()}")
    
    wholesale_matched, wholesale_unmatched = split_matched_unmatched(wholesale_df)

    # Align schemas & combine
    print("🔍 Aligning columns...")
    retail_matched, wholesale_matched = align_columns(retail_matched, wholesale_matched)
    retail_unmatched, wholesale_unmatched = align_columns(retail_unmatched, wholesale_unmatched)

    merged_matched = pd.concat([retail_matched, wholesale_matched], ignore_index=True)
    merged_unmatched = pd.concat([retail_unmatched, wholesale_unmatched], ignore_index=True)

    print(f"🔍 Final merged columns: {list(merged_matched.columns)}")
    if 'TOTAL_QUANTITY' in merged_matched.columns:
        print(f"   Final TOTAL_QUANTITY: sum={merged_matched['TOTAL_QUANTITY'].sum()}")

    # Upload both matched and unmatched
    upload_to_snowflake(merged_matched,   "matched_inventory_with_snop_category")
    upload_to_snowflake(merged_unmatched, "unmatched_inventory_without_snop_category")
