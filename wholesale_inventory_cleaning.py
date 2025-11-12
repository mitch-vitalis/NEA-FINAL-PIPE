import pandas as pd
import os
import datetime
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()  # Optional for local testing

def run_wholesale_inventory_cleaning():
    # --- Inventory Date (1 day lag) ---
    inventory_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # --- Source Snowflake Connection (NEA_SALES) ---
    source_conn = snowflake.connector.connect(
        user=os.getenv("NEA_SF_USER"),
        password=os.getenv("NEA_SF_PASS"),
        account=os.getenv("NEA_SF_ACCT"),
        role="READ_ONLY_NEA",
        warehouse="COMPUTE_WH",
        database="NEA_SALES",
        schema="WHOLESALE"
    )

    # --- Pull Inventory Data ---
    with source_conn.cursor() as cs:
        cs.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")
        cs.execute(f"""
            WITH latest_date AS (
                SELECT MAX(INVENTORYDATE) AS max_date
                FROM VWHOLESALEPRODUCTS
            )
            SELECT
                'Wholesale' AS LOCATIONNAME,
                PRODUCTNAME,
                BRAND AS BRANDNAME,
                PRODUCTSKU,
                UNITSIZE AS WEIGHTUNIT,
                UNITSPERCASE,
                INVENTORYDATE,
                QUANTITY AS QUANTITYONHAND
            FROM VWHOLESALEPRODUCTS
            WHERE INVENTORYDATE = (SELECT max_date FROM latest_date)
            AND QUANTITY IS NOT NULL
            AND QUANTITY > 0
            AND PRODUCTARCHIVED = false
        """)
        rows = cs.fetchall()
        columns = [col[0] for col in cs.description]
        inventory_df = pd.DataFrame(rows, columns=columns)

    source_conn.close()

    # --- DEBUG CHECKPOINT 1: Raw Snowflake Data ---
    print(f"🔍 CHECKPOINT 1 - Raw inventory from Snowflake: {len(inventory_df)} products")
    trim_raw = inventory_df[inventory_df['PRODUCTNAME'].str.contains('TRIM', case=False, na=False)]
    print(f"🔍 TRIM products in raw data: {len(trim_raw)}")
    for _, row in trim_raw.iterrows():
        print(f"  - {row['PRODUCTNAME']} | SKU: {row.get('PRODUCTSKU', 'NULL')} | Qty: {row.get('QUANTITYONHAND', 'NULL')}")

    # --- SIMPLIFIED Unit Conversion (bulk/trim trigger only) ---
    def convert_to_units(row):
        try:
            product_name = str(row.get('PRODUCTNAME') or '').upper()
            quantity = float(row.get('QUANTITYONHAND') or 0)
            units_per_case = float(row.get('UNITSPERCASE') or 1)
            
            # Debug: Print first few products to verify function is running
            if not hasattr(convert_to_units, 'counter'):
                convert_to_units.counter = 0
            convert_to_units.counter += 1
            
            if convert_to_units.counter <= 3:
                print(f"🔍 DEBUG Product {convert_to_units.counter}: '{product_name}' | Qty: {quantity}")
            
            # Convert bulk/trim from pounds to grams
            if 'BULK' in product_name or 'TRIM' in product_name:
                result = quantity * 453.6  # Convert pounds to grams
                print(f"🔧 INVENTORY Converting: {product_name} | {quantity} lbs → {result} grams")
                return result
            else:
                # All other products use quantity as-is or apply units per case
                return quantity * units_per_case if units_per_case != 1 else quantity
                
        except Exception as e:
            print(f"⚠️ INVENTORY Conversion error: {e} | Row: {row.get('PRODUCTNAME', 'Unknown')}")
            return 0.0

    # Apply conversion with debug output
    print("🔧 DEBUG: Starting wholesale inventory unit conversion...")
    inventory_df['TOTAL_QUANTITY'] = inventory_df.apply(convert_to_units, axis=1)
    
    # Debug: Show before/after totals
    original_total = inventory_df['QUANTITYONHAND'].sum()
    converted_total = inventory_df['TOTAL_QUANTITY'].sum()
    print(f"🔧 DEBUG: Original total: {original_total}, Converted total: {converted_total}")

    # --- DEBUG CHECKPOINT 2: After Conversion ---
    print(f"🔍 CHECKPOINT 2 - After conversion: {len(inventory_df)} products")
    trim_converted = inventory_df[inventory_df['PRODUCTNAME'].str.contains('TRIM', case=False, na=False)]
    print(f"🔍 TRIM products after conversion: {len(trim_converted)}")
    for _, row in trim_converted.iterrows():
        print(f"  - {row['PRODUCTNAME']} | TOTAL_QUANTITY: {row.get('TOTAL_QUANTITY', 'NULL')}")

    # --- Matching Logic ---
    inventory_df['MATCHED_SNOP_CATEGORY'] = inventory_df['PRODUCTSKU']
    inventory_df['MATCH_RESULT'] = 'Matched (wholesale clean)'
    inventory_df['MATCH_SCORE'] = 100
    inventory_df['MATCHED_REFERENCE'] = 'wholesale direct'

    # --- Flag Rows with Missing PRODUCTSKU as Unmatched ---
    inventory_df.loc[inventory_df['PRODUCTSKU'].isna(), [
        'MATCHED_SNOP_CATEGORY', 'MATCH_RESULT', 'MATCH_SCORE', 'MATCHED_REFERENCE'
    ]] = [None, 'Missing SKU', None, None]

    # --- DEBUG CHECKPOINT 3: After Matching Logic ---
    print(f"🔍 CHECKPOINT 3 - After matching logic: {len(inventory_df)} products")
    trim_matched = inventory_df[inventory_df['PRODUCTNAME'].str.contains('TRIM', case=False, na=False)]
    print(f"🔍 TRIM products after matching: {len(trim_matched)}")
    for _, row in trim_matched.iterrows():
        print(f"  - {row['PRODUCTNAME']} | S&OP Category: '{row.get('MATCHED_SNOP_CATEGORY', 'NULL')}' | Match Result: '{row.get('MATCH_RESULT', 'NULL')}'")

    # --- TRIM/BULK Override for Missing SKUs ---
    print("🔧 Applying TRIM/BULK overrides for missing SKUs...")
    override_count = 0
    
    for idx, row in inventory_df.iterrows():
        if pd.isna(row['MATCHED_SNOP_CATEGORY']) or str(row['MATCHED_SNOP_CATEGORY']).strip() == "":
            product_name = str(row['PRODUCTNAME']).upper()
            brand_name = str(row.get('BRANDNAME', '')).upper()
            
            # Override for TRIM products
            if "TRIM" in product_name:
                if "NEA FIRE" in brand_name or "NEA FIRE" in product_name:
                    inventory_df.at[idx, 'MATCHED_SNOP_CATEGORY'] = "NEA Fire Bulk Flower g"
                    inventory_df.at[idx, 'MATCH_SCORE'] = 95
                    inventory_df.at[idx, 'MATCHED_REFERENCE'] = "trim name brand rule"
                    inventory_df.at[idx, 'MATCH_RESULT'] = "Trim Override"
                else:
                    inventory_df.at[idx, 'MATCHED_SNOP_CATEGORY'] = "NEA Bulk Flower g"
                    inventory_df.at[idx, 'MATCH_SCORE'] = 90
                    inventory_df.at[idx, 'MATCHED_REFERENCE'] = "trim name rule"
                    inventory_df.at[idx, 'MATCH_RESULT'] = "Trim Override"
                override_count += 1
                print(f"🔧 TRIM Override: {row['PRODUCTNAME']} → {inventory_df.at[idx, 'MATCHED_SNOP_CATEGORY']}")
            
            # Override for BULK products (if they also have missing SKUs)
            elif "BULK" in product_name:
                if "NEA FIRE" in brand_name or "NEA FIRE" in product_name:
                    inventory_df.at[idx, 'MATCHED_SNOP_CATEGORY'] = "NEA Fire Bulk Flower g"
                    inventory_df.at[idx, 'MATCH_SCORE'] = 95
                    inventory_df.at[idx, 'MATCHED_REFERENCE'] = "bulk name brand rule"
                    inventory_df.at[idx, 'MATCH_RESULT'] = "Bulk Override"
                else:
                    inventory_df.at[idx, 'MATCHED_SNOP_CATEGORY'] = "NEA Bulk Flower g"
                    inventory_df.at[idx, 'MATCH_SCORE'] = 90
                    inventory_df.at[idx, 'MATCHED_REFERENCE'] = "bulk name rule"
                    inventory_df.at[idx, 'MATCH_RESULT'] = "Bulk Override"
                override_count += 1
                print(f"🔧 BULK Override: {row['PRODUCTNAME']} → {inventory_df.at[idx, 'MATCHED_SNOP_CATEGORY']}")

    print(f"✅ Applied {override_count} TRIM/BULK overrides")

    # --- DEBUG CHECKPOINT 4: After Override Logic ---
    print(f"🔍 CHECKPOINT 4 - After overrides: {len(inventory_df)} products")
    trim_final = inventory_df[inventory_df['PRODUCTNAME'].str.contains('TRIM', case=False, na=False)]
    print(f"🔍 TRIM products after overrides: {len(trim_final)}")
    for _, row in trim_final.iterrows():
        print(f"  - {row['PRODUCTNAME']} | S&OP Category: '{row.get('MATCHED_SNOP_CATEGORY', 'NULL')}' | Match Result: '{row.get('MATCH_RESULT', 'NULL')}'")

    # --- Final Output Columns (no PRODUCTSKU) ---
    final_cols = [
        'LOCATIONNAME', 'INVENTORYDATE', 'MATCHED_SNOP_CATEGORY', 'PRODUCTNAME',
        'TOTAL_QUANTITY', 'MATCH_RESULT', 'MATCH_SCORE', 'MATCHED_REFERENCE',
        'BRANDNAME'
    ]
    inventory_df = inventory_df[final_cols].copy()

    # --- DEBUG CHECKPOINT 5: Final Output ---
    print(f"🔍 CHECKPOINT 5 - Final output: {len(inventory_df)} products")
    trim_output = inventory_df[inventory_df['PRODUCTNAME'].str.contains('TRIM', case=False, na=False)]
    print(f"🔍 TRIM products in final output: {len(trim_output)}")

    print(f"✅ Wholesale inventory conversion complete: {len(inventory_df)} products")
    return inventory_df.copy()