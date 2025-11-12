def run_wholesale_cleaning():
    import pandas as pd
    import os
    import datetime
    import snowflake.connector

    # --- CONFIG ---
    output_base_dir = r"C:\Users\Mitch\OneDrive\Desktop\Consulting\Vitalis Files\NEA\Match_Archive"

    # --- Query Date Range ---
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=90)
    
    # --- Snowflake Connection ---
    conn = snowflake.connector.connect(
        user=os.environ.get("NEA_SF_USER"), 
        password=os.environ.get("NEA_SF_PASS"), 
        account=os.environ.get("NEA_SF_ACCT"),
        role="READ_ONLY_NEA",
        warehouse="COMPUTE_WH",
        database="NEA_SALES",
        schema="WHOLESALE"
    )

    # --- Query Wholesale Data ---
    with conn.cursor() as cs:
        cs.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        cs.execute(f"""
            SELECT
                'Wholesale' AS LOCATIONNAME,
                DELIVERYDATE AS TRANSACTIONDATE,
                PRODUCTNAME,
                BRAND AS BRANDNAME,
                PRODUCTSKU,
                WEIGHTUNIT,
                UNITSPERCASE,
                PRODUCTSKU AS "S&OP Category",
                BUYERNAME,
                SUM(QUANTITY) AS QUANTITY,
                SUM(LINETOTAL) AS TOTAL_REVENUE
            FROM VWHOLESALESALES
            WHERE DELIVERYDATE BETWEEN '{start_date}' AND '{end_date}'
                AND LOWER(BUYERNAME) NOT IN (
                    'northeast alternatives - fall river',
                    'near / northeast alternatives retail, llc - seekonk',
                    'near / northeast alternatives retail, llc - new bedford'
                )
            GROUP BY
                DELIVERYDATE,
                PRODUCTNAME,
                BRAND,
                PRODUCTSKU,
                WEIGHTUNIT,
                UNITSPERCASE,
                BUYERNAME
        """)
        rows = cs.fetchall()
        columns = [col[0] for col in cs.description]
        wholesale_df = pd.DataFrame(rows, columns=columns)

    conn.close()

    # --- Convert to Unit Count ---
    def convert_to_units(row):
        try:
            weight_unit = row.get('WEIGHTUNIT', '').strip() if row.get('WEIGHTUNIT') else ''
            product_name = row.get('PRODUCTNAME', '').upper() if row.get('PRODUCTNAME') else ''
            quantity = row.get('QUANTITY', 0)
            units_per_case = row.get('UNITSPERCASE', 1)
            
            # Handle None values
            if quantity is None:
                quantity = 0
            if units_per_case is None:
                units_per_case = 1
                
            quantity = float(quantity)
            units_per_case = float(units_per_case)
            
            # Debug logging for bulk/trim products
            if 'BULK' in product_name or 'TRIM' in product_name:
                print(f"🔧 SALES Converting: {product_name} | {quantity} {weight_unit} → ", end="")
            
            if weight_unit == 'Grams':
                if 'BULK' in product_name or 'TRIM' in product_name:
                    result = quantity * 453.6  # Convert pounds to grams
                    print(f"{result} grams")
                    return result
                else:
                    return quantity  # Already in grams
            elif weight_unit in ['Units', 'Unit']:
                return quantity
            else:
                return quantity * units_per_case
                
        except Exception as e:
            print(f"⚠️ SALES Conversion error: {e} | Row: {row.get('PRODUCTNAME', 'Unknown')}")
            return 0.0

    # Apply conversion (FIXED: removed duplicate line)
    wholesale_df['UNIT_COUNT'] = wholesale_df.apply(convert_to_units, axis=1)

    # --- Mark matches and unmatched ---
    wholesale_df['Matched S&OP Category'] = wholesale_df['S&OP Category']
    wholesale_df['Match Score'] = 100
    wholesale_df['Matched Reference'] = 'wholesale direct'
    wholesale_df['Match Result'] = 'Matched (wholesale clean)'
    wholesale_df.loc[wholesale_df['PRODUCTSKU'].isna(), ['Matched S&OP Category', 'Match Score', 'Matched Reference', 'Match Result']] = [None, None, None, 'Missing SKU']

    # --- Final Output Column Order ---
    final_cols = [
        'LOCATIONNAME', 'TRANSACTIONDATE', 'Matched S&OP Category', 'PRODUCTNAME',
        'TOTAL_REVENUE', 'UNIT_COUNT', 'Match Result', 'Match Score',
        'Matched Reference', 'PRODUCTSKU', 'BRANDNAME'
    ]

    # --- Export ---
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_folder = os.path.join(output_base_dir, f"Wholesale_Run_{timestamp}")
    os.makedirs(run_folder, exist_ok=True)

    matched = wholesale_df[wholesale_df['Matched S&OP Category'].notna()].copy()
    unmatched = wholesale_df[wholesale_df['Matched S&OP Category'].isna()].copy()

    # --- Ensure output column consistency ---
    matched_final = matched[final_cols]
    unmatched_final = unmatched[final_cols]

    # Rename UNIT_COUNT → TOTAL_QUANTITY for consistency with retail outputs
    matched_final = matched_final.rename(columns={"UNIT_COUNT": "TOTAL_QUANTITY"})
    unmatched_final = unmatched_final.rename(columns={"UNIT_COUNT": "TOTAL_QUANTITY"})

    # --- Category Summary ---
    category_summary = matched.groupby(['Matched S&OP Category', 'PRODUCTNAME'], dropna=False).agg({
        'UNIT_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum'
    }).reset_index()
    category_summary.insert(0, 'LOCATIONNAME', 'Wholesale')

    # --- Daily Summary ---
    matched['TRANSACTIONDATE'] = pd.to_datetime(matched['TRANSACTIONDATE']).dt.date
    daily_summary = matched.groupby(['LOCATIONNAME', 'TRANSACTIONDATE', 'Matched S&OP Category'], dropna=False).agg({
        'UNIT_COUNT': 'sum',
        'TOTAL_REVENUE': 'sum'
    }).reset_index()

    # --- Save Files ---
    matched_final.to_csv(os.path.join(run_folder, "matched_sales_with_snop_category.csv"), index=False)
    unmatched_final.to_csv(os.path.join(run_folder, "unmatched_sales_without_snop_category.csv"), index=False)
    category_summary.to_csv(os.path.join(run_folder, "matched_category_summary.csv"), index=False)
    daily_summary.to_csv(os.path.join(run_folder, "daily_category_summary.csv"), index=False)

    print(f"✅ Wholesale Match Complete: {len(matched)}/{len(wholesale_df)}")

    return matched_final.copy(), unmatched_final.copy(), category_summary.copy(), daily_summary.copy()