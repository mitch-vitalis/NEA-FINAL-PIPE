def run_retail_cleaning():
    import pandas as pd
    from rapidfuzz import process, fuzz
    import re
    import os
    import datetime
    import shutil
    import snowflake.connector
    import datetime


    # --- import snowflake product catalog ---
    # --- Snowflake Catalog Connection ---
    catalog_conn = snowflake.connector.connect(
    user=os.getenv("MY_SF_USER"),
    password=os.getenv("MY_SF_PASS"),
    account=os.getenv("MY_SF_ACCT"),  # update this
    warehouse="COMPUTE_WH",
    database="NEA_FORECASTING",
    schema="PUBLIC"
    )
    with catalog_conn.cursor() as cs:
        cs.execute("SELECT * FROM PRODUCT_CATALOG")
        rows = cs.fetchall()
        columns = [col[0] for col in cs.description]
        catalog_df = pd.DataFrame(rows, columns=columns)
    catalog_conn.close()




    # --- CONFIG ---
    test_mode = False
    
    # --- Query Date Range ---
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=90)
    # --- Load Data ---
    #product_catalog_file = r"C:\Users\Mitch\OneDrive\Desktop\VS Projects\NEA Projects\Product Catelog.csv"

    # -- OLD CONNECTIONS --
        #sales_export_file = r"C:\Users\Mitch\OneDrive\Desktop\VS Projects\NEA Projects\Time_Retail_Sales.csv"
    print("🧪 ENV DEBUG")
    print("MY_SF_USER:", os.getenv("MY_SF_USER"))
    print("MY_SF_PASS:", os.getenv("MY_SF_PASS"))
    print("MY_SF_ACCT:", os.getenv("MY_SF_ACCT"))


    # Snowflake connection test
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("NEA_SF_USER"),
            password=os.getenv("NEA_SF_PASS"),
            account=os.getenv("NEA_SF_ACCT"),
            role="READ_ONLY_NEA",
            warehouse="COMPUTE_WH",
            database="NEA_SALES",
            schema="PUBLIC"
        )

        with conn.cursor() as cs:
            cs.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")  # 🔄 Disable caching
            cs.execute("SELECT CURRENT_VERSION()")
            version = cs.fetchone()[0]
            print(f"✅ Connected to Snowflake version: {version}")

            # Add random comment to force SQL uniqueness
            import random
            query_run_id = random.randint(1000, 9999)

            cs.execute(f"""
                -- FORCE REFRESH: {query_run_id}
                SELECT
                    LOCATIONNAME,
                    PRODUCTID,
                    PRODUCTNAME,
                    SKU,
                    MASTERCATEGORY,
                    BRANDNAME,
                    PRODUCTGRAMS,
                    SUM(PRODUCTGRAMS) AS WEIGHTSOLD,
                    CATEGORY,
                    TRANSACTIONDATE,
                    COUNT(DISTINCT TRANSACTIONID) AS TOTAL_TRANSACTIONS,
                    SUM(QUANTITY) AS TOTAL_QUANTITY,
                    SUM(netsaleforitem) AS TOTAL_REVENUE,
                    AVG(UNITCOST) AS AVG_UNIT_COST
                FROM (
                    SELECT
                        LOCATIONNAME,
                        PRODUCTID,
                        PRODUCTNAME,
                        SKU,
                        MASTERCATEGORY,
                        BRANDNAME,
                        PRODUCTGRAMS,
                        CATEGORY,
                        TRANSACTIONDATE,
                        TRANSACTIONID,
                        NETWEIGHT,
                        QUANTITY,
                        NETSALEFORITEM,
                        UNITCOST
                    FROM NEA_SALES.PUBLIC.VSALES
                    WHERE
                        TRANSACTIONTYPE ILIKE 'Retail'
                        AND TRANSACTIONDATE BETWEEN '{start_date}' AND '{end_date}'
                        AND MASTERCATEGORY IN ('NEA Flower', 'NEA MIPs')
                        AND RETURNDATE IS NULL
                        AND ISVOID = 'false'
                        AND 1 = 1 -- Force invalidate: {query_run_id}
                )
                GROUP BY
                    LOCATIONNAME,
                    PRODUCTID,
                    PRODUCTNAME,
                    PRODUCTGRAMS,
                    SKU,
                    MASTERCATEGORY,
                    BRANDNAME,
                    CATEGORY,
                    TRANSACTIONDATE
            """)
            rows = cs.fetchall()
            columns = [col[0] for col in cs.description]
            sales_export_df = pd.DataFrame(rows, columns=columns)
            if 'TRANSACTIONDATE' in sales_export_df.columns:
                sales_export_df['TRANSACTIONDATE'] = pd.to_datetime(sales_export_df['TRANSACTIONDATE']).dt.date

            print("✅ Sample data:")
            print(sales_export_df.head())
            print(f"✅ Query date range: {start_date} to {end_date}")


    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    if 'sales_export_df' not in locals():
        raise RuntimeError("❌ sales_export_df was never defined. Likely due to Snowflake connection or query failure.")

    product_catalog_df = catalog_df
    #sales_export_df = pd.read_csv(sales_export_file)

    # --- Clean Column Names ---
    product_catalog_df.columns = product_catalog_df.columns.str.strip()
    sales_export_df.columns = sales_export_df.columns.str.strip()

    # --- Cleaning Functions ---
    def clean_text(text):
        if pd.isna(text):
            return ''
        return str(text).replace('AU:', '').replace('MED:', '').strip().lower()

    def extract_grams(text):
        if pd.isna(text):
            return None
        match = re.search(r'(\d+\.?\d*)(g|mg)', str(text).lower())
        if match:
            val, unit = float(match.group(1)), match.group(2)
            return val if unit == 'g' else round(val / 1000, 4)
        return None

    def extract_flavor_keywords(text):
        if pd.isna(text):
            return []
        flavor = str(text).lower()
        for word in ['gummy', 'gummies', 'chocolate', 'hybrid', 'indica', 'sativa', 'dab', 'fx', 'nano', 'rso', 'cannatini', 'edible', 'infused', 'distillate', 'smalls', 'tops', 'live', 'concentrate', 'sauce', 'wax', 'preroll', 'pre-roll']:
            flavor = flavor.replace(word, '')
        flavor = re.sub(r'[^a-zA-Z\s]', '', flavor)
        return [w for w in flavor.split() if len(w) >= 3]

    def detect_product_type(name):
        if pd.isna(name):
            return None
        text = str(name).lower()
        if any(t in text for t in ['flower', 'pre-roll', 'preroll', 'smalls', 'tops']):
            return 'flower'
        if any(t in text for t in ['vape', 'cartridge']):
            return 'vape'
        if any(t in text for t in ['shatter', 'wax', 'crumble', 'batter', 'sugar', 'live', 'resin', 'rosin', 'sauce']):
            return 'concentrate'
        if any(t in text for t in ['gummies', 'chocolate', 'drink', 'edible', 'capsule', 'syrup']):
            return 'edible'
        return None

    def clean_flavor_for_string(text):
        if pd.isna(text):
            return ''
        flavor = str(text).lower()
        for word in ['gummy', 'gummies', 'chocolate', 'hybrid', 'indica', 'sativa', 'dab', 'fx', 'nano', 'rso', 'cannatini', 'edible', 'infused', 'distillate', 'smalls', 'tops', 'live', 'concentrate', 'sauce', 'wax', 'preroll', 'pre-roll']:
            flavor = flavor.replace(word, '')
        flavor = re.sub(r'[^a-zA-Z\s]', '', flavor)
        return flavor.strip()

    def extract_strain(text):
        text = str(text).lower()
        if 'hybrid' in text:
            return 'hybrid'
        if 'indica' in text:
            return 'indica'
        if 'sativa' in text:
            return 'sativa'
        return None

    approved_brands = [
    'NEA Fire', 'NEA Premium', 'NEA Awarded', 'Sapura', 'Cannatini', 'Valorem',
    'Dab FX', 'Double Baked', 'Farm To Fam', 'SWEETSPOT', 'Dab FX+', 'Northeast Alternatives',
    'Higher Celebrations', 'NEA Pride Jays', ''
    ]

    # Case-insensitive brand gate
    sales_export_df['__brand_lc'] = sales_export_df['BRANDNAME'].astype(str).str.strip().str.lower()
    approved_lc = {b.lower() for b in approved_brands}

    wrong_brand_mask = ~sales_export_df['__brand_lc'].isin(approved_lc)
    wrong_brand_df = sales_export_df.loc[wrong_brand_mask].copy()
    for col in ['Matched S&OP Category', 'Match Score', 'Matched Reference', 'Match Result', 'Suggested Match']:
        if col not in wrong_brand_df.columns:
            wrong_brand_df[col] = None
    wrong_brand_df['Match Result'] = "Wrong Brand"

    sales_export_df = sales_export_df.loc[~wrong_brand_mask].drop(columns='__brand_lc').reset_index(drop=True)
    print(f"✅ Filtered to {len(sales_export_df)} rows with approved brands only.")

    # Pre-cleaning Raw Match Lookup
    
    if 'PRODUCTNAME' not in product_catalog_df.columns:
        raise KeyError("❌ Column 'PRODUCTNAME' is missing from product_catalog_df. Please verify the PRODUCT_CATALOG table structure.")
    raw_match_map = product_catalog_df.set_index('PRODUCTNAME')['SNOPCATEGORY'].dropna().to_dict()
    

    # Downstream cleaning steps
    sales_export_df['Cleaned PRODUCTNAME'] = sales_export_df['PRODUCTNAME'].apply(clean_text)
    sales_export_df['PRODUCTGRAMS'] = sales_export_df['PRODUCTNAME'].apply(extract_grams)
    sales_export_df['ProductType'] = sales_export_df['PRODUCTNAME'].apply(detect_product_type)
    sales_export_df['FlavorTokens'] = sales_export_df['PRODUCTNAME'].apply(extract_flavor_keywords)
    sales_export_df['FlavorCleaned'] = sales_export_df['PRODUCTNAME'].apply(clean_flavor_for_string)
    sales_export_df['StrainType'] = sales_export_df['PRODUCTNAME'].apply(extract_strain)

    product_catalog_df['Normalized Name'] = product_catalog_df['PRODUCTNAME'].str.lower()
    product_catalog_df['GRAMS'] = product_catalog_df['Normalized Name'].apply(extract_grams)
    reference_names = product_catalog_df['Normalized Name'].dropna().unique().tolist()
    name_to_category = product_catalog_df.set_index('Normalized Name')['SNOPCATEGORY'].to_dict()
    name_to_grams = product_catalog_df.set_index('Normalized Name')['GRAMS'].to_dict()
    sop_category_list = product_catalog_df['SNOPCATEGORY'].dropna().str.lower().tolist()

    # --- Matching Logic ---
    def grams_check(product_grams, match_grams, product_type):
        if not product_grams or not match_grams:
            return True  # Skip check if grams info is missing
        if product_type not in ['flower', 'concentrate', 'vape', 'preroll']:
            return True

        # Prevent bulk (e.g., >7g) matching with small packs (e.g., 3.5g)
        if (product_grams > 7 and match_grams <= 3.5) or (match_grams > 7 and product_grams <= 3.5):
            return False

        # Standard tolerance for near-equal match
        return abs(product_grams - match_grams) <= 0.05

    def flavor_check(product_flavors, match_flavors, product_flavor_str, match_flavor_str):
        if not match_flavors:
            return False
        common = set(product_flavors).intersection(set(match_flavors))
        if len(common) >= 2:
            return True
        string_score = fuzz.ratio(product_flavor_str, match_flavor_str)
        return string_score >= 85

    def strain_check(product_strain, matched_category):
        matched_strain = extract_strain(matched_category)
        if product_strain and matched_strain:
            return product_strain == matched_strain
        return True

    def pr_lock(product_name, matched_category):
        matched_category = str(matched_category).lower()
        if 'pr' in product_name.lower() or 'preroll' in product_name.lower():
            if 'concentrate' in matched_category or 'vape' in matched_category:
                return False
        return True

    def strain_strict_lock(product_type, product_strain, matched_category):
        if product_type != 'flower':
            return True
        matched_strain = extract_strain(matched_category)
        if product_strain and matched_strain:
            return product_strain == matched_strain
        return False

    def infused_lock(product_name, matched_category):
        matched_category = str(matched_category).lower()
        product_name = str(product_name).lower()
        if ('infused' in product_name and 'infused' not in matched_category) or ('infused' in matched_category and 'infused' not in product_name):
            return False
        return True

    def preground_lock(product_name, matched_category):
        if any(x in str(product_name).lower() for x in ['preground', '7g', 'bulk', 'ounce']):
            if 'preroll' in str(matched_category).lower():
                return False
        return True

    def brand_category_lock(product_brand, matched_category):
        if not matched_category or not product_brand:
            return True
        product_brand = product_brand.strip().lower()
        matched_category = str(matched_category).lower()
        if product_brand == "nea fire" and "nea fire" not in matched_category:
            return False
        if product_brand == "nea awarded" and "nea awarded" not in matched_category:
            return False
        if product_brand == "nea premium" and "nea premium" not in matched_category:
            return False
        if product_brand == "valorem" and "valorem" not in matched_category:
            return False
        return True

    def best_match_exact_priority(candidates, product_name):
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[1], fuzz.partial_ratio(x[0], product_name)), reverse=True)
        return candidates[0]

    def match_best_category(row, name_to_grams, name_to_category, reference_names, sop_category_list):
        product_name = row['PRODUCTNAME']
        cleaned_name = row['Cleaned PRODUCTNAME']
        product_grams = row['PRODUCTGRAMS']
        product_type = row['ProductType']
        flavor_tokens = row['FlavorTokens']
        flavor_string = row['FlavorCleaned']
        product_strain = row['StrainType']

        candidates = process.extract(cleaned_name, reference_names, scorer=fuzz.token_sort_ratio, limit=5)
        valid_matches = []

        for match_name, score, _ in candidates:
            ref_grams = name_to_grams.get(match_name)
            match_flavor_tokens = extract_flavor_keywords(match_name)
            match_flavor_string = clean_flavor_for_string(match_name)
            matched_category = name_to_category.get(match_name, "")

            if not grams_check(product_grams, ref_grams, product_type):
                continue
            if product_type == 'edible' and not flavor_check(flavor_tokens, match_flavor_tokens, flavor_string, match_flavor_string):
                continue
            if not pr_lock(product_name, matched_category):
                continue
            if not infused_lock(product_name, matched_category):
                continue
            if not strain_check(product_strain, matched_category):
                continue
            if not preground_lock(product_name, matched_category):
                continue
            if not brand_category_lock(row.get('BRANDNAME', ''), matched_category):
                continue
            if not strain_strict_lock(product_type, product_strain, matched_category):
                continue
            if score >= 75:
                valid_matches.append((match_name, score))

        if valid_matches:
            best = best_match_exact_priority(valid_matches, cleaned_name)
            if best:
                return name_to_category[best[0]], best[1], best[0], "Matched (Strict Rules)"

        if product_type == 'edible':
            backup_candidates = process.extract(cleaned_name, sop_category_list, scorer=fuzz.partial_ratio, limit=5)
            backup_candidates = [c for c in backup_candidates if c[1] >= 70]
            if backup_candidates:
                backup = best_match_exact_priority(backup_candidates, cleaned_name)
                if backup:
                    return backup[0], backup[1], backup[0], "Backup S&OP Match"

        return None, None, None, "No Acceptable Match"
    def fallback_preroll_match(row, catalog_df):
        product = str(row['PRODUCTNAME']).lower()
        brand = str(row['BRANDNAME']).lower()
        strain = extract_strain(product)
        grams = extract_grams(product)

        # Match pack size
        pack_match = re.search(r'\((\d+)pk\)', product)
        pack_size = int(pack_match.group(1)) if pack_match else None

        for _, ref_row in catalog_df.iterrows():
            ref_name = str(ref_row['PRODUCTNAME']).lower()
            ref_brand = str(ref_row.get('Brand', '')).lower()
            ref_strain = extract_strain(ref_name)
            ref_grams = extract_grams(ref_name)
            ref_pack = re.search(r'\((\d+)pk\)', ref_name)
            ref_pack_size = int(ref_pack.group(1)) if ref_pack else None

            if all([
                'preroll' in product,
                'preroll' in ref_name,
                strain == ref_strain,
                grams and ref_grams and abs(grams - ref_grams) < 0.05,
                pack_size == ref_pack_size,
                brand == ref_brand
            ]):
                return ref_row['SNOPCATEGORY'], 90, ref_row['PRODUCTNAME'], "Fallback Preroll Match"

        return None, None, None, "No Acceptable Match"

    # --- Apply Matching ---
    for idx, row in sales_export_df.iterrows():
        raw_name = row['PRODUCTNAME']
        if raw_name in raw_match_map:
            sales_export_df.at[idx, 'Matched S&OP Category'] = raw_match_map[raw_name]
            sales_export_df.at[idx, 'Match Score'] = 100
            sales_export_df.at[idx, 'Matched Reference'] = raw_name
            sales_export_df.at[idx, 'Match Result'] = "Matched (Exact Match)"
            continue

        alt_key = f"{raw_name} - {row['BRANDNAME']}".strip()
        if alt_key in raw_match_map:
            sales_export_df.at[idx, 'Matched S&OP Category'] = raw_match_map[alt_key]
            sales_export_df.at[idx, 'Match Score'] = 99
            sales_export_df.at[idx, 'Matched Reference'] = alt_key
            sales_export_df.at[idx, 'Match Result'] = "Matched (Exact Match w/ Brand)"
            continue

        cat, score, ref, result = match_best_category(
            row, name_to_grams, name_to_category, reference_names, sop_category_list
        )

        # Fallback: structured pre-roll match
        if not cat and row['ProductType'] == 'flower' and 'preroll' in row['Cleaned PRODUCTNAME']:
            cat, score, ref, result = fallback_preroll_match(row, product_catalog_df)

        sales_export_df.at[idx, 'Matched S&OP Category'] = cat
        sales_export_df.at[idx, 'Match Score'] = score
        sales_export_df.at[idx, 'Matched Reference'] = ref
        sales_export_df.at[idx, 'Match Result'] = result


    # --- Assign Bulk Flower Category for Unmatched Products ---
    for idx, row in sales_export_df.iterrows():
        if pd.isna(row['Matched S&OP Category']) or row['Matched S&OP Category'] == "":
            product_name = str(row['PRODUCTNAME']).lower()
            brand_name = str(row['BRANDNAME']).lower()

            if "bulk" in product_name:
                if "nea fire" in brand_name or "nea fire" in product_name:
                    sales_export_df.at[idx, 'Matched S&OP Category'] = "NEA Fire Bulk Flower g"
                    sales_export_df.at[idx, 'Match Score'] = 100
                    sales_export_df.at[idx, 'Matched Reference'] = "bulk name brand rule"
                    sales_export_df.at[idx, 'Match Result'] = "Bulk Override"
                else:
                    sales_export_df.at[idx, 'Matched S&OP Category'] = "NEA Bulk Flower g"
                    sales_export_df.at[idx, 'Match Score'] = 95
                    sales_export_df.at[idx, 'Matched Reference'] = "bulk name rule"
                    sales_export_df.at[idx, 'Match Result'] = "Bulk Override"

    # --- Save Outputs ---
    output_folder = r"C:\Users\Mitch\OneDrive\Desktop\Consulting\Vitalis Files\NEA"

    # Required columns
    required_cols = [
        'LOCATIONNAME', 'Matched S&OP Category', 'PRODUCTNAME',
        'TOTAL_QUANTITY', 'TOTAL_REVENUE', 'WEIGHTSOLD',
        'Match Result', 'Match Score', 'Matched Reference'
    ]

    # Include TRANSACTIONDATE if present
    if 'TRANSACTIONDATE' in sales_export_df.columns and 'TRANSACTIONDATE' not in required_cols:
        required_cols.insert(1, 'TRANSACTIONDATE')

    # Only add non-duplicate product/brand columns
    product_brand_cols = [
        col for col in sales_export_df.columns
        if (col.startswith('PRODUCT') or col.startswith('BRAND')) and col not in required_cols
    ]

    # --- Ensure TRANSACTIONDATE is preserved in matched_final ---
    required_cols = [
        'LOCATIONNAME', 'Matched S&OP Category', 'PRODUCTNAME',
        'TOTAL_QUANTITY', 'TOTAL_REVENUE', 'WEIGHTSOLD',
        'Match Result', 'Match Score', 'Matched Reference'
    ]

    # Add TRANSACTIONDATE if it's in the dataframe
    if 'TRANSACTIONDATE' in sales_export_df.columns and 'TRANSACTIONDATE' not in required_cols:
        required_cols.insert(1, 'TRANSACTIONDATE')

    # Add remaining product/brand columns that aren't already in required_cols
    product_brand_cols = [
        col for col in sales_export_df.columns
        if (col.startswith('PRODUCT') or col.startswith('BRAND')) and col not in required_cols
    ]
    # --- Build matched_final with TRANSACTIONDATE if available ---
    required_cols = [
        'LOCATIONNAME', 'Matched S&OP Category', 'PRODUCTNAME',
        'TOTAL_QUANTITY', 'TOTAL_REVENUE', 'WEIGHTSOLD',
        'Match Result', 'Match Score', 'Matched Reference'
    ]

    # Add TRANSACTIONDATE if it's present in the DataFrame
    if 'TRANSACTIONDATE' in sales_export_df.columns:
        required_cols.insert(1, 'TRANSACTIONDATE')

    # Add any PRODUCT or BRAND columns not already included
    product_brand_cols = [
        col for col in sales_export_df.columns
        if (col.startswith('PRODUCT') or col.startswith('BRAND')) and col not in required_cols
    ]

    # Debug print before building
    print("✅ TRANSACTIONDATE present in sales_export_df:", 'TRANSACTIONDATE' in sales_export_df.columns)
    print("✅ Final required_cols:", required_cols)

    # Inclusion rule: any row with a category is "matched" (captures Matched, Backup, and Bulk Override)
    include_mask = sales_export_df['Matched S&OP Category'].notna() & (sales_export_df['Matched S&OP Category'] != "")

    matched_final = sales_export_df.loc[
        include_mask,
        required_cols + product_brand_cols
    ].copy()

    # Unmatched is the true complement + wrong_brand_df
    unmatched_core = sales_export_df.loc[~include_mask].copy()
    unmatched_final = pd.concat([wrong_brand_df, unmatched_core], ignore_index=True)

    print("✅ matched_final columns (final):", matched_final.columns.tolist())

    # Accounting guardrail: ensure no silent drops (validate only the true partition of the filtered source)
    _total_rows = len(sales_export_df)
    _core_rows = len(matched_final) + len(unmatched_core)
    if _core_rows != _total_rows:
        raise AssertionError(
            f"Row accounting mismatch within filtered source: matched ({len(matched_final)}) + unmatched_core ({len(unmatched_core)}) != source ({_total_rows})."
        )

    # Informative message that unmatched_final also includes wrong_brand_df for auditing
    print(
        "✅ Row accounting OK within filtered source: "
        f"matched ({len(matched_final)}) + unmatched_core ({len(unmatched_core)}) = source ({_total_rows}). "
        f"(Plus {len(wrong_brand_df)} wrong-brand rows appended to unmatched_final for audit.)"
    )


    # --- Category Summary Report ---
    category_summary = matched_final.groupby(['Matched S&OP Category', 'PRODUCTNAME'], dropna=False).agg({
        'TOTAL_QUANTITY': 'sum',
        'TOTAL_REVENUE': 'sum',
        'WEIGHTSOLD': 'sum'
    }).reset_index()

    # --- Daily Category Summary Report ---
    daily_summary = matched_final.groupby(
        ['LOCATIONNAME', 'TRANSACTIONDATE', 'Matched S&OP Category'], dropna=False
    ).agg({
        'TOTAL_QUANTITY': 'sum',
        'TOTAL_REVENUE': 'sum'
    }).reset_index()

    # --- Output Files ---
    matched_output_path = os.path.join(output_folder, "matched_sales_with_snop_category.csv")
    unmatched_output_path = os.path.join(output_folder, "unmatched_sales_without_snop_category.csv")
    summary_output_path = os.path.join(output_folder, "matched_category_summary.csv")
    daily_output_path = os.path.join(output_folder, "daily_category_summary.csv")

#     matched_final.to_csv(matched_output_path, index=False)
#     unmatched_final.to_csv(unmatched_output_path, index=False)
#     category_summary.to_csv(summary_output_path, index=False)
#     daily_summary.to_csv(daily_output_path, index=False)

    if not test_mode:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        archive_folder = os.path.join(output_folder, f"Match_Archive", f"Run_{timestamp}")
        os.makedirs(archive_folder, exist_ok=True)
#         shutil.move(matched_output_path, os.path.join(archive_folder, os.path.basename(matched_output_path)))
#         shutil.move(unmatched_output_path, os.path.join(archive_folder, os.path.basename(unmatched_output_path)))
#         shutil.move(summary_output_path, os.path.join(archive_folder, os.path.basename(summary_output_path)))
#         shutil.move(daily_output_path, os.path.join(archive_folder, os.path.basename(daily_output_path)))

    # --- Summary ---
    total = len(sales_export_df)
    matched = len(matched_final)
    unmatched = len(unmatched_final)
    match_rate = round(matched / total * 100, 2)

    print(f"✅ Final Match Rate: {match_rate}% ({matched}/{total} matched)")
    print(f"✅ Saved to archive folder: {archive_folder}")
    print(f"✅ Query date range: {start_date} to {end_date}")

    return matched_final, unmatched_final, category_summary, daily_summary
