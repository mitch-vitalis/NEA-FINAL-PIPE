import pandas as pd
import re
import os
import unicodedata
import snowflake.connector
from rapidfuzz import process, fuzz
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ——— PARAMETERS ———
STRICT_THRESHOLD  = 75  # token_sort_ratio cutoff for strict rules
PARTIAL_THRESHOLD = 70  # partial_ratio cutoff for edibles backup

# ---------------------- Cleaning Helpers ----------------------

def clean_text(text):
    return str(text).replace('AU:', '').replace('MED:', '').strip().lower() if pd.notna(text) else ''

def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ''
    s = str(text)
    # Decompose accents/special forms, drop non-ascii
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
    # Unify dashes/quotes/whitespace, lowercase
    s = s.replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-')  # – — −
    s = s.replace('\u2018', "'").replace('\u2019', "'").replace('\u00A0', ' ')
    s = re.sub(r'\s+', ' ', s)
    return s.strip().lower()

def extract_grams(text):
    match = re.search(r'(\d+\.?\d*)(g|mg)', str(text).lower())
    if not match:
        return None
    val = float(match.group(1))
    return val if match.group(2) == 'g' else round(val / 1000, 4)

def extract_flavor_keywords(text):
    txt = re.sub(r'[^a-zA-Z\s]', '', str(text).lower())
    for word in ['gummy','gummies','chocolate','hybrid','indica','sativa','dab','fx','nano','rso',
                 'cannatini','edible','infused','distillate','smalls','tops','live','concentrate',
                 'sauce','wax','preroll','pre-roll']:
        txt = txt.replace(word, '')
    return [w for w in txt.split() if len(w) >= 3]

def clean_flavor_for_string(text):
    txt = re.sub(r'[^a-zA-Z\s]', '', str(text).lower())
    for word in ['gummy','gummies','chocolate','hybrid','indica','sativa','dab','fx','nano','rso',
                 'cannatini','edible','infused','distillate','smalls','tops','live','concentrate',
                 'sauce','wax','preroll','pre-roll']:
        txt = txt.replace(word, '')
    return txt.strip()

def extract_strain(text):
    txt = str(text).lower()
    if 'hybrid' in txt: return 'hybrid'
    if 'indica' in txt: return 'indica'
    if 'sativa' in txt: return 'sativa'
    return None

def detect_product_type(name):
    txt = str(name).lower()
    if any(t in txt for t in ['shatter','wax','crumble','batter','sugar','live','resin','rosin','sauce']):
        return 'concentrate'
    if any(t in txt for t in ['gummies','chocolate','drink','edible','capsule','syrup']):
        return 'edible'
    if 'preroll' in txt or 'pre-roll' in txt:
        return 'flower'
    if re.search(r'\b(\d+(\.\d+)?g)\b', txt):
        return 'flower'
    if any(t in txt for t in ['flower','smalls','tops','bulk']):
        return 'flower'
    if any(t in txt for t in ['vape','cartridge']):
        return 'vape'
    return None

# ---------------------- Validation Rules ----------------------

def grams_check(product_grams, match_grams, product_type):
    if not product_grams or not match_grams:
        return True
    if product_type not in ['flower','concentrate','vape','preroll']:
        return True
    if (product_grams > 7 and match_grams <= 3.5) or (match_grams > 7 and product_grams <= 3.5):
        return False
    return abs(product_grams - match_grams) <= 0.05

def flavor_check(product_flavors, match_flavors, product_flavor_str, match_flavor_str):
    if not match_flavors:
        return False
    common = set(product_flavors).intersection(set(match_flavors))
    if len(common) >= 2:
        return True
    return fuzz.ratio(product_flavor_str, match_flavor_str) >= 85

def strain_check(product_strain, matched_category):
    matched = extract_strain(matched_category)
    if product_strain and matched:
        return product_strain == matched
    return True

def pr_lock(product_name, matched_category):
    pn = str(product_name).lower()
    mc = str(matched_category).lower()
    if 'preroll' in mc and 'preroll' not in pn:
        return False
    return True

def strain_strict_lock(product_type, product_strain, matched_category):
    if product_type != 'flower':
        return True
    matched = extract_strain(matched_category)
    if product_strain and matched:
        return product_strain == matched
    return True

def infused_lock(product_name, matched_category):
    pn = str(product_name).lower()
    mc = str(matched_category).lower()
    if ('infused' in pn and 'infused' not in mc) or ('infused' in mc and 'infused' not in pn):
        return False
    return True

def preground_lock(product_name, matched_category):
    pn = str(product_name).lower()
    mc = str(matched_category).lower()
    if any(x in pn for x in ['preground','7g','bulk','ounce']):
        if 'preroll' in mc:
            return False
    return True

def brand_category_lock(product_brand, matched_category):
    if not product_brand or not matched_category:
        return True
    pb = product_brand.strip().lower()
    mc = str(matched_category).lower()
    
    # More flexible brand matching - only enforce for specific cases
    if pb == 'nea fire' and 'nea fire' not in mc and 'nea' not in mc:
        return False
    if pb == 'valorem' and 'valorem' not in mc:
        return False
    
    # For NEA Premium, allow matching to any NEA category
    if pb == 'nea premium' and 'nea' not in mc:
        return False
    
    # For NEA Awarded, allow matching to any NEA category  
    if pb == 'nea awarded' and 'nea' not in mc:
        return False
    
    return True

def category_type_conflict_lock(product_type, matched_category):
    pt = product_type
    mc = str(matched_category).lower()
    if pt == 'flower' and 'concentrate' in mc:
        return False
    if pt == 'concentrate' and 'flower' in mc:
        return False
    return True

def best_match_exact_priority(candidates, product_name):
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[1], fuzz.partial_ratio(x[0], product_name)), reverse=True)
    return candidates[0]

# ---------------------- Packaging Check ----------------------

def packaging_lock(product_name, match_name):
    p = re.search(r'\((\d+)pk\)', str(product_name).lower())
    m = re.search(r'\((\d+)pk\)', str(match_name).lower())
    if p and m:
        return p.group(1) == m.group(1)
    return not p and not m

# ---------------------- Fallback Preroll ----------------------

def fallback_preroll_match(row, catalog_df):
    product   = str(row['PRODUCTNAME']).lower()
    brand     = str(row['BRANDNAME']).lower()
    strain    = extract_strain(product)
    grams     = extract_grams(product)
    pack      = re.search(r'\((\d+)pk\)', product)
    pack_size = int(pack.group(1)) if pack else None

    for _, ref in catalog_df.iterrows():
        rn   = str(ref['PRODUCTNAME']).lower()
        rs   = extract_strain(rn)
        rg   = extract_grams(rn)
        rp   = re.search(r'\((\d+)pk\)', rn)
        rps  = int(rp.group(1)) if rp else None
        rb   = str(ref.get('Brand','')).lower()
        if all([
            'preroll' in product,
            'preroll' in rn,
            strain == rs,
            grams and rg and abs(grams-rg) < 0.05,
            pack_size == rps,
            brand == rb
        ]):
            return ref['SNOPCATEGORY'], 90, ref['PRODUCTNAME'], "Fallback Preroll Match"
    return None, None, None, "No Acceptable Match"

# ---------------------- Matching Logic ----------------------

def _exception_name_keys(raw: str, brand: str) -> list[str]:
    """
    Generate extra match keys ONLY for exact-match lookup, to handle odd spellings
    or mojibake of specific product names without changing global logic.
    """
    keys = set()

    def _add(s: str):
        if s is None:
            return
        # Reuse existing normalization so keys align with catalog dicts
        keys.add(normalize_text(clean_text(s)))

    # Base names (as-is)
    _add(raw)
    _add(f"{raw} - {brand}")

    # Targeted mojibake repairs (name-only, not global). Add more pairs if ever needed.
    replacements = [
        ("Ã©", "é"), ("ã©", "é"),
        ("Ã±", "ñ"), ("ã±", "ñ"),
        ("Ã¼", "ü"), ("ã¼", "ü"),
    ]
    for bad, good in replacements:
        if bad in str(raw):
            fixed = str(raw).replace(bad, good)
            _add(fixed)
            _add(f"{fixed} - {brand}")

    # Opportunistic decode attempts (latin-1/cp1252 → utf-8), scoped to the name only
    for enc in ("latin-1", "cp1252"):
        try:
            fixed = str(raw).encode(enc, "strict").decode("utf-8", "strict")
            if fixed != raw:
                _add(fixed)
                _add(f"{fixed} - {brand}")
        except Exception:
            pass

    return [k for k in keys if k]  # deduped, normalized keys

def match_best_category(row, name_to_grams, name_to_category,
                        reference_names, sop_category_list, catalog_df):
    raw           = row['PRODUCTNAME']
    brand         = row['BRANDNAME']
    alt           = f"{raw} - {brand}"
    cleaned       = row['Cleaned PRODUCTNAME']
    p_grams       = row['PRODUCTGRAMS']
    p_type        = row['ProductType']
    p_flavors     = row['FlavorTokens']
    p_flavor_str  = row['FlavorCleaned']
    p_strain      = row['StrainType']
    raw_norm = normalize_text(clean_text(raw))
    alt_norm = normalize_text(clean_text(alt))

    # exact matches (standard keys)
    if raw_norm in name_to_category:
        return name_to_category[raw_norm], 100, raw_norm, "Matched (Exact Match)"
    if alt_norm in name_to_category:
        return name_to_category[alt_norm], 99, alt_norm, "Matched (Exact Match w/ Brand)"

    # --- Name-only exception keys (no global logic change) ---
    for key in _exception_name_keys(raw, brand):
        if key in name_to_category:
            return name_to_category[key], 100, key, "Matched (Exact Match – Name Exception)"

    # gather fuzzy candidates
    candidates = process.extract(cleaned, reference_names, scorer=fuzz.token_sort_ratio, limit=5)

    # high-confidence override
    if candidates and candidates[0][1] == 100:
        ref = candidates[0][0]
        return name_to_category[ref], 100, ref, "High-Confidence Override"

    # strict-rule fuzzy matching
    valid = []
    for cand, score, _ in candidates:
        if score < STRICT_THRESHOLD:
            continue
        mc = name_to_category.get(cand)
        if not pr_lock(raw, mc): continue
        if not category_type_conflict_lock(p_type, mc): continue
        if 'preroll' in raw.lower() and not packaging_lock(raw, cand): continue
        m_grams      = name_to_grams.get(cand)
        m_flavors    = extract_flavor_keywords(cand)
        m_flavor_str = clean_flavor_for_string(cand)
        if not grams_check(p_grams, m_grams, p_type): continue
        if p_type=='edible' and not flavor_check(p_flavors, m_flavors, p_flavor_str, m_flavor_str): continue
        if not infused_lock(raw, mc): continue
        if not strain_check(p_strain, mc): continue
        if not preground_lock(raw, mc): continue
        if not brand_category_lock(brand, mc): continue
        if not strain_strict_lock(p_type, p_strain, mc): continue
        valid.append((cand, score))

    if valid:
        best = best_match_exact_priority(valid, cleaned)
        return name_to_category[best[0]], best[1], best[0], "Matched (Strict Rules)"

    # edible backup
    if p_type == 'edible':
        backups = process.extract(cleaned, sop_category_list, scorer=fuzz.partial_ratio, limit=5)
        backups = [b for b in backups if b[1] >= PARTIAL_THRESHOLD]
        if backups:
            bk = best_match_exact_priority(backups, cleaned)
            return bk[0], bk[1], bk[0], "Backup S&OP Match"

    return None, None, None, "No Acceptable Match"

# ---------------------- Main Function ----------------------

def run_retail_inventory_cleaning():
    # load product catalog
    conn = snowflake.connector.connect(
        user=os.getenv("MY_SF_USER"),
        password=os.getenv("MY_SF_PASS"),
        account=os.getenv("MY_SF_ACCT"),
        warehouse="COMPUTE_WH",
        database="NEA_FORECASTING",
        schema="PUBLIC"
    )
    with conn.cursor() as cs:
        cs.execute("SELECT * FROM PRODUCT_CATALOG")
        rows = cs.fetchall()
        cols = [c[0] for c in cs.description]
        catalog_df = pd.DataFrame(rows, columns=cols)
    conn.close()

    # pull latest retail inventory
    inv = snowflake.connector.connect(
        user=os.getenv("NEA_SF_USER"),
        password=os.getenv("NEA_SF_PASS"),
        account=os.getenv("NEA_SF_ACCT"),
        warehouse="COMPUTE_WH",
        database="NEA_SALES",
        schema="PUBLIC"
    )
    qry = '''
        WITH latest_date AS (
            SELECT MAX(INVENTORYDATE) AS max_date
            FROM NEA_SALES.PUBLIC.VRETAILINVENTORY
        )
        SELECT *
        FROM NEA_SALES.PUBLIC.VRETAILINVENTORY
        WHERE INVENTORYDATE = (SELECT max_date FROM latest_date)
          AND QUANTITYAVAILABLE IS NOT NULL
          AND MASTERCATEGORY IN ('NEA Flower','NEA MIPs')
          AND QUANTITYAVAILABLE > 0;
    '''
    with inv.cursor() as cs:
        cs.execute(qry)
        rows = cs.fetchall()
        cols = [c[0] for c in cs.description]
        df = pd.DataFrame(rows, columns=cols)
    inv.close()
    print(f"🐛 DEBUG: raw inventory rows pulled = {len(df)}")
    
    # normalize and enrich
    df.columns = df.columns.str.strip()
    catalog_df.columns = catalog_df.columns.str.strip()

    # DEBUG: Check if QUANTITYAVAILABLE exists
    print(f"🔍 DEBUG: Columns after data pull:")
    print(f"   Available columns: {list(df.columns)}")
    print(f"   QUANTITYAVAILABLE present: {'QUANTITYAVAILABLE' in df.columns}")
    if 'QUANTITYAVAILABLE' in df.columns:
        print(f"   QUANTITYAVAILABLE sample: {df['QUANTITYAVAILABLE'].head().tolist()}")
        print(f"   QUANTITYAVAILABLE sum: {df['QUANTITYAVAILABLE'].sum()}")

    catalog_df['Normalized'] = catalog_df['PRODUCTNAME'].apply(lambda x: normalize_text(clean_text(x)))
    catalog_df['GRAMS']      = catalog_df['Normalized'].apply(extract_grams)
    name_to_category         = catalog_df.set_index('Normalized')['SNOPCATEGORY'].to_dict()
    name_to_grams            = catalog_df.set_index('Normalized')['GRAMS'].to_dict()
    reference_names          = list(name_to_category.keys())
    sop_list                 = catalog_df['SNOPCATEGORY'].dropna().str.lower().tolist()

    df['Cleaned PRODUCTNAME'] = df['PRODUCTNAME'].apply(lambda x: normalize_text(clean_text(x)))
    df['PRODUCTGRAMS']        = df['PRODUCTNAME'].apply(extract_grams)
    df['ProductType']         = df['PRODUCTNAME'].apply(detect_product_type)
    df['FlavorTokens']        = df['PRODUCTNAME'].apply(extract_flavor_keywords)
    df['FlavorCleaned']       = df['PRODUCTNAME'].apply(clean_flavor_for_string)
    df['StrainType']          = df['PRODUCTNAME'].apply(extract_strain)
    df['ProductType'] = df.apply(
        lambda row: 'concentrate'
        if ('concentrate' in str(row.get('CATEGORY','')).lower()
            or str(row.get('MASTERCATEGORY','')).lower() == 'nea mips')
        else row['ProductType'],
        axis=1
    )

    # filter brands
    approved = [
        'NEA Fire','NEA Premium','NEA Awarded','Sapura','Cannatini','Valorem',
        'Dab FX','Double Baked','Farm To Fam','SWEETSPOT','Dab FX+',
        'Northeast Alternatives','Higher Celebrations','NEA Pride Jays',''
    ]
    wrong = df[~df['BRANDNAME'].isin(approved)].copy()
    wrong[['Matched S&OP Category','Match Score','Matched Reference','Match Result']] = None, None, None, 'Wrong Brand'
    df = df[df['BRANDNAME'].isin(approved)].reset_index(drop=True)

    # DEBUG: Check columns after brand filtering
    print(f"🔍 DEBUG: Columns after brand filtering:")
    print(f"   QUANTITYAVAILABLE present: {'QUANTITYAVAILABLE' in df.columns}")
    if 'QUANTITYAVAILABLE' in df.columns:
        print(f"   QUANTITYAVAILABLE sum after filtering: {df['QUANTITYAVAILABLE'].sum()}")

    # init match/output columns
    df[['Matched S&OP Category','Match Score','Matched Reference','Match Result']] = None, None, None, None
    df['Failed Checks'] = None

    # matching loop
    for i, row in df.iterrows():
        cat, score, ref, result = match_best_category(
            row, name_to_grams, name_to_category,
            reference_names, sop_list, catalog_df
        )
        df.at[i,'Matched S&OP Category'] = cat
        df.at[i,'Match Score']           = score
        df.at[i,'Matched Reference']     = ref
        df.at[i,'Match Result']          = result

        # fallback preroll
        if pd.isna(cat) and row['ProductType']=='flower' and 'preroll' in row['Cleaned PRODUCTNAME']:
            cat2, sc2, ref2, res2 = fallback_preroll_match(row, catalog_df)
            if cat2:
                df.at[i,'Matched S&OP Category'] = cat2
                df.at[i,'Match Score']           = sc2
                df.at[i,'Matched Reference']     = ref2
                df.at[i,'Match Result']          = res2

        # bulk override
        if pd.isna(df.at[i,'Matched S&OP Category']):
            pn = row['PRODUCTNAME'].lower()
            bn = row['BRANDNAME'].lower()
            if 'bulk' in pn:
                if 'nea fire' in bn:
                    df.at[i,'Matched S&OP Category'] = 'NEA Fire Bulk Flower g'
                    df.at[i,'Match Score']           = 100
                    df.at[i,'Matched Reference']     = 'bulk name brand rule'
                    df.at[i,'Match Result']          = 'Bulk Override'
                else:
                    df.at[i,'Matched S&OP Category'] = 'NEA Bulk Flower g'
                    df.at[i,'Match Score']           = 95
                    df.at[i,'Matched Reference']     = 'bulk name rule'
                    df.at[i,'Match Result']          = 'Bulk Override'

        # log failed locks for truly unmatched
        if df.at[i,'Match Result'] == 'No Acceptable Match':
            cleaned = row['Cleaned PRODUCTNAME']
            p_type  = row['ProductType']
            p_grams = row['PRODUCTGRAMS']
            p_strain= row['StrainType']
            brand   = row['BRANDNAME']
            raw     = row['PRODUCTNAME']

            best = process.extractOne(cleaned, reference_names, scorer=fuzz.token_sort_ratio)
            cand, _, _ = best if best else (None, None, None)
            mc = name_to_category.get(cand, "")

            checks = {
                'pr_lock': pr_lock(raw, mc),
                'type_conflict': category_type_conflict_lock(p_type, mc),
                'packaging': packaging_lock(raw, cand) if 'preroll' in raw.lower() else True,
                'brand_lock': brand_category_lock(brand, mc),
                'infused_lock': infused_lock(raw, mc),
                'strain_check': strain_check(p_strain, mc),
                'strain_strict_lock': strain_strict_lock(p_type, p_strain, mc),
                'grams_check': grams_check(p_grams, name_to_grams.get(cand), p_type),
            }
            failed = [name for name, ok in checks.items() if not ok]
            df.at[i,'Failed Checks'] = ",".join(failed)

    # split matched / unmatched by presence of a category (PRESERVE ALL COLUMNS)
    matched = df[
        df['Matched S&OP Category'].notna() &
        (df['Matched S&OP Category'].str.strip()!='')
    ].copy()

    unmatched_core = df[
        df['Matched S&OP Category'].isna() |
        (df['Matched S&OP Category'].str.strip()=='')
    ].copy()

    # Add wrong brand items to unmatched (but preserve columns)
    unmatched = pd.concat([wrong, unmatched_core], ignore_index=True)

    # DEBUG: Verify QUANTITYAVAILABLE is preserved
    print(f"🔍 DEBUG after split:")
    print(f"   Matched shape: {matched.shape}")
    print(f"   QUANTITYAVAILABLE in matched: {'QUANTITYAVAILABLE' in matched.columns}")
    if 'QUANTITYAVAILABLE' in matched.columns:
        print(f"   Matched QUANTITYAVAILABLE sum: {matched['QUANTITYAVAILABLE'].sum()}")
    print(f"   Unmatched shape: {unmatched.shape}")
    print(f"   QUANTITYAVAILABLE in unmatched: {'QUANTITYAVAILABLE' in unmatched.columns}")

    # guard‐rail assertion to ensure no row loss
    raw_count = len(df) + len(wrong)
    out_count = len(matched) + len(unmatched)
    assert raw_count == out_count, f"⚠ Row count mismatch raw={raw_count}, out={out_count}"

    # Map QUANTITYAVAILABLE to TOTAL_QUANTITY for consistency with wholesale
    if 'QUANTITYAVAILABLE' in matched.columns:
        matched['TOTAL_QUANTITY'] = pd.to_numeric(matched['QUANTITYAVAILABLE'], errors='coerce').fillna(0)
        print(f"✅ Added TOTAL_QUANTITY to retail matched inventory: {matched['TOTAL_QUANTITY'].sum()} total units")
    else:
        print(f"❌ QUANTITYAVAILABLE not found in matched DataFrame!")
        matched['TOTAL_QUANTITY'] = 0
        
    if 'QUANTITYAVAILABLE' in unmatched.columns:
        unmatched['TOTAL_QUANTITY'] = pd.to_numeric(unmatched['QUANTITYAVAILABLE'], errors='coerce').fillna(0)
        print(f"✅ Added TOTAL_QUANTITY to retail unmatched inventory: {unmatched['TOTAL_QUANTITY'].sum()} total units")
    else:
        print(f"❌ QUANTITYAVAILABLE not found in unmatched DataFrame!")
        unmatched['TOTAL_QUANTITY'] = 0

    return matched, unmatched
