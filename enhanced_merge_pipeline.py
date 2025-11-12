import pandas as pd
import os
import snowflake.connector
from retail_cleaning import run_retail_cleaning
from wholesale_cleaning import run_wholesale_cleaning
from snowflake.connector.pandas_tools import write_pandas
from tableau_kpi_calculator import TableauKPICalculator

# --- Environment Variables (GitHub Secrets) ---
SF_USER = os.getenv("MY_SF_USER")
SF_PASS = os.getenv("MY_SF_PASS")
SF_ACCOUNT = os.getenv("MY_SF_ACCT")
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE = "NEA_FORECASTING"
SF_SCHEMA = "PUBLIC"

def upload_to_snowflake(df, table_name):
    """Enhanced upload function with better error handling"""
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
    
    try:
        if "TRANSACTIONDATE" in df.columns:
            conn.cursor().execute(
                f"DELETE FROM {table_name} WHERE TRANSACTIONDATE >= DATEADD(DAY, -90, CURRENT_DATE());"
            )
            print(f"🔄 Cleared {table_name} (last 90 days).")
            
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
        print(f"✅ Uploaded to {table_name}: {nrows} rows")
        
    finally:
        conn.close()

def create_forecast_placeholder_table():
    """
    Create a placeholder forecast table structure for future ML integration
    This allows immediate Tableau development while forecast model is built
    """
    conn = snowflake.connector.connect(
        user=SF_USER, password=SF_PASS, account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA
    )
    
    try:
        # Create forecast table structure
        conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS FORECAST_VS_ACTUALS_SUMMARY (
            LOCATIONNAME VARCHAR(100),
            PERIOD_TYPE VARCHAR(20),
            PERIOD_DATE DATE,
            SNOP_CATEGORY VARCHAR(200),
            BRANDNAME VARCHAR(100),
            FORECAST_QUANTITY FLOAT,
            ACTUAL_QUANTITY FLOAT,
            FORECAST_REVENUE FLOAT,
            ACTUAL_REVENUE FLOAT,
            QUANTITY_VARIANCE FLOAT,
            QUANTITY_VARIANCE_PCT FLOAT,
            REVENUE_VARIANCE FLOAT,
            REVENUE_VARIANCE_PCT FLOAT,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """)
        
        # Insert sample/placeholder data for Tableau development
        conn.cursor().execute("""
        DELETE FROM FORECAST_VS_ACTUALS_SUMMARY WHERE PERIOD_DATE >= CURRENT_DATE() - 30;
        """)
        
        # Generate placeholder forecasts based on recent actuals + some variance
        conn.cursor().execute("""
        INSERT INTO FORECAST_VS_ACTUALS_SUMMARY (
            LOCATIONNAME, PERIOD_TYPE, PERIOD_DATE, SNOP_CATEGORY, BRANDNAME,
            FORECAST_QUANTITY, ACTUAL_QUANTITY, FORECAST_REVENUE, ACTUAL_REVENUE,
            QUANTITY_VARIANCE, QUANTITY_VARIANCE_PCT, REVENUE_VARIANCE, REVENUE_VARIANCE_PCT
        )
        WITH daily_actuals AS (
            SELECT 
                LOCATIONNAME,
                "Matched S&OP Category" as SNOP_CATEGORY,
                BRANDNAME,
                TRANSACTIONDATE,
                SUM(TOTAL_QUANTITY) as ACTUAL_QUANTITY,
                SUM(TOTAL_REVENUE) as ACTUAL_REVENUE
            FROM matched_sales_with_snop_category
            WHERE TRANSACTIONDATE >= CURRENT_DATE() - 30
            GROUP BY LOCATIONNAME, "Matched S&OP Category", BRANDNAME, TRANSACTIONDATE
        ),
        with_forecast AS (
            SELECT *,
                -- Simulate forecast as actual * random factor (0.8 to 1.2)
                ACTUAL_QUANTITY * (0.8 + (RANDOM() * 0.4)) as FORECAST_QUANTITY,
                ACTUAL_REVENUE * (0.8 + (RANDOM() * 0.4)) as FORECAST_REVENUE
            FROM daily_actuals
        )
        SELECT 
            LOCATIONNAME,
            'DAILY' as PERIOD_TYPE,
            TRANSACTIONDATE as PERIOD_DATE,
            SNOP_CATEGORY,
            BRANDNAME,
            FORECAST_QUANTITY,
            ACTUAL_QUANTITY,
            FORECAST_REVENUE,
            ACTUAL_REVENUE,
            ACTUAL_QUANTITY - FORECAST_QUANTITY as QUANTITY_VARIANCE,
            CASE WHEN FORECAST_QUANTITY > 0 
                 THEN ((ACTUAL_QUANTITY - FORECAST_QUANTITY) / FORECAST_QUANTITY) * 100 
                 ELSE NULL END as QUANTITY_VARIANCE_PCT,
            ACTUAL_REVENUE - FORECAST_REVENUE as REVENUE_VARIANCE,
            CASE WHEN FORECAST_REVENUE > 0 
                 THEN ((ACTUAL_REVENUE - FORECAST_REVENUE) / FORECAST_REVENUE) * 100 
                 ELSE NULL END as REVENUE_VARIANCE_PCT
        FROM with_forecast;
        """)
        
        print("✅ Created placeholder forecast vs actuals table")
        
    finally:
        conn.close()

def run_enhanced_pipeline():
    """
    Enhanced pipeline that includes original processing + new Tableau KPIs
    """
    print("🚀 Starting Enhanced NEA Pipeline with Tableau KPIs...")
    
    # --- Step 1: Original Sales Processing ---
    print("📊 Running retail and wholesale sales cleaning...")
    retail_matched, retail_unmatched, _, _ = run_retail_cleaning()
    wholesale_matched, wholesale_unmatched, _, _ = run_wholesale_cleaning()

    # Align columns and combine
    def align_columns(df1, df2):
        common_cols = list(set(df1.columns).intersection(df2.columns))
        return df1[common_cols], df2[common_cols]

    retail_matched, wholesale_matched = align_columns(retail_matched, wholesale_matched)
    retail_unmatched, wholesale_unmatched = align_columns(retail_unmatched, wholesale_unmatched)

    merged_matched = pd.concat([retail_matched, wholesale_matched], ignore_index=True)
    merged_unmatched = pd.concat([retail_unmatched, wholesale_unmatched], ignore_index=True)

    # Upload core sales tables
    upload_to_snowflake(merged_matched, "matched_sales_with_snop_category")
    upload_to_snowflake(merged_unmatched, "unmatched_sales_without_snop_category")
    
    # --- Step 2: Create Enhanced Summary Tables for Tableau ---
    print("📈 Creating enhanced summary tables...")
    
    # Enhanced Category Summary with additional metrics
    enhanced_category_summary = merged_matched.groupby(
        ['LOCATIONNAME', 'Matched S&OP Category', 'BRANDNAME'], dropna=False
    ).agg({
        'TOTAL_QUANTITY': ['sum', 'mean', 'count'],
        'TOTAL_REVENUE': ['sum', 'mean'],
        'TRANSACTIONDATE': ['min', 'max']  # Date range
    }).round(2)
    
    # Flatten column names
    enhanced_category_summary.columns = [
        'TOTAL_QUANTITY_SUM', 'TOTAL_QUANTITY_AVG', 'TRANSACTION_COUNT',
        'TOTAL_REVENUE_SUM', 'TOTAL_REVENUE_AVG', 'FIRST_SALE_DATE', 'LAST_SALE_DATE'
    ]
    enhanced_category_summary = enhanced_category_summary.reset_index()
    
    # Add calculated fields for Tableau
    enhanced_category_summary['AVG_REVENUE_PER_TRANSACTION'] = (
        enhanced_category_summary['TOTAL_REVENUE_SUM'] / enhanced_category_summary['TRANSACTION_COUNT']
    ).round(2)
    
    enhanced_category_summary['AVG_QUANTITY_PER_TRANSACTION'] = (
        enhanced_category_summary['TOTAL_QUANTITY_SUM'] / enhanced_category_summary['TRANSACTION_COUNT']
    ).round(2)
    
    upload_to_snowflake(enhanced_category_summary, "enhanced_category_summary")
    
    # Enhanced Daily Summary with moving averages
    daily_summary = merged_matched.groupby(
        ['LOCATIONNAME', 'TRANSACTIONDATE', 'Matched S&OP Category', 'BRANDNAME'], dropna=False
    ).agg({
        'TOTAL_QUANTITY': 'sum',
        'TOTAL_REVENUE': 'sum'
    }).reset_index()
    
    # Calculate 7-day moving averages (for Tableau trend analysis)
    daily_summary = daily_summary.sort_values(['LOCATIONNAME', 'Matched S&OP Category', 'BRANDNAME', 'TRANSACTIONDATE'])
    
    daily_summary['QUANTITY_7D_MA'] = daily_summary.groupby(
        ['LOCATIONNAME', 'Matched S&OP Category', 'BRANDNAME']
    )['TOTAL_QUANTITY'].transform(lambda x: x.rolling(7, min_periods=1).mean())
    
    daily_summary['REVENUE_7D_MA'] = daily_summary.groupby(
        ['LOCATIONNAME', 'Matched S&OP Category', 'BRANDNAME']
    )['TOTAL_REVENUE'].transform(lambda x: x.rolling(7, min_periods=1).mean())
    
    upload_to_snowflake(daily_summary, "enhanced_daily_summary")
    
    # --- Step 3: Create Placeholder Forecast Table ---
    create_forecast_placeholder_table()
    
    # --- Step 4: Run Tableau KPI Calculations ---
    print("🎯 Calculating Tableau-ready KPIs...")
    kpi_calculator = TableauKPICalculator()
    kpi_results = kpi_calculator.run_all_kpi_calculations()
    
    print("✅ Enhanced Pipeline Complete!")
    print(f"📊 Core Sales: {len(merged_matched)} matched, {len(merged_unmatched)} unmatched")
    print(f"📈 KPI Results: {kpi_results}")
    
    return {
        'sales_matched': len(merged_matched),
        'sales_unmatched': len(merged_unmatched),
        'kpi_calculations': kpi_results
    }

if __name__ == "__main__":
    results = run_enhanced_pipeline()
    print(f"🎉 Pipeline Results: {results}")