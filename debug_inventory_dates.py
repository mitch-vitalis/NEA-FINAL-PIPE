import snowflake.connector
import pandas as pd
import os
from datetime import datetime, date


# Connect to Snowflake
conn = snowflake.connector.connect(
    user="MITCH",
    password="VitalisLLC2024!",
    account="QHCDSDU-NXB10527",
    warehouse="COMPUTE_WH",
    database="NEA_FORECASTING",
    schema="PUBLIC"
)

cursor = conn.cursor()

print("🔍 INVENTORY DATE DEBUGGING")
print("=" * 50)

# Check current date
print(f"Today's date: {date.today()}")
print(f"Expected inventory date: 2025-08-27 (from your pipeline run)")
print()

# Check what's in the KPI results table
print("📊 CURRENT KPI TABLE DATES:")
try:
    cursor.execute("""
    SELECT 
        INVENTORY_DATE,
        COUNT(*) as product_count
    FROM simple_inventory_kpis
    GROUP BY INVENTORY_DATE
    ORDER BY INVENTORY_DATE DESC
    """)
    kpi_dates = cursor.fetchall()
    if kpi_dates:
        print("Current KPI table shows these inventory dates:")
        for date_row in kpi_dates:
            print(f"  {date_row[0]}: {date_row[1]:,} products")
    else:
        print("  ❌ No data in KPI table!")
except Exception as e:
    print(f"  ❌ Error reading KPI table: {e}")
print()

# Check source inventory table
print("📅 SOURCE INVENTORY TABLE DATES:")
cursor.execute("""
SELECT 
    INVENTORYDATE,
    COUNT(*) as record_count
FROM matched_inventory_with_snop_category
WHERE INVENTORYDATE >= '2025-08-20'
GROUP BY INVENTORYDATE
ORDER BY INVENTORYDATE DESC
""")
source_dates = cursor.fetchall()
if source_dates:
    print("Source inventory table contains:")
    for date_row in source_dates:
        print(f"  {date_row[0]}: {date_row[1]:,} records")
else:
    print("  ❌ No recent inventory records found!")
print()

# Check what the KPI query would actually select right now
print("🎯 WHAT KPI QUERY WOULD SELECT RIGHT NOW:")
cursor.execute("""
WITH latest_inventory AS (
    SELECT 
        LOCATIONNAME as location_name,
        "Matched S&OP Category" as snop_category,
        PRODUCTNAME as product_name,
        BRANDNAME as brand_name,
        INVENTORYDATE as inventory_date,
        COALESCE("TOTAL_QUANTITY", 0) as current_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY LOCATIONNAME, "Matched S&OP Category", PRODUCTNAME, BRANDNAME 
            ORDER BY INVENTORYDATE DESC
        ) as rn
    FROM matched_inventory_with_snop_category
    WHERE INVENTORYDATE >= CURRENT_DATE() - 14
    AND "TOTAL_QUANTITY" IS NOT NULL
    AND "TOTAL_QUANTITY" > 0
),
current_inventory AS (
    SELECT 
        location_name,
        snop_category,
        product_name,
        brand_name,
        inventory_date,
        SUM(current_quantity) as current_quantity
    FROM latest_inventory
    WHERE rn = 1
    GROUP BY location_name, snop_category, product_name, brand_name, inventory_date
)
SELECT 
    inventory_date,
    COUNT(*) as products_selected
FROM current_inventory
GROUP BY inventory_date
ORDER BY inventory_date DESC
""")

kpi_selection = cursor.fetchall()
if kpi_selection:
    print("KPI query RIGHT NOW would select:")
    for date_row in kpi_selection:
        print(f"  {date_row[0]}: {date_row[1]:,} products")
else:
    print("  ❌ KPI query selects nothing right now!")

conn.close()

print()
print("🔧 NEXT STEPS:")
print("If you see 2025-08-27 data in source but old dates in KPI table:")
print("1. Re-run: python tableau_kpi_calculator.py")
print("2. The KPI calculator should pick up the fresh 2025-08-27 data")
print()
print("If you still see old dates in the KPI selection query above:")
print("1. There might be an issue with the source data query")
print("2. Check if the inventory pipeline actually updated the right table")