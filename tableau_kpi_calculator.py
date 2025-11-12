import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

class SimplifiedKPICalculator:
    """
    Simplified KPI Calculator - 30-day focus only
    """
    
    def __init__(self):
        print("🔐 ENV DEBUG")
        print("MY_SF_USER:", "✅ SET" if os.getenv("MY_SF_USER") else "❌ MISSING")
        print("MY_SF_PASS:", "✅ SET" if os.getenv("MY_SF_PASS") else "❌ MISSING")
        print("MY_SF_ACCT:", "✅ SET" if os.getenv("MY_SF_ACCT") else "❌ MISSING")
        
        self.sf_user = os.getenv("MY_SF_USER")
        self.sf_pass = os.getenv("MY_SF_PASS") 
        self.sf_account = os.getenv("MY_SF_ACCT")
        self.sf_warehouse = "COMPUTE_WH"
        self.sf_database = "NEA_FORECASTING"
        self.sf_schema = "PUBLIC"
        
        if not all([self.sf_user, self.sf_pass, self.sf_account]):
            missing = []
            if not self.sf_user: missing.append("MY_SF_USER")
            if not self.sf_pass: missing.append("MY_SF_PASS") 
            if not self.sf_account: missing.append("MY_SF_ACCT")
            raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")
        
    def get_snowflake_connection(self):
        return snowflake.connector.connect(
            user=self.sf_user,
            password=self.sf_pass,
            account=self.sf_account,
            warehouse=self.sf_warehouse,
            database=self.sf_database,
            schema=self.sf_schema
        )
    
    def create_simple_kpi_table(self):
        """Create simplified KPI table with only essential metrics"""
        conn = self.get_snowflake_connection()
        try:
            cursor = conn.cursor()
            
            # Drop and recreate table to ensure schema matches
            cursor.execute("DROP TABLE IF EXISTS simple_inventory_kpis;")
            
            cursor.execute("""
            CREATE TABLE simple_inventory_kpis (
                LOCATION_NAME VARCHAR(100),
                SNOP_CATEGORY VARCHAR(200),
                PRODUCT_NAME VARCHAR(500),
                BRAND_NAME VARCHAR(100),
                INVENTORY_DATE DATE,
                CURRENT_QUANTITY FLOAT,
                
                -- 30-day metrics
                TOTAL_SALES_30D FLOAT,
                LOCATION_DAILY_VELOCITY FLOAT,
                COMBINED_DAILY_VELOCITY FLOAT,
                DAYS_ON_HAND FLOAT,
                
                -- Simple status fields
                INVENTORY_STATUS VARCHAR(50),
                REORDER_POINT FLOAT,
                SUGGESTED_ACTION VARCHAR(100),
                
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
            """)
            
            print("✅ Created simple KPI table successfully")
            
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            raise
        finally:
            conn.close()
    
    def calculate_simple_kpis(self):
        """
        Calculate simple 30-day KPIs using your company's methodology
        """
        conn = self.get_snowflake_connection()
        
        try:
            cursor = conn.cursor()
            
            # Check data availability
            cursor.execute("SELECT COUNT(*) FROM matched_inventory_with_snop_category")
            inv_count = cursor.fetchone()[0]
            print(f"🔍 DEBUG: {inv_count} inventory records found")
            
            cursor.execute("SELECT COUNT(*) FROM matched_sales_with_snop_category WHERE TRANSACTIONDATE >= CURRENT_DATE() - 30")
            sales_count = cursor.fetchone()[0]
            print(f"🔍 DEBUG: {sales_count} sales records found (last 30 days)")
            
            if inv_count == 0:
                print("❌ No inventory data found.")
                return pd.DataFrame()
            
            if sales_count == 0:
                print("❌ No recent sales data found.")
                return pd.DataFrame()
            
            # UPDATED: 30-day KPI calculation with wholesale + retail velocity logic
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
            ),
            -- Sales velocity by location (wholesale + retail)
            sales_30d AS (
                SELECT 
                    LOCATIONNAME as location_name,
                    "Matched S&OP Category" as snop_category,
                    PRODUCTNAME as product_name,
                    BRANDNAME as brand_name,
                    SUM(COALESCE(TOTAL_QUANTITY, 0)) as total_sales_30d
                FROM matched_sales_with_snop_category
                WHERE TRANSACTIONDATE >= CURRENT_DATE() - 30
                AND TOTAL_QUANTITY IS NOT NULL
                AND TOTAL_QUANTITY > 0
                GROUP BY LOCATIONNAME, "Matched S&OP Category", PRODUCTNAME, BRANDNAME
            ),
            -- Total retail velocity for wholesale calculations
            retail_velocity AS (
                SELECT 
                    "Matched S&OP Category" as snop_category,
                    PRODUCTNAME as product_name,
                    BRANDNAME as brand_name,
                    SUM(COALESCE(TOTAL_QUANTITY, 0)) / 30.0 as total_retail_daily_velocity
                FROM matched_sales_with_snop_category
                WHERE TRANSACTIONDATE >= CURRENT_DATE() - 30
                AND TOTAL_QUANTITY IS NOT NULL
                AND TOTAL_QUANTITY > 0
                AND LOCATIONNAME != 'Wholesale'  -- Only retail locations
                GROUP BY "Matched S&OP Category", PRODUCTNAME, BRANDNAME
            )
            SELECT 
                i.location_name,
                i.snop_category,
                i.product_name,
                i.brand_name,
                i.inventory_date,
                i.current_quantity,
                
                -- 30-day sales metrics
                COALESCE(s.total_sales_30d, 0) as total_sales_30d,
                
                -- Location-specific daily sales velocity
                CASE 
                    WHEN COALESCE(s.total_sales_30d, 0) > 0 THEN 
                        s.total_sales_30d / 30.0
                    ELSE 0 
                END as location_daily_velocity,
                
                -- Combined velocity for wholesale (includes retail demand)
                CASE 
                    WHEN i.location_name = 'Wholesale' THEN
                        -- Wholesale velocity = own sales + total retail demand
                        COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)
                    ELSE 
                        -- Retail locations use only their own velocity
                        CASE WHEN COALESCE(s.total_sales_30d, 0) > 0 THEN s.total_sales_30d / 30.0 ELSE 0 END
                END as combined_daily_velocity,
                
                -- Days on hand using appropriate velocity
                CASE 
                    WHEN i.location_name = 'Wholesale' THEN
                        -- Wholesale DOH = inventory / (wholesale velocity + retail velocity)
                        CASE 
                            WHEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) > 0 THEN 
                                ROUND(i.current_quantity / (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)), 1)
                            ELSE 999.9 
                        END
                    ELSE 
                        -- Retail DOH = inventory / own velocity
                        CASE 
                            WHEN COALESCE(s.total_sales_30d, 0) > 0 THEN 
                                ROUND(i.current_quantity / (s.total_sales_30d / 30.0), 1)
                            ELSE 999.9 
                        END
                END as days_on_hand,
                
                -- Simple reorder point based on product type
                CASE 
                    -- Flower products: 14-day supply
                    WHEN LOWER(i.snop_category) LIKE '%flower%' 
                      OR LOWER(i.snop_category) LIKE '%bulk%'
                      OR LOWER(i.snop_category) LIKE '%smalls%'
                      OR LOWER(i.snop_category) LIKE '%tops%'
                      OR LOWER(i.snop_category) LIKE '%3.5g%'
                      OR LOWER(i.snop_category) LIKE '%7g%'
                      OR (LOWER(i.snop_category) LIKE '%preroll%' AND LOWER(i.snop_category) NOT LIKE '%infused%')
                    THEN 
                        CASE 
                            WHEN i.location_name = 'Wholesale' THEN
                                CASE WHEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) > 0 
                                     THEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) * 14 
                                     ELSE 0 END
                            ELSE 
                                CASE WHEN s.total_sales_30d > 0 THEN (s.total_sales_30d / 30.0) * 14 ELSE 0 END
                        END
                    
                    -- Edibles: 30-day supply
                    WHEN LOWER(i.snop_category) LIKE '%gummy%' 
                      OR LOWER(i.snop_category) LIKE '%chocolate%'
                      OR LOWER(i.snop_category) LIKE '%edible%'
                      OR LOWER(i.snop_category) LIKE '%20pk%'
                      OR LOWER(i.snop_category) LIKE '%10pk%'
                    THEN 
                        CASE 
                            WHEN i.location_name = 'Wholesale' THEN
                                CASE WHEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) > 0 
                                     THEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) * 30 
                                     ELSE 0 END
                            ELSE 
                                CASE WHEN s.total_sales_30d > 0 THEN s.total_sales_30d ELSE 0 END
                        END
                    
                    -- Vapes & Concentrates: 21-day supply
                    WHEN LOWER(i.snop_category) LIKE '%vape%'
                      OR LOWER(i.snop_category) LIKE '%cartridge%'
                      OR LOWER(i.snop_category) LIKE '%concentrate%'
                      OR LOWER(i.snop_category) LIKE '%live resin%'
                      OR LOWER(i.snop_category) LIKE '%.5g%'
                    THEN 
                        CASE 
                            WHEN i.location_name = 'Wholesale' THEN
                                CASE WHEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) > 0 
                                     THEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) * 21 
                                     ELSE 0 END
                            ELSE 
                                CASE WHEN s.total_sales_30d > 0 THEN (s.total_sales_30d / 30.0) * 21 ELSE 0 END
                        END
                    
                    -- Default: 21-day supply
                    ELSE 
                        CASE 
                            WHEN i.location_name = 'Wholesale' THEN
                                CASE WHEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) > 0 
                                     THEN (COALESCE(s.total_sales_30d / 30.0, 0) + COALESCE(rv.total_retail_daily_velocity, 0)) * 21 
                                     ELSE 0 END
                            ELSE 
                                CASE WHEN s.total_sales_30d > 0 THEN (s.total_sales_30d / 30.0) * 21 ELSE 0 END
                        END
                END as reorder_point
                
            FROM current_inventory i
            LEFT JOIN sales_30d s ON 
                i.location_name = s.location_name AND
                i.snop_category = s.snop_category AND
                i.product_name = s.product_name AND
                i.brand_name = s.brand_name
            LEFT JOIN retail_velocity rv ON 
                i.snop_category = rv.snop_category AND
                i.product_name = rv.product_name AND
                i.brand_name = rv.brand_name
            WHERE i.current_quantity > 0
            ORDER BY i.location_name, i.snop_category, i.product_name
            """)
            
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            print(f"🔍 DEBUG: Query returned {len(results)} rows")
            
        except Exception as e:
            print(f"❌ Error calculating KPIs: {e}")
            conn.close()
            return pd.DataFrame()
            
        conn.close()
        
        if df.empty:
            print("❌ No KPI data calculated")
            return df

        # Convert to uppercase for consistency
        df.columns = df.columns.str.upper()
        
        print(f"📊 Adding status indicators...")
        
        # Simple inventory status based on days on hand
        def get_status(days_on_hand, combined_velocity):
            if days_on_hand <= 0:
                return 'OUT_OF_STOCK'
            elif combined_velocity == 0:
                return 'NO_RECENT_SALES'
            elif days_on_hand < 7:
                return 'LOW_STOCK'
            elif days_on_hand <= 30:
                return 'OPTIMAL'
            elif days_on_hand <= 60:
                return 'HIGH_STOCK'
            else:
                return 'EXCESS_STOCK'
        
        # Simple suggested action
        def get_action(status):
            action_map = {
                'OUT_OF_STOCK': 'URGENT_REORDER',
                'LOW_STOCK': 'REORDER_NOW',
                'NO_RECENT_SALES': 'REVIEW_PRICING',
                'OPTIMAL': 'MAINTAIN_CURRENT',
                'HIGH_STOCK': 'MAINTAIN_CURRENT',
                'EXCESS_STOCK': 'CONSIDER_PROMOTION'
            }
            return action_map.get(status, 'MAINTAIN_CURRENT')
        
        # Apply status calculations
        df['INVENTORY_STATUS'] = df.apply(
            lambda row: get_status(row['DAYS_ON_HAND'], row['COMBINED_DAILY_VELOCITY']), 
            axis=1
        )
        
        df['SUGGESTED_ACTION'] = df['INVENTORY_STATUS'].apply(get_action)
        
        print(f"✅ KPI calculation complete: {len(df)} products")
        print(f"📊 Status distribution:")
        print(df['INVENTORY_STATUS'].value_counts().to_dict())
        
        return df
    
    def upload_to_snowflake(self, df, table_name):
        """Upload dataframe to Snowflake"""
        if df.empty:
            print(f"⚠️ No data to upload for {table_name}")
            return
            
        conn = self.get_snowflake_connection()
        try:
            # Clear existing data
            conn.cursor().execute(f"DELETE FROM {table_name};")
            
            # Clean dataframe
            df_clean = df.copy()
            df_clean.columns = [str(col).upper().strip() for col in df_clean.columns]
            df_clean = df_clean.reset_index(drop=True)
            
            # Upload
            success, nchunks, nrows, _ = write_pandas(conn, df_clean, table_name.upper())
            print(f"✅ Uploaded {nrows} rows to {table_name}")
            
        except Exception as e:
            print(f"❌ Error uploading to {table_name}: {e}")
            raise
        finally:
            conn.close()
    
    def run_simple_kpi_calculations(self):
        """Execute simplified KPI calculations"""
        print("🚀 Starting Simplified KPI calculations (30-day focus)...")
        
        # Create table
        print("🏗️ Creating simple KPI table...")
        self.create_simple_kpi_table()
        
        # Calculate KPIs
        print("📊 Calculating 30-day KPIs...")
        try:
            kpi_df = self.calculate_simple_kpis()
            if not kpi_df.empty:
                self.upload_to_snowflake(kpi_df, "simple_inventory_kpis")
                
                # Show sample results
                print("\n📋 Sample KPI Results:")
                sample_cols = ['LOCATION_NAME', 'SNOP_CATEGORY', 'CURRENT_QUANTITY', 
                              'TOTAL_SALES_30D', 'LOCATION_DAILY_VELOCITY', 'COMBINED_DAILY_VELOCITY', 
                              'DAYS_ON_HAND', 'INVENTORY_STATUS']
                available_cols = [col for col in sample_cols if col in kpi_df.columns]
                if available_cols:
                    print(kpi_df[available_cols].head(10).to_string())
                
                return {'simple_kpis': len(kpi_df)}
            else:
                return {'simple_kpis': 0}
                
        except Exception as e:
            print(f"❌ KPI calculation failed: {e}")
            return {'simple_kpis': 'FAILED'}

if __name__ == "__main__":
    try:
        calculator = SimplifiedKPICalculator()
        results = calculator.run_simple_kpi_calculations()
        print(f"📊 Final Results: {results}")
    except Exception as e:
        print(f"❌ Simple KPI Calculator failed: {e}")
