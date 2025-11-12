import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

class TableauKPICalculator:
    """
    Calculates inventory and forecasting KPIs optimized for Tableau consumption
    """
    
    def __init__(self):
        self.sf_user = os.getenv("MY_SF_USER")
        self.sf_pass = os.getenv("MY_SF_PASS") 
        self.sf_account = os.getenv("MY_SF_ACCT")
        self.sf_warehouse = "COMPUTE_WH"
        self.sf_database = "NEA_FORECASTING"
        self.sf_schema = "PUBLIC"
        
    def get_snowflake_connection(self):
        return snowflake.connector.connect(
            user=self.sf_user,
            password=self.sf_pass,
            account=self.sf_account,
            warehouse=self.sf_warehouse,
            database=self.sf_database,
            schema=self.sf_schema
        )
    
    def calculate_days_on_hand(self):
        """
        Calculate days on hand based on current inventory vs recent sales velocity
        """
        conn = self.get_snowflake_connection()
        
        # Get current inventory
        inventory_query = """
        SELECT 
            LOCATIONNAME,
            "Matched S&OP Category" as SNOP_CATEGORY,
            PRODUCTNAME,
            BRANDNAME,
            INVENTORYDATE,
            SUM("TOTAL_QUANTITY") as CURRENT_QUANTITY
        FROM matched_inventory_with_snop_category
        WHERE INVENTORYDATE = (SELECT MAX(INVENTORYDATE) FROM matched_inventory_with_snop_category)
        GROUP BY LOCATIONNAME, "Matched S&OP Category", PRODUCTNAME, BRANDNAME, INVENTORYDATE
        """
        
        # Get recent sales velocity 
        sales_velocity_query = """
        WITH daily_sales AS (
            SELECT 
                LOCATIONNAME,
                "Matched S&OP Category" as SNOP_CATEGORY,
                PRODUCTNAME,
                BRANDNAME,
                TRANSACTIONDATE,
                SUM(TOTAL_QUANTITY) as DAILY_QUANTITY
            FROM matched_sales_with_snop_category
            WHERE TRANSACTIONDATE >= CURRENT_DATE() - 30
            GROUP BY LOCATIONNAME, "Matched S&OP Category", PRODUCTNAME, BRANDNAME, TRANSACTIONDATE
        )
        SELECT 
            LOCATIONNAME,
            SNOP_CATEGORY,
            PRODUCTNAME, 
            BRANDNAME,
            AVG(CASE WHEN TRANSACTIONDATE >= CURRENT_DATE() - 7 THEN DAILY_QUANTITY END) as AVG_DAILY_SALES_7D,
            AVG(DAILY_QUANTITY) as AVG_DAILY_SALES_30D,
            COUNT(DISTINCT TRANSACTIONDATE) as DAYS_WITH_SALES,
            MAX(TRANSACTIONDATE) as LAST_SALE_DATE
        FROM daily_sales
        GROUP BY LOCATIONNAME, SNOP_CATEGORY, PRODUCTNAME, BRANDNAME
        """
        
        inventory_df = pd.read_sql(inventory_query, conn)
        velocity_df = pd.read_sql(sales_velocity_query, conn)
        conn.close()
        
        # Merge inventory with velocity
        doh_df = inventory_df.merge(
            velocity_df, 
            on=['LOCATIONNAME', 'SNOP_CATEGORY', 'PRODUCTNAME', 'BRANDNAME'],
            how='left'
        )
        
        # Calculate days on hand
        doh_df['AVG_DAILY_SALES_7D'] = doh_df['AVG_DAILY_SALES_7D'].fillna(0)
        doh_df['AVG_DAILY_SALES_30D'] = doh_df['AVG_DAILY_SALES_30D'].fillna(0)
        
        doh_df['DAYS_ON_HAND_7D_AVG'] = np.where(
            doh_df['AVG_DAILY_SALES_7D'] > 0,
            doh_df['CURRENT_QUANTITY'] / doh_df['AVG_DAILY_SALES_7D'],
            999  # High number for no recent sales
        )
        
        doh_df['DAYS_ON_HAND_30D_AVG'] = np.where(
            doh_df['AVG_DAILY_SALES_30D'] > 0,
            doh_df['CURRENT_QUANTITY'] / doh_df['AVG_DAILY_SALES_30D'],
            999
        )
        
        # Inventory status categorization
        def categorize_inventory_status(row):
            if row['CURRENT_QUANTITY'] <= 0:
                return 'STOCK_OUT'
            elif row['DAYS_ON_HAND_7D_AVG'] <= 3:
                return 'CRITICAL_LOW'
            elif row['DAYS_ON_HAND_7D_AVG'] <= 7:
                return 'LOW_STOCK'
            elif row['DAYS_ON_HAND_30D_AVG'] >= 60:
                return 'EXCESS_STOCK'
            elif row['DAYS_ON_HAND_30D_AVG'] >= 30:
                return 'HIGH_STOCK'
            else:
                return 'OPTIMAL'
        
        doh_df['INVENTORY_STATUS'] = doh_df.apply(categorize_inventory_status, axis=1)
        
        # Suggested reorder points (configurable by category)
        category_reorder_days = {
            'flower': 14,
            'concentrate': 21, 
            'edible': 30,
            'vape': 21
        }
        
        def get_reorder_point(row):
            # Extract product type from category for reorder logic
            category_lower = str(row['SNOP_CATEGORY']).lower()
            if 'flower' in category_lower:
                return row['AVG_DAILY_SALES_30D'] * category_reorder_days['flower']
            elif 'concentrate' in category_lower:
                return row['AVG_DAILY_SALES_30D'] * category_reorder_days['concentrate']
            elif 'edible' in category_lower or 'gummies' in category_lower:
                return row['AVG_DAILY_SALES_30D'] * category_reorder_days['edible']
            elif 'vape' in category_lower:
                return row['AVG_DAILY_SALES_30D'] * category_reorder_days['vape']
            else:
                return row['AVG_DAILY_SALES_30D'] * 21  # Default 3 weeks
        
        doh_df['REORDER_POINT'] = doh_df.apply(get_reorder_point, axis=1)
        
        # Suggested actions
        def get_suggested_action(row):
            if row['INVENTORY_STATUS'] == 'STOCK_OUT':
                return 'URGENT_REORDER'
            elif row['INVENTORY_STATUS'] == 'CRITICAL_LOW':
                return 'REORDER_NOW'
            elif row['INVENTORY_STATUS'] == 'LOW_STOCK':
                return 'SCHEDULE_REORDER'
            elif row['INVENTORY_STATUS'] == 'EXCESS_STOCK':
                return 'DISCOUNT_OR_PROMO'
            elif row['INVENTORY_STATUS'] == 'HIGH_STOCK':
                return 'MONITOR_CLOSELY'
            else:
                return 'MAINTAIN_CURRENT'
        
        doh_df['SUGGESTED_ACTION'] = doh_df.apply(get_suggested_action, axis=1)
        
        return doh_df
    
    def calculate_stock_performance_alerts(self):
        """
        Identify stock-outs and slow movers
        """
        conn = self.get_snowflake_connection()
        
        query = """
        WITH current_inventory AS (
            SELECT 
                LOCATIONNAME,
                "Matched S&OP Category" as SNOP_CATEGORY,
                PRODUCTNAME,
                BRANDNAME,
                SUM("TOTAL_QUANTITY") as CURRENT_QUANTITY
            FROM matched_inventory_with_snop_category
            WHERE INVENTORYDATE = (SELECT MAX(INVENTORYDATE) FROM matched_inventory_with_snop_category)
            GROUP BY LOCATIONNAME, "Matched S&OP Category", PRODUCTNAME, BRANDNAME
        ),
        recent_sales AS (
            SELECT 
                LOCATIONNAME,
                "Matched S&OP Category" as SNOP_CATEGORY,
                PRODUCTNAME,
                BRANDNAME,
                MAX(TRANSACTIONDATE) as LAST_SALE_DATE,
                SUM(TOTAL_QUANTITY) as TOTAL_QUANTITY_90D
            FROM matched_sales_with_snop_category
            WHERE TRANSACTIONDATE >= CURRENT_DATE() - 90
            GROUP BY LOCATIONNAME, "Matched S&OP Category", PRODUCTNAME, BRANDNAME
        )
        SELECT 
            i.LOCATIONNAME,
            i.SNOP_CATEGORY,
            i.PRODUCTNAME,
            i.BRANDNAME,
            i.CURRENT_QUANTITY,
            s.LAST_SALE_DATE,
            COALESCE(s.TOTAL_QUANTITY_90D, 0) as TOTAL_QUANTITY_90D,
            DATEDIFF(day, s.LAST_SALE_DATE, CURRENT_DATE()) as DAYS_SINCE_LAST_SALE
        FROM current_inventory i
        LEFT JOIN recent_sales s ON 
            i.LOCATIONNAME = s.LOCATIONNAME AND
            i.SNOP_CATEGORY = s.SNOP_CATEGORY AND
            i.PRODUCTNAME = s.PRODUCTNAME AND
            i.BRANDNAME = s.BRANDNAME
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Performance flags
        def get_performance_flag(row):
            if row['CURRENT_QUANTITY'] <= 0:
                return 'STOCK_OUT'
            elif pd.isna(row['LAST_SALE_DATE']):
                return 'NO_RECENT_SALES'
            elif row['DAYS_SINCE_LAST_SALE'] > 60:
                return 'SLOW_MOVER'
            elif row['DAYS_SINCE_LAST_SALE'] > 30:
                return 'DECLINING_SALES'
            elif row['TOTAL_QUANTITY_90D'] < 5:  # Less than 5 units in 90 days
                return 'LOW_VELOCITY'
            else:
                return 'NORMAL'
        
        df['PERFORMANCE_FLAG'] = df.apply(get_performance_flag, axis=1)
        
        # Alert priority
        priority_map = {
            'STOCK_OUT': 'HIGH',
            'NO_RECENT_SALES': 'MEDIUM', 
            'SLOW_MOVER': 'MEDIUM',
            'DECLINING_SALES': 'LOW',
            'LOW_VELOCITY': 'LOW',
            'NORMAL': None
        }
        
        df['ALERT_PRIORITY'] = df['PERFORMANCE_FLAG'].map(priority_map)
        
        # Recommended actions
        action_map = {
            'STOCK_OUT': 'Investigate demand and restock if needed',
            'NO_RECENT_SALES': 'Review product placement and pricing',
            'SLOW_MOVER': 'Consider promotion or discontinuation',
            'DECLINING_SALES': 'Monitor trend and adjust strategy',
            'LOW_VELOCITY': 'Evaluate minimum stock levels',
            'NORMAL': 'Continue current strategy'
        }
        
        df['RECOMMENDED_ACTION'] = df['PERFORMANCE_FLAG'].map(action_map)
        
        return df[df['ALERT_PRIORITY'].notna()]  # Only return items needing attention
    
    def calculate_inventory_velocity_trends(self):
        """
        Calculate velocity trends and turnover metrics
        """
        conn = self.get_snowflake_connection()
        
        query = """
        WITH daily_sales AS (
            SELECT 
                LOCATIONNAME,
                "Matched S&OP Category" as SNOP_CATEGORY,
                BRANDNAME,
                TRANSACTIONDATE,
                SUM(TOTAL_QUANTITY) as DAILY_QUANTITY,
                SUM(TOTAL_REVENUE) as DAILY_REVENUE
            FROM matched_sales_with_snop_category
            WHERE TRANSACTIONDATE >= CURRENT_DATE() - 90
            GROUP BY LOCATIONNAME, "Matched S&OP Category", BRANDNAME, TRANSACTIONDATE
        ),
        rolling_metrics AS (
            SELECT *,
                AVG(DAILY_QUANTITY) OVER (
                    PARTITION BY LOCATIONNAME, SNOP_CATEGORY, BRANDNAME 
                    ORDER BY TRANSACTIONDATE 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as SALES_VELOCITY_7D,
                AVG(DAILY_QUANTITY) OVER (
                    PARTITION BY LOCATIONNAME, SNOP_CATEGORY, BRANDNAME 
                    ORDER BY TRANSACTIONDATE 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) as SALES_VELOCITY_30D
            FROM daily_sales
        )
        SELECT *,
            CASE 
                WHEN SALES_VELOCITY_7D > SALES_VELOCITY_30D * 1.2 THEN 'ACCELERATING'
                WHEN SALES_VELOCITY_7D < SALES_VELOCITY_30D * 0.8 THEN 'DECLINING'
                ELSE 'STABLE'
            END as VELOCITY_TREND
        FROM rolling_metrics
        WHERE TRANSACTIONDATE >= CURRENT_DATE() - 30  -- Last 30 days only
        """
        
        df = pd.read_sql(query, conn)
        
        # Calculate category rankings by velocity
        df['CATEGORY_RANK'] = df.groupby(['LOCATIONNAME', 'TRANSACTIONDATE'])['SALES_VELOCITY_7D'].rank(
            method='dense', ascending=False
        )
        
        conn.close()
        return df
    
    def upload_to_snowflake(self, df, table_name):
        """Upload dataframe to Snowflake table"""
        conn = self.get_snowflake_connection()
        try:
            # Clear existing data for current date range if applicable
            if 'INVENTORYDATE' in df.columns:
                dates = df['INVENTORYDATE'].unique()
                if len(dates) > 0:
                    date_list = "','".join([str(d) for d in dates])
                    conn.cursor().execute(f"DELETE FROM {table_name} WHERE INVENTORYDATE IN ('{date_list}');")
            
            success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
            print(f"✅ Uploaded {nrows} rows to {table_name}")
            
        finally:
            conn.close()
    
    def run_all_kpi_calculations(self):
        """Execute all KPI calculations and upload to Snowflake"""
        print("🚀 Starting Tableau KPI calculations...")
        
        # Days on Hand Analysis
        print("📊 Calculating Days on Hand...")
        doh_df = self.calculate_days_on_hand()
        self.upload_to_snowflake(doh_df, "days_on_hand_analysis")
        
        # Stock Performance Alerts  
        print("🚨 Calculating Stock Performance Alerts...")
        alerts_df = self.calculate_stock_performance_alerts()
        self.upload_to_snowflake(alerts_df, "stock_performance_alerts")
        
        # Inventory Velocity Trends
        print("📈 Calculating Inventory Velocity Trends...")
        velocity_df = self.calculate_inventory_velocity_trends()
        self.upload_to_snowflake(velocity_df, "inventory_velocity_trends")
        
        print("✅ All Tableau KPI calculations complete!")
        
        return {
            'days_on_hand': len(doh_df),
            'alerts': len(alerts_df), 
            'velocity_trends': len(velocity_df)
        }

if __name__ == "__main__":
    calculator = TableauKPICalculator()
    results = calculator.run_all_kpi_calculations()
    print(f"📊 Results: {results}")
