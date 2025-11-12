from retail_inventory_cleaning import run_retail_inventory_cleaning
import pandas as pd
import os
from datetime import date

if __name__ == "__main__":
    matched_df, unmatched_df = run_retail_inventory_cleaning()

    # --- Export to CSV ---
    today_str = date.today().strftime("%Y-%m-%d")
    matched_df.to_csv(f"matched_retail_inventory_{today_str}.csv", index=False)
    unmatched_df.to_csv(f"unmatched_retail_inventory_{today_str}.csv", index=False)

    # --- Output Summary ---
    total_rows = len(matched_df) + len(unmatched_df)
    print(matched_df.head())
    print(f"\n✅ Test complete. Total: {total_rows} rows")
    print(f"🟢 Matched: {len(matched_df)} rows")
    print(f"🔴 Unmatched: {len(unmatched_df)} rows")
    print(f"📁 Files saved: matched_retail_inventory_{today_str}.csv, unmatched_retail_inventory_{today_str}.csv")
