from wholesale_inventory_cleaning import run_wholesale_inventory_cleaning
import pandas as pd
import os
from datetime import date

if __name__ == "__main__":
    df = run_wholesale_inventory_cleaning()

    # --- Split into matched / unmatched ---
    matched_df = df[df['MATCHED_SNOP_CATEGORY'].notna()].copy()
    unmatched_df = df[df['MATCHED_SNOP_CATEGORY'].isna()].copy()

    # --- Export to CSV ---
    today_str = date.today().strftime("%Y-%m-%d")
    matched_df.to_csv(f"matched_wholesale_inventory_{today_str}.csv", index=False)
    unmatched_df.to_csv(f"unmatched_wholesale_inventory_{today_str}.csv", index=False)

    # --- Output Summary ---
    print(df.head())
    print(f"\n✅ Test complete. Total: {len(df)} rows")
    print(f"🟢 Matched: {len(matched_df)} rows")
    print(f"🔴 Unmatched: {len(unmatched_df)} rows")
    print(f"📁 Files saved: matched_wholesale_inventory_{today_str}.csv, unmatched_wholesale_inventory_{today_str}.csv")
