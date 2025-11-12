# 🧾 NEA Sales & Operations Planning (S&OP) Report Automation

This project automates the retrieval, cleaning, matching, and loading of both **retail** and **wholesale** cannabis sales data for Northeast Alternatives (NEA). It powers demand forecasting, executive reporting, and category performance analytics via Snowflake and Power BI.

---

## 📦 Project Overview

The workflow:
- Connects securely to a **Snowflake** warehouse.
- Pulls data from:
  - `NEA_SALES.PUBLIC.VSALES` (Retail sales)
  - `NEA_SALES.WHOLESALE.VWHOLESALESALES` (Wholesale sales)
- Normalizes product names and **matches to standardized S&OP Categories** from a curated product catalog.
- Aggregates matched data into:
  - Daily summaries
  - Category-level summaries
- Loads final output into **Snowflake tables** for Power BI consumption.

---

## 🛠 Key Features

- ✅ **Integrated Retail & Wholesale Pipelines**
- ✅ **Dynamic Matching Logic** (fuzzy for retail, strict for wholesale)
- ✅ **Unit & Case Conversion** for wholesale products
- ✅ **Custom Product Category Rules** (e.g. bulk, strain, brand locks)
- ✅ **De-duplication Logic** via `TRANSACTIONDATE` filtering
- ✅ **Snowflake Upload with Smart Overwrite**
- ✅ **Match Archive System** to back up each run locally
- ✅ **GitHub Actions Integration** for full automation

---

## 📁 Snowflake Output Tables

The following tables are updated dynamically:

| Table Name                              | Description                                 |
|----------------------------------------|---------------------------------------------|
| `matched_sales_with_snop_category`     | Final cleaned + categorized transactions    |
| `unmatched_sales_without_snop_category`| Products that failed to match               |
| `matched_category_summary`             | Aggregated revenue/quantity by category     |
| `daily_category_summary`               | Daily totals per location and category      |

Rows are deleted by `TRANSACTIONDATE` for each run to prevent duplication.

---

## 🧪 Local Testing Instructions

```bash
# Install required packages
pip install -r requirements.txt

# Run the pipeline
python merge_outputs.py
```

Outputs are saved locally to your `Merged_Output/` and archived in timestamped folders.

---

## 🔐 GitHub Secrets Configuration

This repo uses **GitHub Secrets** to handle credentials:

| Secret Name     | Description                   |
|-----------------|-------------------------------|
| `MY_SF_USER`    | Snowflake user                |
| `MY_SF_PASS`    | Snowflake password            |
| `MY_SF_ACCT`    | Snowflake account identifier  |

These are accessed in the code via `os.getenv("SECRET_NAME")`.

---

## 🧠 Notes & Considerations

- This project supports 18-month historical runs but defaults to the **last 35 days** during scheduled automation.
- If any column mismatch occurs in Snowflake, check your reference catalog structure and data types.
- Duplicate prevention is handled by deleting rows within the `TRANSACTIONDATE` range of the current dataset before insert.

---

## 🧩 Upcoming Enhancements

- 📊 Forecasting integration (via Prophet)
- 📬 Slack or email alerts on match performance
- 📈 Cumulative trend views in Power BI

---

For questions or enhancements, reach out to the project maintainer.
