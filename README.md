# Fund Analytics Pipelines

Two path-agnostic data pipelines for fund analytics:

1. **WAR REPORT** (daily XLSX → consolidated analytics)
2. **CLIENT FUNDS LIFE CYCLE** (monthly CSV → multi-sheet Excel + CSVs)

The script avoids hard-coded paths; all inputs/outputs are set via CLI flags or environment variables.

---

## Features

### WAR REPORT
- Reads multiple daily Excel files and builds a consolidated dataset
- Outliers by fund/day, per-fund stats and rankings
- Full correlation matrix, pairwise correlations, “top” correlation table
- Optional per-fund evolution sheets and ARIMA forecasts

### LIFE CYCLE
- Tags client movements (buy, sell, increase, reduce, keep) with a November “first snapshot” rule
- KPIs: funds per client, clients per fund (overall & monthly)
- Combinations of funds, churn (global & per fund per month), acquisition, retention
- Client profiles (recency, frequency, quantity, inactivity, current status)
- Flows/transitions between funds month-to-month
- Exports one Excel with many sheets **and** a CSV for each sheet

---

## Requirements

- Python 3.10+
- Install dependencies:
  ```bash
  pip install -r requirements.txt
