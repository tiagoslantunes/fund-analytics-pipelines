
# Fund Analytics Pipelines

A production-ready Python toolkit containing **two modular, path-agnostic data pipelines** for fund analytics:

1. **WAR REPORT** – Daily XLSX → consolidated analytics  
2. **CLIENT FUNDS LIFE CYCLE** – Monthly CSV → multi-sheet Excel + CSV exports

All inputs/outputs are configured via **CLI flags** or **environment variables** — no hard-coded paths.

---

## 📌 Overview

### 1) WAR REPORT Pipeline
Purpose: Aggregate and analyse daily “WAR” Excel reports.

**Process flow:**
```mermaid
flowchart LR
    A[Daily XLSX Files] --> B[Read & Clean Data]
    B --> C[Date Filter & Normalise Columns]
    C --> D[Consolidated Dataset]
    D --> E[Outliers by Fund & Day]
    D --> F[Stats & Rankings]
    D --> G[Correlation Analysis]
    D -->|Optional| H[Per-Fund Evolution]
    D -->|Optional| I[ARIMA Forecasts]
    E & F & G & H & I --> J[Excel Report]
````

**Key features:**

* Reads multiple Excel files from an input folder (configurable `glob` pattern)
* Consolidates, cleans, and normalises data
* Detects **outliers**:

  * By fund
  * By day
* Computes **per-fund statistics & rankings**
* Builds **correlation analysis**:

  * Full correlation matrix
  * Pairwise correlations
  * Best positive/negative partner per fund
  * “Top N” correlation table
* **Optional**:

  * Per-fund **evolution sheets**
  * **ARIMA** time-series forecasts

---

### 2) CLIENT FUNDS LIFE CYCLE Pipeline

Purpose: Analyse the behaviour of clients across funds over time.

**Process flow:**

```mermaid
flowchart LR
    A[Monthly CSV Snapshot] --> B[Load & Clean Data]
    B --> C["Tag Movements (Buy/Sell/etc.)"]
    C --> D[KPIs & Metrics]
    D --> E[Funds per Client / Clients per Fund]
    D --> F[Fund Combinations]
    D --> G[Churn Analysis]
    D --> H[Acquisition & Retention]
    D --> I[Client Profiles]
    D --> J[Flows/Transitions]
    E & F & G & H & I & J --> K[Excel with Multiple Sheets]
    E & F & G & H & I & J --> L[CSV per Sheet]
```

**Key features:**

* Tags **movements** (`buy`, `sell`, `increase`, `reduce`, `keep`) with a **November “first snapshot” rule**
* Computes **KPIs**:

  * Funds per client
  * Clients per fund (overall & per month)
* Generates **combinations** of funds per client
* Calculates **churn**:

  * Global churn by month
  * Churn per fund per month
* Tracks **acquisition & retention**:

  * New clients (global & per fund)
  * Retention rate per fund per month
* Builds **client profiles** (recency, frequency, quantity, inactivity, churn months)
* Maps **flows/transitions** between funds month-to-month
* **Exports**:

  * One Excel workbook (many sheets)
  * Individual CSV per sheet (Power BI-ready)

---

## 📦 Installation

### Requirements

* **Python**: 3.10 or newer
* **Dependencies**:

  ```bash
  pip install -r requirements.txt
  ```

  Example `requirements.txt`:

  ```txt
  pandas>=2.0
  numpy>=1.25
  openpyxl>=3.1
  statsmodels>=0.14   # only needed for ARIMA forecasts
  ```

---

## 🚀 Usage

The script is called `pipelines.py` and has **two subcommands**: `war` and `life`.

### 1. WAR REPORT

```bash
python pipelines.py war \
  --input ./data/war_input \
  --output ./out/war \
  --start 2024-01-02 \
  --end 2024-06-30 \
  --glob "*.xlsx" \
  --usecols "A:I" \
  --skiprows 10 \
  --evolution-funds "Fund A" "Fund B" \
  --arima-funds "Fund A" \
  --arima-horizon 15
```

**Important flags:**

* `--input` / `--output`: input/output directories
* `--start` / `--end`: analysis period (YYYY-MM-DD)
* `--glob`: filename pattern
* `--usecols`: Excel read range
* `--skiprows`: rows to skip when reading
* `--evolution-funds`: funds to include in “Evolution\_” sheets
* `--arima-funds`: funds to forecast
* `--arima-horizon`: business days to forecast

---

### 2. LIFE CYCLE

```bash
python pipelines.py life \
  --csv ./data/life/data.csv \
  --output ./out/life \
  --excel-name Funds_Life.xlsx \
  --first-month-known 2023-11-30
```

**Important flags:**

* `--csv` / `--output`: input CSV path and output folder
* `--excel-name`: name of the output Excel
* `--first-month-known`: first month snapshot for November rule

---

## ⚙️ Environment Variables

Instead of CLI flags, you can set environment variables:

| Variable          | Purpose                     | Example                   |
| ----------------- | --------------------------- | ------------------------- |
| `WAR_INPUT_DIR`   | WAR input directory         | `./data/war_input`        |
| `WAR_OUTPUT_DIR`  | WAR output directory        | `./out/war`               |
| `LIFE_INPUT_CSV`  | LIFE CYCLE CSV path         | `./data/life/data.csv`    |
| `LIFE_OUTPUT_DIR` | LIFE CYCLE output directory | `./out/life`              |
| `LOG_LEVEL`       | Logging level               | `DEBUG`, `INFO` (default) |

Example:

```bash
export WAR_INPUT_DIR=./data/war_input
export WAR_OUTPUT_DIR=./out/war
export LOG_LEVEL=DEBUG
python pipelines.py war
```

---

## 📂 Outputs

**WAR REPORT**

* `Dados_Concatenados` sheet
* Outlier sheets: `Outliers_Fundo`, `Outliers_Dia`
* Stats & rankings: `Estatisticas`, `Rankings`
* Correlations: `Matriz_Correlacao_Completa`, `Correlacoes_Completas`, `Resumo_por_Fundo`, `Top_Correlacoes`
* Optional: `Evolution_*`, `Forecast_*`

**LIFE CYCLE**

* Multi-sheet Excel (`Funds_Life.xlsx` by default)
* One CSV per sheet in `csv/` subfolder

---

## 🛠 Troubleshooting

* **Empty output**: Check date filters (`--start`, `--end`) match your data.
* **Missing columns**: Ensure input file headers match expected formats.
* **ARIMA not running**: Install `statsmodels` (`pip install statsmodels`).
* **Excel locked**: Close the file before running the script.

---
