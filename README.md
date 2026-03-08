# Ad Inventory Demand Forecasting System

A production-style ad inventory forecasting system using BigQuery ML that benchmarks three forecasting approaches: TimesFM 2.5 (zero-shot), ARIMA_PLUS (statistical), and ARIMA_PLUS_XREG (feature-enriched). Built to mirror the forecasting challenges Google Ad Manager solves for publishers managing $237B+ in annual ad revenue.

## Business Problem

Publishers using ad networks face two costly failure modes from inaccurate inventory forecasts:

- **Overbooking**: Selling more guaranteed impressions than available leads to contractual penalties
- **Underbooking**: Unfilled inventory gets backfilled at dramatically lower programmatic CPMs

This project builds a forecasting system using Wikipedia pageviews as an ad inventory proxy, exhibiting the same statistical properties as real ad traffic: day-of-week cycles, holiday spikes, desktop/mobile splits, and long-term trends.

## Architecture

```
Wikipedia Pageviews (BQ Public Dataset)
        +                                 Feature Engineering       BigQuery Tables
US Holidays (reference CSV)          ->   (SQL transforms)      ->  (partitioned, clustered)
        |
        v
+--------------------------------------+
|       BigQuery ML Forecasting        |
|                                      |
|  AI.FORECAST   -> TimesFM 2.5        |
|  CREATE MODEL  -> ARIMA_PLUS         |
|  CREATE MODEL  -> ARIMA_PLUS_XREG    |
+--------------------------------------+
        |
        v
+--------------------------------------+
|      Evaluation & Comparison         |
|  Metrics: MAPE, RMSE, MAE, MASE      |
+--------------------------------------+
        |
        v
+--------------------------------------+
|        Streamlit Dashboard           |
|  - 90-day forecast visualization     |
|  - Model comparison                  |
|  - Forecast decomposition            |
|  - Anomaly detection                 |
+--------------------------------------+
```

## Quick Start

### Prerequisites

- Python 3.11+
- Google Cloud project with BigQuery enabled
- Service account credentials with BigQuery permissions

### Setup

```bash
# Clone and navigate
git clone https://github.com/vxa8502/ad-inventory-forecast.git
cd ad-inventory-forecast

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Configure credentials
cp .env.example .env
# Edit .env with your GCP_PROJECT_ID and credentials path
```

### Run Data Pipeline

```bash
# Estimate costs first (recommended)
python scripts/run_pipeline.py --dry-run

# Execute pipeline
python scripts/run_pipeline.py
```

## Project Structure

```
ad-inventory-forecast/
├── config/
│   └── settings.py          # Centralized configuration
├── sql/
│   ├── 01_schema/            # Dataset and table DDL
│   ├── 02_extract/           # Data extraction queries
│   ├── 03_transform/         # Feature engineering
│   └── 04_validate/          # Quality checks
├── src/
│   ├── bq_client.py          # BigQuery client wrapper
│   ├── sql_runner.py         # SQL file loading/execution
│   └── validators.py         # Data validation functions
├── scripts/
│   └── run_pipeline.py       # Pipeline orchestration
├── data/reference/
│   └── us_holidays.csv       # Holiday lookup data
└── tests/
    ├── test_sql_runner.py
    └── test_validators.py
```

## Cost Analysis

| Resource | Free Tier | Estimated Usage | Cost |
|----------|-----------|-----------------|------|
| BigQuery queries | 1 TiB/month | ~50-200 GB total | $0 |
| BigQuery ML | 10 GB/month | ~2-5 GB | $0 |
| AI.FORECAST (TimesFM) | Preview pricing | ~$0.50-2.00 total | ~$1 |
| **Total** | | | **~$1-3** |

**Actual extraction cost**: $6.23 for full 2023-2024 Wikipedia extraction (table above shows free tier estimates for ongoing usage).

**Note**: BigQuery Sandbox (no billing) does NOT support BigQuery ML. Enable billing with the $300 free trial and set a daily cost cap.

## Data Sources

- **Wikipedia Pageviews**: `bigquery-public-data.wikipedia.pageviews_*` (2023-2024, 35 articles)
- **US Holidays**: Custom reference table (62 holidays, 2022-2024)

See [Data Card](data/reference/data_card.md) for full dataset documentation.

## Models

| Model | Type | Key Features |
|-------|------|--------------|
| TimesFM 2.5 | Foundation model | Zero-shot, no training required |
| ARIMA_PLUS | Statistical | Auto seasonality, holiday effects, decomposition |
| ARIMA_PLUS_XREG | Statistical + regressors | Calendar features (day-of-week, holidays) |

## Project Status

| Component | Status | Description |
|-----------|--------|-------------|
| Data Foundation | Complete | 35 articles, 25,544 daily observations, feature engineering |
| Model Development | In Progress | TimesFM, ARIMA_PLUS, ARIMA_PLUS_XREG training |
| Dashboard | Pending | Streamlit visualization and deployment |

## Results

*Model comparison results will be added after model training completion.*

## Documentation

- [Data Card](data/reference/data_card.md) - Dataset documentation, validation results, and known limitations

## License

MIT
