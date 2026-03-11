# Ad Inventory Demand Forecasting System

**[Live Demo](https://ad-inventory-dashboard-b3wcx2kc4q-uc.a.run.app)** | [GitHub](https://github.com/vxa8502/ad-inventory-forecast)

I built an ad inventory forecasting system in BigQuery that benchmarks TimesFM 2.5 against ARIMA+ across 34 simulated ad units — directly mirroring the 24-month forecasting horizon Google Ad Manager uses to predict available inventory for publishers managing $237B+ in annual ad revenue.

## Problem

Publishers using ad networks face two costly failure modes:

- **Overbooking**: Selling more impressions than available leads to contractual penalties
- **Underbooking**: Unfilled inventory gets backfilled at lower CPMs

This project benchmarks BigQuery ML's three forecasting approaches using Wikipedia pageviews as an ad inventory proxy.

## Results

Walk-forward validation across two 92-day test windows:

| Model | Avg MAPE | Avg MASE | Key Insight |
|-------|----------|----------|-------------|
| **TimesFM 2.5** | **17.9%** | **0.82** | Robust to distribution shift |
| ARIMA+ | 35.3% | 1.14 | Interpretable, decomposable |
| ARIMA+ XREG | 73.8% | 2.31 | Failed on event-driven content |

**Key finding**: ARIMA+ XREG achieved 1113% MAPE on Barbie because learned intercepts cannot adapt to post-hype traffic decay. Production systems should route based on content volatility, not use a single model everywhere.

## Architecture

```
Wikipedia Pageviews (BQ Public)
        |
        v
+--------------------------------------+
|       BigQuery ML Forecasting        |
|  AI.FORECAST   -> TimesFM 2.5        |
|  CREATE MODEL  -> ARIMA+             |
|  CREATE MODEL  -> ARIMA+ XREG        |
+--------------------------------------+
        |
        v
+--------------------------------------+
|        Streamlit Dashboard           |
|  Forecast viz | Model comparison     |
|  Decomposition | Anomaly detection   |
+--------------------------------------+
        |
        v
     Cloud Run
```

## Quick Start

```bash
git clone https://github.com/vxa8502/ad-inventory-forecast.git
cd ad-inventory-forecast
python -m venv .venv && source .venv/bin/activate
cp .env.example .env  # Add GCP credentials
pip install -r requirements.txt
streamlit run app/main.py
```

## Project Structure

```
ad-inventory-forecast/
├── app/                    # Streamlit dashboard (5 pages)
├── config/                 # Settings
├── sql/                    # All BigQuery SQL (extract, transform, model, evaluate)
├── src/                    # Python utilities
├── scripts/                # Pipeline orchestration
├── data/reference/         # Holiday lookup CSV
├── pipelines/              # Vertex AI retraining pipeline
└── tests/
```

## Deployment

Deployed on Cloud Run with CI/CD via Cloud Build. See `Dockerfile`, `cloudbuild.yaml`.

## Data

- **Source**: `bigquery-public-data.wikipedia.pageviews_*` (2023-2024)
- **Articles**: 34 across 5 verticals (Tech, Sports, Finance, Health, Entertainment)
- **Extraction cost**: $6.23

See [Data Card](data/reference/data_card.md) for methodology and limitations.

## Cost

~$1-3 total (BigQuery free tier + TimesFM preview pricing). Cloud Run: $0/month at demo traffic.

## License

MIT
