# Ad Inventory Demand Forecasting System

**[Live Demo](https://ad-inventory-dashboard-b3wcx2kc4q-uc.a.run.app)** |
[GitHub](https://github.com/vxa8502/ad-inventory-forecast)

![Deployed on Cloud Run](https://img.shields.io/badge/Deployed-Cloud%20Run-blue)
![BigQuery ML](https://img.shields.io/badge/BigQuery-ML-orange)
![Cost](https://img.shields.io/badge/Cost-~%240%2Fmonth-green)

A production-style ad inventory forecasting system using BigQuery ML that benchmarks three forecasting approaches: TimesFM 2.5 (zero-shot), ARIMA+ (statistical), and ARIMA+ XREG (feature-enriched). Built to mirror the forecasting challenges Google Ad Manager solves for publishers managing $237B+ in annual ad revenue.

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
|  CREATE MODEL  -> ARIMA+             |
|  CREATE MODEL  -> ARIMA+ XREG        |
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
|  - 92-day forecast visualization     |
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

## Results

Walk-forward validation with two folds:

| Fold | Training Period | Test Period | Days |
|------|-----------------|-------------|------|
| fold_1 | 2023-01-01 to 2024-06-30 | 2024-07-01 to 2024-09-30 | 92 |
| fold_2 | 2023-01-01 to 2024-09-30 | 2024-10-01 to 2024-12-31 | 92 |

| Model | Fold 1 MAPE | Fold 2 MAPE | Avg MAPE | Key Strength |
|-------|-------------|-------------|----------|--------------|
| **TimesFM 2.5** | **18.7%** | **17.1%** | **17.9%** | Robust to distribution shift |
| ARIMA+ | 29.8% | 40.7% | 35.3% | Interpretable, handles seasonality |
| ARIMA+ XREG | 94.5% | 53.0% | 73.8% | Competitive on stable articles |

**Key Finding**: TimesFM 2.5 (zero-shot foundation model) outperformed trained statistical models due to robustness against distribution shift. Event-driven articles (movies, sports) had 40-85% traffic declines between training and test periods, causing ARIMA+ XREG's learned intercepts to over-predict catastrophically.

## Model Analysis

### The Failure Was More Instructive Than The Win

TimesFM 2.5 achieved 17.9% MAPE, outperforming ARIMA+ (35.3%) and ARIMA+ XREG (73.8%). But the real finding was *why* XREG failed so dramatically on event-driven content:

| Article | ARIMA+ | XREG | TimesFM |
|---------|--------|------|---------|
| Barbie_(film) | 32.0% | **1113.8%** | 24.4% |
| Oppenheimer_(film) | 57.5% | **979.7%** | 26.4% |
| Super_Bowl | 41.6% | **211.8%** | 25.1% |

**Root Cause**: XREG learns intercepts from training data. When Barbie traffic dropped 85% after its theatrical run ended, the model kept predicting peak-hype levels. Calendar regressors (weekend, holiday) cannot capture "months since movie release."

Conversely, XREG *outperformed* on stable evergreen content where learned intercepts remained valid:

| Article | Traffic Change | ARIMA+ MAPE | XREG MAPE | Winner |
|---------|---------------|-------------|-----------|--------|
| Diabetes | -2.8% | 7.4% | 7.1% | XREG |
| S&P_500 | -2.9% | 14.9% | 13.7% | XREG |
| Bitcoin | -22.5% | 14.7% | 12.4% | XREG |

### Pre-Training Hypotheses (Validated)

Before training, we documented expected patterns by vertical:

| Vertical | Expected Pattern | Key Test | Result |
|----------|-----------------|----------|--------|
| Technology | Strong weekday (developers) | Python weekend dips > 20% | -28% to -33% |
| Sports | Game-day spikes | NFL Sunday/Monday spikes | 25-32% amplitude |
| Finance | Weekday (market hours) | Bitcoin weekend -10% to -12% | -12% |
| Health | Flu season Oct-Mar | Influenza yearly amplitude > 30% | Confirmed |
| Entertainment | Event-driven | Barbie/Oppenheimer step changes | 20/34 articles |

5/5 spot-check articles passed hypothesis validation, confirming ARIMA+ learned sensible structure.

### ARIMA Order Distribution

| Statistic | Value |
|-----------|-------|
| Articles with d=1 (non-stationary) | 31/34 (91%) |
| Articles with d=0 (stationary) | 3/34 (9%) |
| Articles with holiday_effect | 34/34 (100%) |
| Articles with step_changes | 20/34 (59%) |

**Insight**: Wikipedia traffic is fundamentally non-stationary. Only Federal_Reserve, UFC, and Beyonce were stationary enough for d=0.

### TimesFM Confidence Interval Calibration

11 of 68 article-folds (16%) have confidence intervals exceeding 100% of forecast:

| Article | Fold | CI % |
|---------|------|------|
| Oppenheimer_(film) | fold_1 | 182.3% |
| ChatGPT | fold_1 | 180.2% |
| Beyonce | fold_2 | 153.8% |

Wide intervals correlate with event-driven content. TimesFM correctly expresses uncertainty for volatile series rather than producing overconfident narrow intervals. **The model knows when it doesn't know.**

### Anomaly Detection

Using `ML.DETECT_ANOMALIES` on ARIMA+ with 0.95 probability threshold:

| Event | Date | Articles | Spike | Probability |
|-------|------|----------|-------|-------------|
| Super Bowl LVIII | 2024-02-11 | Super_Bowl, NFL | 8-12x | >0.99 |
| Barbenheimer | 2023-07-21 | Barbie, Oppenheimer | 50-100x | >0.99 |
| iPhone 15 Launch | 2023-09-12 | IPhone, Apple_Inc. | 3-5x | 0.96-0.98 |
| Eras Tour | 2023 (multiple) | Taylor_Swift | 2-4x | 0.95-0.98 |

## Production Routing Strategy

| Content Type | Recommended Model | Rationale |
|--------------|-------------------|-----------|
| **Stable** (CV < 0.5) | ARIMA+ XREG | Calendar features add value; lowest MAPE on evergreen content |
| **Event-Driven** (CV >= 0.5) | TimesFM 2.5 | Foundation model handles distribution shift; XREG fails catastrophically |
| **Cold-Start** (< 90 days) | TimesFM 2.5 | Zero-shot inference requires no training data |
| **Interpretability Required** | ARIMA+ | ML.EXPLAIN_FORECAST provides trend/seasonal/holiday decomposition |

**Key Insight**: No single model wins everywhere. Production systems should route based on content volatility.

## Lessons Learned

1. **d=1 is the norm**: Wikipedia traffic is fundamentally non-stationary. Don't expect d=0 for trending topics.

2. **Event-driven content breaks XREG**: Barbie, Oppenheimer, ChatGPT show that post-hype decay makes learned intercepts catastrophic.

3. **Weekend effects vary by audience**: Developers (Python) show -30% weekend dip; entertainment shows weekend increases; crypto is nearly flat (-12%).

4. **Foundation models handle distribution shift**: TimesFM's zero-shot approach avoids overfitting to historical levels.

5. **Holiday detection works universally**: All 34 articles detected holiday effects with `holiday_region='US'`.

6. **Anomalies inform feature engineering**: Detected patterns suggest adding `product_launch` and `game_day` regressors.

## Models

| Model | Type | Key Features | BigQuery Edition |
|-------|------|--------------|------------------|
| TimesFM 2.5 | Foundation model | Zero-shot, no training required | Enterprise |
| ARIMA+ | Statistical | Auto seasonality, holiday effects, decomposition | Standard |
| ARIMA+ XREG | Statistical + regressors | Calendar features (day-of-week, holidays) | Standard |

All model artifacts stored in `{project}.ad_inventory.*` (forecasts, metrics, coefficients, decomposition).

## Data Sources

- **Wikipedia Pageviews**: `bigquery-public-data.wikipedia.pageviews_*` (2023-2024, 34 articles extracted, 19 used in dashboard)
- **US Holidays**: Custom reference table (83 holidays, 2022-2025; extended through 2025 for forecast horizon)

See [Data Card](data/reference/data_card.md) for full dataset documentation including article selection rationale and known limitations.

## Cost Analysis

| Resource | Free Tier | Estimated Usage | Cost |
|----------|-----------|-----------------|------|
| BigQuery queries | 1 TiB/month | ~50-200 GB total | $0 |
| BigQuery ML | 10 GB/month | ~2-5 GB | $0 |
| AI.FORECAST (TimesFM) | Preview pricing | ~$0.50-2.00 total | ~$1 |
| **Total** | | | **~$1-3** |

**Actual extraction cost**: $6.23 for full 2023-2024 Wikipedia extraction (table above shows free tier estimates for ongoing usage).

**Note**: BigQuery Sandbox (no billing) does NOT support BigQuery ML. Enable billing with the $300 free trial and set a daily cost cap.

## Project Structure

```
ad-inventory-forecast/
├── app/                          # Streamlit dashboard
│   ├── pages/                    # 5 dashboard pages
│   ├── components/               # Charts, tables, filters
│   └── data/queries.py           # BigQuery data fetching
├── config/
│   └── settings.py               # Centralized configuration
├── sql/
│   ├── 01_schema/                # Dataset and table DDL
│   ├── 02_extract/               # Data extraction queries
│   ├── 03_transform/             # Feature engineering
│   ├── 04_validate/              # Quality checks
│   ├── 05_model/                 # Model training SQL
│   └── 06_evaluate/              # Forecast generation and metrics
├── src/
│   ├── bq_client.py              # BigQuery client wrapper
│   ├── sql_runner.py             # SQL file loading/execution
│   └── validators.py             # Data validation functions
├── scripts/
│   ├── run_pipeline.py           # Data pipeline orchestration
│   ├── run_model_pipeline.py     # Model training orchestration
│   ├── analyze_ci_widths.py      # TimesFM CI analysis
│   └── spot_check_decomposition.py  # ARIMA validation
├── data/reference/
│   └── us_holidays.csv           # Holiday lookup data (83 entries)
└── tests/
    ├── test_sql_runner.py
    └── test_validators.py
```

## Model Inspection Tools

### ARIMA Order Analysis

```bash
python -m scripts.run_model_pipeline --model arima_plus --fold fold_1

bq query --use_legacy_sql=false "
SELECT ad_unit, arima_order, AIC, has_holiday_effect
FROM \`PROJECT.ad_inventory.arima_evaluate_fold_1\`
ORDER BY AIC
"
```

### Confidence Interval Analysis

```bash
python -m scripts.analyze_ci_widths
```

Flags articles with:
- **WIDE** (CI > 100%): High uncertainty, typically event-driven content
- **NARROW** (CI < 20%): Potential overconfidence, verify coverage metric

## Project Status

| Component | Status | Description |
|-----------|--------|-------------|
| Data Foundation | Complete | 34 articles, 24,854 daily observations, feature engineering |
| Model Development | Complete | Walk-forward validation with 2 folds, 3 models |
| Dashboard | Complete | Streamlit visualization deployed on Cloud Run |

## Deployment

### Cloud Run (Production)

The dashboard is deployed on Google Cloud Run with automatic CI/CD via Cloud Build.

**Architecture:**
- Streamlit app containerized with Docker
- BigQuery ML models queried on-demand
- Automatic deploys on push to main branch
- ~$0/month within Cloud Run free tier (low traffic)

**Manual Deployment:**

```bash
# Create Artifact Registry repository (one-time)
gcloud artifacts repositories create ad-inventory \
  --repository-format=docker \
  --location=us-central1

# Build and push image
gcloud auth configure-docker us-central1-docker.pkg.dev
docker build -t us-central1-docker.pkg.dev/PROJECT_ID/ad-inventory/dashboard:latest .
docker push us-central1-docker.pkg.dev/PROJECT_ID/ad-inventory/dashboard:latest

# Deploy to Cloud Run
gcloud run deploy ad-inventory-dashboard \
  --image=us-central1-docker.pkg.dev/PROJECT_ID/ad-inventory/dashboard:latest \
  --platform=managed \
  --region=us-central1 \
  --allow-unauthenticated \
  --memory=1Gi
```

**CI/CD Setup:**

```bash
# Connect GitHub trigger (deploys on push to main)
gcloud builds triggers create github \
  --repo-name=ad-inventory-forecast \
  --repo-owner=vxa8502 \
  --branch-pattern='^main$' \
  --build-config=cloudbuild.yaml
```

### Local Development

```bash
# Install app dependencies
pip install -r requirements-app.txt

# Run Streamlit locally
streamlit run app/main.py
```

## License

MIT
