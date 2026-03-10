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

### Replicating This Setup

#### 1. Enable Required APIs

```bash
gcloud services enable \
  bigquery.googleapis.com \
  bigqueryconnection.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com
```

#### 2. Create Service Account with IAM Permissions

```bash
# Create dedicated service account
gcloud iam service-accounts create dashboard-sa \
  --display-name="Ad Inventory Dashboard"

# Grant BigQuery permissions (minimum required)
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:dashboard-sa@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:dashboard-sa@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

| Role | Purpose |
|------|---------|
| `roles/bigquery.user` | Run queries, create jobs |
| `roles/bigquery.dataViewer` | Read tables in `ad_inventory` dataset |

For CI/CD, Cloud Build also needs:
- `roles/run.admin` (deploy to Cloud Run)
- `roles/iam.serviceAccountUser` (act as dashboard-sa)

#### 3. Set Up Cost Guardrails

BigQuery Sandbox (no billing) does NOT support BigQuery ML. Enable billing safely:

```bash
# Activate $300 free trial (GCP Console > Billing)
# Then set budget alert:

gcloud billing budgets create \
  --billing-account=BILLING_ACCOUNT_ID \
  --display-name="Ad Inventory Daily Cap" \
  --budget-amount=5USD \
  --threshold-rule=percent=50 \
  --threshold-rule=percent=90 \
  --threshold-rule=percent=100
```

**Recommended settings:**
- Daily budget: $5 (covers worst-case full-table scans)
- Alert thresholds: 50%, 90%, 100%
- Email notifications to project owner

#### 4. Create Artifact Registry Repository

```bash
gcloud artifacts repositories create ad-inventory \
  --repository-format=docker \
  --location=us-central1
```

#### 5. Build and Deploy

```bash
# Authenticate Docker
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build image (use --platform for M1/M2 Macs)
docker build --platform linux/amd64 \
  -t us-central1-docker.pkg.dev/PROJECT_ID/ad-inventory/dashboard:latest .

# Push to Artifact Registry
docker push us-central1-docker.pkg.dev/PROJECT_ID/ad-inventory/dashboard:latest

# Deploy to Cloud Run
gcloud run deploy ad-inventory-dashboard \
  --image=us-central1-docker.pkg.dev/PROJECT_ID/ad-inventory/dashboard:latest \
  --platform=managed \
  --region=us-central1 \
  --allow-unauthenticated \
  --service-account=dashboard-sa@PROJECT_ID.iam.gserviceaccount.com \
  --memory=1Gi \
  --timeout=300
```

#### 6. CI/CD Setup (GitHub Trigger)

```bash
# Connect GitHub repository (requires OAuth in Cloud Console)
gcloud builds triggers create github \
  --repo-name=ad-inventory-forecast \
  --repo-owner=vxa8502 \
  --branch-pattern='^main$' \
  --build-config=cloudbuild.yaml
```

The `cloudbuild.yaml` pipeline runs: **test** (pytest) -> **build** -> **push** -> **deploy**.

### Streamlit Community Cloud (Alternative)

For sharing without GCP context, deploy to Streamlit Community Cloud:

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Connect your GitHub account
3. Select `vxa8502/ad-inventory-forecast`
4. Set main file path: `app/main.py`
5. Add secrets in Advanced Settings:
   ```toml
   [gcp_service_account]
   type = "service_account"
   project_id = "your-project-id"
   private_key = "-----BEGIN PRIVATE KEY-----\n..."
   client_email = "dashboard-sa@project.iam.gserviceaccount.com"
   ```

### Local Development

```bash
# Install app dependencies
pip install -r requirements-app.txt

# Run Streamlit locally
streamlit run app/main.py
```

## Scaling to 10M Ad Slots

This prototype forecasts 19 ad units. Production ad servers manage 10M+ slots. Here's how the architecture extends:

### Retraining Cadence

| Model | Retraining Schedule | Rationale |
|-------|---------------------|-----------|
| **ARIMA+** | Weekly (Saturday 2 AM UTC) | Learns from recent traffic patterns; weekly balances freshness vs. compute cost |
| **ARIMA+ XREG** | Weekly (Saturday 2 AM UTC) | Same as ARIMA+; external regressors (holidays) updated quarterly |
| **TimesFM 2.5** | Never | Zero-shot foundation model; no training required |

### Cloud Scheduler Implementation

```bash
# Create Cloud Scheduler job for weekly ARIMA+ retraining
gcloud scheduler jobs create http arima-weekly-retrain \
  --location=us-central1 \
  --schedule="0 2 * * 6" \
  --uri="https://us-central1-aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/us-central1/pipelineJobs" \
  --http-method=POST \
  --oauth-service-account-email=pipeline-sa@PROJECT_ID.iam.gserviceaccount.com \
  --message-body='{"displayName": "arima-retrain-weekly", "templateUri": "gs://PROJECT_ID-pipelines/arima_retrain.yaml"}'

# Daily forecast refresh (6 AM before sales team)
gcloud scheduler jobs create http daily-forecast-refresh \
  --location=us-central1 \
  --schedule="0 6 * * *" \
  --uri="https://CLOUD_RUN_URL/api/refresh-forecasts" \
  --http-method=POST \
  --oidc-service-account-email=scheduler-sa@PROJECT_ID.iam.gserviceaccount.com
```

### Vertex AI Pipelines DAG

```python
# pipelines/arima_retrain.py (simplified)
from kfp.v2 import dsl
from kfp.v2.dsl import component

@component(base_image="python:3.11-slim")
def extract_recent_traffic(project_id: str, lookback_days: int) -> str:
    """Extract last N days of pageview data to staging table."""
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.ad_inventory.staging_recent`
    AS SELECT * FROM `{project_id}.ad_inventory.daily_impressions`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    """
    client.query(query).result()
    return f"{project_id}.ad_inventory.staging_recent"

@component(base_image="python:3.11-slim")
def train_arima(project_id: str, staging_table: str) -> str:
    """Retrain ARIMA+ model on recent data."""
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE MODEL `{project_id}.ad_inventory.arima_model`
    OPTIONS(model_type='ARIMA_PLUS', time_series_timestamp_col='date',
            time_series_data_col='daily_impressions', time_series_id_col='ad_unit',
            holiday_region='US')
    AS SELECT date, ad_unit, daily_impressions FROM `{staging_table}`
    """
    client.query(query).result()
    return f"{project_id}.ad_inventory.arima_model"

@component(base_image="python:3.11-slim")
def evaluate_and_promote(project_id: str, model_path: str, mape_threshold: float):
    """Evaluate new model; promote only if MAPE improves."""
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    # Run ML.EVALUATE, compare to previous, swap if better
    # Rollback logic omitted for brevity

@dsl.pipeline(name="arima-weekly-retrain")
def arima_retrain_pipeline(project_id: str = "PROJECT_ID"):
    extract_task = extract_recent_traffic(project_id=project_id, lookback_days=730)
    train_task = train_arima(project_id=project_id, staging_table=extract_task.output)
    evaluate_and_promote(project_id=project_id, model_path=train_task.output, mape_threshold=0.20)
```

### Orchestration

Replace manual SQL scripts with **Vertex AI Pipelines**:
- Kubeflow-based DAGs for extract -> train -> evaluate -> promote
- Built-in experiment tracking and model versioning
- Automatic rollback on metric regression (MAPE > threshold)

### Scheduling

- **Weekly ARIMA+ retraining**: Cloud Scheduler triggers Saturday 2 AM UTC (low traffic)
- **Daily forecast refresh**: 6 AM before sales team starts
- **Anomaly alerts**: Pub/Sub notifications when probability > 0.95

### Model Strategy at Scale

| Dimension | ARIMA+ | TimesFM |
|-----------|--------|---------|
| Training | One model per ad unit (10M models) | Single zero-shot call |
| Retraining | Weekly per unit | None required |
| Cost | ~$0.001/unit/week | ~$0.0001/forecast |
| Cold-start | Needs 90+ days history | Works immediately |

**Hybrid approach**: Use TimesFM for cold-start and long-tail slots (80% of inventory, low volume). Reserve ARIMA+ XREG for top 20% revenue-driving slots where interpretability matters to stakeholders.

### Storage

Partitioned forecast tables reduce query costs 10-100x:

```sql
CREATE TABLE forecasts
PARTITION BY DATE(forecast_date)
CLUSTER BY ad_unit
```

### Serving

**BigQuery BI Engine** ($40/month for 1GB reservation) enables sub-second dashboard queries on 10M row tables, eliminating cold-start latency.

### Cost Model

| Component | 10M Slots/Month |
|-----------|-----------------|
| ARIMA+ training (top 2M) | ~$2,000 |
| TimesFM inference (8M) | ~$800 |
| BigQuery storage | ~$50 |
| BI Engine | ~$40 |
| **Total** | **~$3,000/month** |

At $3 CPM on 10M daily impressions, this represents <0.01% of monthly ad revenue.

## License

MIT

