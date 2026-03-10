"""Architecture - Methodology and data flow documentation."""

from __future__ import annotations

import streamlit as st
from streamlit_mermaid import st_mermaid

from app.constants import DEFAULT_CPM
from app.utils.helpers import init_page
from app.messages import (
    ARCHITECTURE_INTRO,
    ARCHITECTURE_MERMAID,
    MODEL_ROUTING_TABLE,
    VOLATILITY_CLASSIFICATION,
    ACTUAL_COST_ANALYSIS,
    SCALING_SECTION,
    WALK_FORWARD_METHODOLOGY,
)

init_page("Architecture", ARCHITECTURE_INTRO)

# -----------------------------------------------------------------------------
# Pipeline Diagram (Mermaid)
# -----------------------------------------------------------------------------
st.divider()
st.subheader("Pipeline Architecture")
st_mermaid(ARCHITECTURE_MERMAID, height=700)

# -----------------------------------------------------------------------------
# Model Routing Table
# -----------------------------------------------------------------------------
st.divider()
st.markdown(MODEL_ROUTING_TABLE)

# -----------------------------------------------------------------------------
# Volatility Classification Rules
# -----------------------------------------------------------------------------
st.divider()
st.markdown(VOLATILITY_CLASSIFICATION)

# -----------------------------------------------------------------------------
# Models Overview
# -----------------------------------------------------------------------------
st.divider()
st.subheader("Model Comparison")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
**TimesFM 2.5**

Google's foundation model for time series.

- 200M parameter transformer
- Pre-trained on 100B+ time points
- Zero-shot (no training required)
- **17.9% MAPE** (~$18 risk per $100 revenue)

Best for: Event-driven, cold-start
""")

with col2:
    st.markdown("""
**ARIMA+**

BigQuery ML's enhanced ARIMA.

- Auto hyperparameter tuning (AIC)
- Holiday region detection (US)
- Spike/dip cleaning, step changes
- **35.3% MAPE** (~$35 risk per $100 revenue)

Best for: Interpretability required
""")

with col3:
    st.markdown("""
**ARIMA+ XREG**

ARIMA+ with external regressors.

- Explicit US holiday features
- Calendar regressors
- Assumes stable relationships
- **73.8% MAPE** (fails on volatile content)

Best for: Stable content (CV < 0.5)
""")

# -----------------------------------------------------------------------------
# Walk-Forward Validation
# -----------------------------------------------------------------------------
st.divider()
st.markdown(WALK_FORWARD_METHODOLOGY)

# -----------------------------------------------------------------------------
# BigQuery Tables
# -----------------------------------------------------------------------------
st.divider()
st.subheader("BigQuery Tables")

st.markdown(f"""
| Table | Description | Rows |
|-------|-------------|------|
| `daily_impressions` | Aggregated pageviews per ad unit per day | 24,854 |
| `forecasts` | Model predictions with 95% confidence intervals | 18,768 |
| `model_metrics` | Per-ad-unit metrics (MAPE, RMSE, MAE, MASE, Coverage) | 1,020 |
| `model_comparison` | Aggregate metrics per model per fold | 45 |
| `business_impact` | Revenue at risk calculations at ${DEFAULT_CPM:.2f} CPM | 204 |
| `forecast_decomposition` | ARIMA+ trend/seasonal/holiday components | 6,256 |
| `anomalies` | ML.DETECT_ANOMALIES output with probability scores | 40,324 |

*Row counts as of March 2026.*
""")

# -----------------------------------------------------------------------------
# Cost Analysis
# -----------------------------------------------------------------------------
st.divider()
st.markdown(ACTUAL_COST_ANALYSIS)

# -----------------------------------------------------------------------------
# Scaling Section
# -----------------------------------------------------------------------------
st.divider()
st.markdown(SCALING_SECTION)

# -----------------------------------------------------------------------------
# Deployment
# -----------------------------------------------------------------------------
st.divider()
st.subheader("Deployment")

st.markdown("""
**Local Development:**
```bash
pip install -r requirements-app.txt
streamlit run app/main.py
```

**Docker (Cloud Run ready):**
```bash
docker build -t ad-forecast-dashboard .
docker run -p 8080:8080 \\
  -v ~/.config/gcloud:/root/.config/gcloud \\
  ad-forecast-dashboard
```

**Cloud Run Deployment:**
```bash
gcloud builds submit --tag gcr.io/PROJECT_ID/ad-inventory-dashboard
gcloud run deploy ad-inventory-dashboard \\
  --image gcr.io/PROJECT_ID/ad-inventory-dashboard \\
  --platform managed \\
  --region us-central1 \\
  --allow-unauthenticated
```
""")
