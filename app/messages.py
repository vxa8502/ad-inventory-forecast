"""Centralized long-form markdown content for dashboard pages."""

from __future__ import annotations

__all__ = [
    # Author / Project
    "AUTHOR_NAME",
    "GITHUB_URL",
    "PROJECT_TAGLINE",
    # Main Dashboard
    "DASHBOARD_INTRO",
    "KEY_FINDING",
    "NAVIGATION_GUIDE",
    "DATA_COVERAGE",
    # Page Intros
    "FORECAST_EXPLORER_INTRO",
    "MODEL_COMPARISON_INTRO",
    "DECOMPOSITION_INTRO",
    "ANOMALY_DETECTION_INTRO",
    "ARCHITECTURE_INTRO",
    # Anomaly Detection
    "ANOMALY_DETECTION_ABOUT",
    "XREG_FAILURE_CALLOUT",
    "XREG_FAILURE_AD_UNITS",
    # Decomposition
    "DECOMPOSITION_INTERPRETABILITY_NOTE",
    "DECOMPOSITION_ABOUT",
    "STEP_CHANGE_CALLOUT",
    "HYPOTHESIS_CALLOUT_TEMPLATE",
    # Architecture
    "ARCHITECTURE_MERMAID",
    "MODEL_ROUTING_TABLE",
    "VOLATILITY_CLASSIFICATION",
    "ACTUAL_COST_ANALYSIS",
    "SCALING_SECTION",
    "WALK_FORWARD_METHODOLOGY",
]

# -----------------------------------------------------------------------------
# Author / Project Info (sidebar)
# -----------------------------------------------------------------------------

AUTHOR_NAME = "Victoria Alabi"
GITHUB_URL = "https://github.com/vxa8502/ad-inventory-forecast"
PROJECT_TAGLINE = (
    "Benchmarking BigQuery ML forecasting approaches against the problem "
    "Google Ad Manager solves for publishers."
)

# -----------------------------------------------------------------------------
# Main Dashboard (main.py)
# -----------------------------------------------------------------------------

DASHBOARD_INTRO = """
Publishers selling ad inventory face a constant tradeoff: **overbook and breach contracts**,
or **underbook and leave money on the table**. This dashboard explores which forecasting
approach helps minimize that revenue risk across different types of content.

We compare three BigQuery ML models on 19 ad units (Wikipedia articles as inventory proxy)
to understand when foundation models outperform statistical methods, and vice versa.
"""

KEY_FINDING = """
### Key Finding

**Foundation models outperform statistical methods on volatile content.**

TimesFM 2.5 achieved 17.9% forecast error (roughly $18 at risk per $100 in ad revenue)
compared to ARIMA+ at 35.3%. The gap widens dramatically on event-driven content:
on Barbie (film), the feature-based model missed by 10x because it couldn't adapt
to unprecedented viral traffic.
"""

NAVIGATION_GUIDE = """
### Navigation

Use the sidebar to explore:

- **Forecast Explorer** - Compare model predictions
- **Model Comparison** - Metrics heatmap & business impact
- **Decomposition** - ARIMA component breakdown
- **Anomaly Detection** - Flagged outliers with events
- **Architecture** - Methodology & data flow
"""

DATA_COVERAGE = """
### Data Coverage

| Metric | Value |
|--------|-------|
| Training Period | Jan 2023 - Jun/Sep 2024 (expanding window per fold) |
| Test Folds | Jul-Sep 2024, Oct-Dec 2024 |
| Ad Units | 19 (of 34 in full dataset) |
| Forecast Horizon | 92 days per fold |

---

*Data sourced from Wikimedia Foundation pageview API via BigQuery public datasets.*
"""

# -----------------------------------------------------------------------------
# PM-Friendly Page Intros
# -----------------------------------------------------------------------------

FORECAST_EXPLORER_INTRO = """
Sales teams need to know how many impressions they can promise advertisers over the next
90 days. This page shows what each model predicts versus what actually happened, so you
can see which forecasts you'd want to bet revenue commitments on.
"""

MODEL_COMPARISON_INTRO = """
Not all models work equally well on all content. Some articles have steady, predictable
traffic; others spike unpredictably during viral moments. This page shows which model
wins where, and translates forecast accuracy into dollars at risk.
"""

DECOMPOSITION_INTRO = """
Understanding *why* a forecast looks the way it does builds trust with stakeholders.
This page breaks down ARIMA+ predictions into their building blocks: long-term growth,
weekly patterns, and holiday effects. When the forecast seems off, this helps diagnose why.
"""

ANOMALY_DETECTION_INTRO = """
Traffic spikes and drops can signal ad server issues, viral content, or major real-world
events. This page surfaces unusual patterns the model detected and cross-references them
with known events like product launches and Super Bowls. Anomalies that match events
validate the model is capturing real phenomena, not just noise.
"""

ARCHITECTURE_INTRO = """
This page documents how the forecasting system works: where the data comes from, how models
are trained and evaluated, and what it would take to scale this to millions of ad slots.
Useful for technical stakeholders evaluating production readiness.
"""

# -----------------------------------------------------------------------------
# Anomaly Detection (pages/4_Anomaly_Detection.py)
# -----------------------------------------------------------------------------

ANOMALY_DETECTION_ABOUT = """
### About Anomaly Detection

Anomalies are detected using BigQuery ML's `ML.DETECT_ANOMALIES` function on ARIMA+ models.

**Detection Method:**
- Model learns expected patterns (trend + seasonality + holidays)
- Points outside the confidence interval are flagged
- Probability indicates confidence that the point is anomalous

**Known Events:**
- Super Bowl (NFL, Super_Bowl articles)
- Barbenheimer release weekend (Barbie, Oppenheimer)
- iPhone 15 launch (IPhone, Apple)
- Taylor Swift Eras Tour announcements
- Bitcoin ETF approval and halving events

Anomalies that align with known events validate the model is capturing
real-world phenomena, not just noise.
"""

XREG_FAILURE_CALLOUT = """
> **Why XREG Failed Here:** During training, Barbenheimer (July 2023) created
> peak-hype anomalies that XREG learned as the baseline level. When traffic
> dropped 85% post-theatrical release, the model kept predicting peak levels
> in the test period, causing errors exceeding 10x. TimesFM adapts better
> because it learned from 100B+ time points across domains, not just this series.
"""

# Ad units that trigger the XREG failure callout
XREG_FAILURE_AD_UNITS: frozenset[str] = frozenset({
    "Barbie_(film)",
    "Oppenheimer_(film)",
})

# -----------------------------------------------------------------------------
# Decomposition (pages/3_Decomposition.py)
# -----------------------------------------------------------------------------

DECOMPOSITION_INTERPRETABILITY_NOTE = """
> **Note:** Decomposition is available for ARIMA+ only. TimesFM 2.5 is a black-box
> foundation model that does not expose internal components. This demonstrates the
> interpretability vs. accuracy tradeoff: TimesFM achieves lower MAPE (17.9%) but
> ARIMA+ provides explainable trend, seasonality, and holiday effects.
"""

DECOMPOSITION_ABOUT = """
### Understanding ARIMA Decomposition

**Trend**: Long-term direction of pageviews (growth, decline, or stable).

**Weekly Seasonality**: Day-of-week patterns (e.g., weekday vs weekend traffic).

**Yearly Seasonality**: Annual cycles (e.g., holiday season peaks, summer dips).

**Holiday Effect**: Specific impact of US holidays (detected via BigQuery ML).

**Step Change**: Sudden level shifts detected by the model (e.g., viral events, post-release decay).

The sum of all components approximates the original time series. Vertical dashed lines
indicate major US holidays with effect magnitude where detected.
"""

STEP_CHANGE_CALLOUT = """
**Step Change Detected**: This article shows sudden level shifts that ARIMA+ captured
via `adjust_step_changes=TRUE`. {count}/{total} articles ({pct}%) in this dataset
have step changes, typically from viral events or content lifecycle transitions.
"""

HYPOTHESIS_CALLOUT_TEMPLATE = """
**Pre-Training Hypothesis Validated**

{metric}: {actual} (hypothesis: {hypothesis}) {status}

*{interpretation}*
"""

# -----------------------------------------------------------------------------
# Architecture (pages/5_Architecture.py)
# -----------------------------------------------------------------------------

ARCHITECTURE_MERMAID = """
flowchart TD
    subgraph Data["Data Layer"]
        WP[Wikipedia Pageviews API]
        BQ[(BigQuery Public Dataset)]
        WP --> BQ
    end

    subgraph Features["Feature Engineering"]
        DI[daily_impressions]
        HOL[US Holidays]
        BQ --> DI
        DI --> HOL
    end

    subgraph Models["Model Training"]
        TFM[TimesFM 2.5<br/>Zero-shot Foundation]
        ARP[ARIMA+<br/>Statistical Baseline]
        XRG[ARIMA+ XREG<br/>Holiday Regressors]
        DI --> TFM
        DI --> ARP
        HOL --> XRG
    end

    subgraph Evaluation["Walk-Forward Evaluation"]
        F1[Fold 1: Jul-Sep 2024]
        F2[Fold 2: Oct-Dec 2024]
        TFM --> F1
        ARP --> F1
        XRG --> F1
        TFM --> F2
        ARP --> F2
        XRG --> F2
    end

    subgraph Output["Output Tables"]
        FC[forecasts]
        MT[model_metrics]
        MC[model_comparison]
        BI[business_impact]
        FD[forecast_decomposition]
        AN[anomalies]
        F1 --> FC
        F2 --> FC
        FC --> MT
        MT --> MC
        MC --> BI
        ARP --> FD
        ARP --> AN
    end

    subgraph Dashboard["Streamlit Dashboard"]
        P1[Forecast Explorer]
        P2[Model Comparison]
        P3[Decomposition]
        P4[Anomaly Detection]
        FC --> P1
        MC --> P2
        BI --> P2
        FD --> P3
        AN --> P4
    end
"""

MODEL_ROUTING_TABLE = """
### Model Selection Matrix

Production model routing based on content characteristics:

| Content Type | Recommended Model | Rationale |
|--------------|-------------------|-----------|
| **Stable** (CV < 0.5) | ARIMA+ XREG | Holiday regressors capture predictable patterns; lowest MAPE on stable content |
| **Event-Driven** (CV >= 0.5) | TimesFM 2.5 | Foundation model handles distribution shift; XREG fails catastrophically on viral content |
| **Cold-Start** (< 90 days history) | TimesFM 2.5 | Zero-shot inference requires no training; ARIMA+ needs sufficient history for seasonality |
| **Interpretability Required** | ARIMA+ | ML.EXPLAIN_FORECAST provides trend/seasonal/holiday decomposition; TimesFM is black-box |

**Key Insight:** No single model wins everywhere. Production systems should route
based on content volatility and business requirements.
"""

VOLATILITY_CLASSIFICATION = """
### Volatility Classification Rules

**Coefficient of Variation (CV)** = standard deviation / mean

CV measures relative volatility independent of traffic scale. Used for model routing:

| CV Range | Classification | Model Recommendation | Examples |
|----------|----------------|----------------------|----------|
| CV < 0.5 | Stable | ARIMA+ or ARIMA+ XREG | Diabetes, Python_(programming_language), Apple_Inc. |
| CV >= 0.5 | Event-Driven | TimesFM preferred | Barbie_(film), Super_Bowl, Bitcoin |

**Why CV = 0.5?** XREG MAPE degrades sharply above this threshold; TimesFM
remains stable regardless of volatility. Use CV < 0.3 for stricter XREG eligibility.
"""

ACTUAL_COST_ANALYSIS = """
### Actual Project Costs

**Total spend: ~$6.23** (within $300 free trial)

| Operation | Data Processed | Cost |
|-----------|----------------|------|
| Initial 2-year extract (34 articles) | 1.1 TB | $5.50 |
| Iterative query development | ~150 GB | $0.75 |
| Model training (3 models x 2 folds) | ~200 MB | <$0.01 |
| Dashboard queries (dev + testing) | ~50 GB | $0.25 |

**Production Cost Estimate:**

| Scenario | Monthly Cost |
|----------|--------------|
| Dashboard (low traffic, cached) | $1-3/month |
| Weekly ARIMA+ retraining (19 units) | <$1/month |
| Daily forecasts (no retraining) | <$0.50/month |

**Cost Controls Applied:**
- `MAX_BYTES_BILLED = 3.5 TB` guard on all queries
- Partition filters on `datehour` (Wikipedia tables)
- 1-hour query cache via `st.cache_data`
- Limited to 19 high-value ad units (not 50+)
"""

SCALING_SECTION = """
### How This Scales to 10M Ad Slots

This prototype handles 19 ad units. Scaling to production ad server scale (10M+ slots)
requires architectural changes:

**1. Orchestration: Vertex AI Pipelines**
```
Training Pipeline (weekly):
  Extract fresh data → Train ARIMA+ per unit → Evaluate → Promote to serving
```
- Replace manual SQL scripts with Kubeflow-based DAGs
- Built-in experiment tracking and model versioning
- Automatic rollback on metric regression

**2. Scheduling: Cloud Scheduler + Cloud Functions**
- Weekly ARIMA+ retraining (Saturday nights, low traffic)
- Daily forecast refresh (6 AM before sales team starts)
- Anomaly alerts via Pub/Sub when probability > 0.95

**3. Model Strategy at Scale**

| Scale | ARIMA+ | TimesFM |
|-------|--------|---------|
| Training | One model per ad unit (10M models) | Single zero-shot call |
| Retraining | Weekly per unit | None required |
| Cost | ~$0.001/unit/week | ~$0.0001/forecast |
| Cold-start | Needs 90+ days | Works immediately |

**Recommendation:** Hybrid approach
- TimesFM for cold-start and long-tail (80% of slots, low volume)
- ARIMA+ XREG for top 20% revenue-driving slots (interpretability matters)

**4. Storage: Partitioned Forecast Tables**
```sql
CREATE TABLE forecasts
PARTITION BY DATE(forecast_date)
CLUSTER BY ad_unit
```
- Query costs drop 10-100x with partition pruning
- Clustering enables efficient ad unit lookups

**5. Serving: BigQuery BI Engine**
- Sub-second dashboard queries on 10M row tables
- $40/month for 1GB BI Engine reservation
- Eliminates cold-start query latency
"""

WALK_FORWARD_METHODOLOGY = """
### Walk-Forward Validation Methodology

**Why Walk-Forward?**

Standard train/test splits leak future information into training.
Walk-forward validation simulates production forecasting:

1. Train only on data available at forecast time
2. Generate forecasts for a forward horizon
3. Evaluate on actuals that arrive later
4. Expand training window and repeat

**Our Configuration:**

| Fold | Training Period | Holdout Period | Horizon |
|------|-----------------|----------------|---------|
| Fold 1 | Jan 2023 - Jun 2024 | Jul - Sep 2024 | 92 days |
| Fold 2 | Jan 2023 - Sep 2024 | Oct - Dec 2024 | 92 days |

**Why Two Folds?**

- **Fold 1** (summer): Tests model on typical seasonal patterns
- **Fold 2** (Q4): Tests model on holiday season + year-end volatility
- **Cross-fold average**: Single headline metric for model comparison

**Why 92-Day Horizon?**

Mirrors Google Ad Manager's 90-day forward booking window.
Sales teams need 3-month visibility for guaranteed campaigns.

**Production Reality:**

At Google scale, walk-forward runs continuously:
- Models retrain weekly on expanding windows
- Forecasts refresh daily with latest actuals
- Metrics compare rolling 30/60/90 day windows
"""
