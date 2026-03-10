"""Model Comparison - Metrics, heatmap, and business impact analysis."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from app.components.charts import comparison_heatmap, mape_boxplot, residuals_histogram
from app.components.filters import model_comparison_filters
from app.components.tables import business_impact_table, metrics_table, summary_metrics_card
from app.constants import DEFAULT_CPM, HIGH_RISK_THRESHOLD, METRIC_MAPE, ValidationMessages, FOLD_CROSS_AVG, FOLD_LABELS, TabNames
from app.data.queries import (
    get_business_impact,
    get_metrics_detail,
    get_model_comparison,
    get_residuals,
    get_volatility_metrics,
)
from app.utils.helpers import (
    build_lookup_dict,
    display_chart_or_warning,
    display_metric_columns,
    fetch_with_fold_fallback,
    filter_by_content_type,
    init_page,
)
from app.messages import MODEL_COMPARISON_INTRO

init_page("Model Comparison", MODEL_COMPARISON_INTRO)

# Sidebar filters
selected_fold, content_type = model_comparison_filters()

# Fetch comparison data (spinner handled by @cached_query decorator)
comparison_df: pd.DataFrame = get_model_comparison(selected_fold)

# Fetch volatility data for content type filtering
volatility_df: pd.DataFrame = get_volatility_metrics()

# Headline stat cards - show MAPE at a glance BEFORE tabs
st.subheader("MAPE at a Glance")
summary_metrics_card(comparison_df, metric_name=METRIC_MAPE)
st.divider()

# Fetch metrics data once for tabs 2 and 3 (DRY)
detail_metrics_df: pd.DataFrame = fetch_with_fold_fallback(selected_fold, get_metrics_detail)

# Apply content type filter to per-ad-unit data
detail_metrics_df = filter_by_content_type(detail_metrics_df, volatility_df, content_type)

# Tabs for different views
tab_headline, tab_heatmap, tab_dist, tab_impact = st.tabs(TabNames.COMPARISON_TABS)

with tab_headline:
    st.subheader("Aggregate Metrics")

    if selected_fold == FOLD_CROSS_AVG:
        st.caption("Cross-fold average: mean of Fold 1 and Fold 2 metrics")
    else:
        st.caption(f"Showing metrics for {FOLD_LABELS.get(selected_fold, selected_fold)}")

    metrics_table(comparison_df, highlight_best=True)

    st.markdown("""
    **Interpretation:**
    - **MAPE**: Mean Absolute Percentage Error (lower is better)
    - **RMSE**: Root Mean Squared Error (lower is better)
    - **MAE**: Mean Absolute Error (lower is better)
    - **MASE**: Mean Absolute Scaled Error (lower is better)
    - **Coverage**: % of actuals within 95% CI (higher is better, target ~95%)
    """)

with tab_heatmap:
    st.subheader("MAPE by Ad Unit")

    if display_chart_or_warning(
        detail_metrics_df,
        required_column="ad_unit",
        chart_builder=comparison_heatmap,
        warning_message=ValidationMessages.NO_PER_UNIT_METRICS,
    ):
        st.markdown("""
        **Reading the heatmap:**
        - Green = Low MAPE (accurate forecasts)
        - Red = High MAPE (poor forecasts)
        - Cells show MAPE percentage
        """)

with tab_dist:
    st.subheader("Error Distributions")

    # Residuals histogram (forecast - actual)
    residuals_df: pd.DataFrame = fetch_with_fold_fallback(
        selected_fold, get_residuals, caption_suffix="residuals"
    )

    # Apply content type filter
    residuals_df = filter_by_content_type(residuals_df, volatility_df, content_type)

    if not residuals_df.empty:
        fig = residuals_histogram(residuals_df)
        st.plotly_chart(fig, width="stretch")
        st.markdown("""
        **Residuals interpretation:**
        - Residual = Forecast - Actual
        - Centered at 0 = unbiased model
        - Positive skew = tends to overforecast
        - Negative skew = tends to underforecast
        """)
    else:
        st.info("No residual data available for this fold.")

    st.divider()

    # MAPE boxplot
    st.subheader("MAPE Distribution by Model")
    if display_chart_or_warning(
        detail_metrics_df,
        required_column="metric_value",
        chart_builder=mape_boxplot,
        warning_message=ValidationMessages.NO_METRICS_FOR_FOLD,
    ):
        st.markdown("""
        **Box plot interpretation:**
        - Box shows interquartile range (25th-75th percentile)
        - Line in box is median MAPE
        - Whiskers extend to 1.5x IQR
        - Dots are outliers (high-error ad units)
        """)

with tab_impact:
    st.subheader("Revenue at Risk")

    impact_df: pd.DataFrame = fetch_with_fold_fallback(
        selected_fold, get_business_impact, caption_suffix="revenue impact"
    )

    # Apply content type filter
    impact_df = filter_by_content_type(impact_df, volatility_df, content_type)

    if not impact_df.empty and "annual_revenue_at_risk" in impact_df.columns:
        # Calculate per-model totals for comparison
        model_totals = impact_df.groupby("model_name").agg({
            "annual_revenue_at_risk": "sum",
            "avg_daily_impressions": "sum",
        }).reset_index()

        # Extract risk by model using lookup dict for cleaner access
        risk_by_model = build_lookup_dict(model_totals, "model_name", "annual_revenue_at_risk")
        impressions_by_model = build_lookup_dict(model_totals, "model_name", "avg_daily_impressions")

        timesfm_risk = risk_by_model.get("timesfm_2_5")
        arima_risk = risk_by_model.get("arima_plus")
        total_impressions = impressions_by_model.get("timesfm_2_5", 0)

        # Comparative insight: savings per 1M impressions
        if timesfm_risk is not None and arima_risk is not None:

            if total_impressions > 0:
                savings = arima_risk - timesfm_risk
                savings_per_1m = (savings / total_impressions) * 1_000_000

                st.success(
                    f"**TimesFM reduces revenue-at-risk by ~${savings_per_1m:,.0f} "
                    f"per 1M daily impressions vs. ARIMA+**"
                )

        st.divider()

        total_risk: float = impact_df["annual_revenue_at_risk"].sum()
        max_risk: float = impact_df["annual_revenue_at_risk"].max()
        high_risk_count: int = len(
            impact_df[impact_df["annual_revenue_at_risk"] > HIGH_RISK_THRESHOLD]
        )
        threshold_display: str = f"${HIGH_RISK_THRESHOLD:,.0f}"

        display_metric_columns({
            "Total Annual Risk": f"${total_risk:,.0f}",
            "Max Single Risk": f"${max_risk:,.0f}",
            f"High-Risk Items (>{threshold_display})": str(high_risk_count),
        })

        st.divider()

        business_impact_table(impact_df)

        st.markdown(f"""
        **Methodology:**
        - Revenue at Risk = Avg Daily Impressions x CPM (${DEFAULT_CPM:.2f}) x MAPE% / 1000
        - Annual Risk = Daily Risk x 365
        - Higher MAPE = more forecasting error = greater planning risk
        """)
    else:
        st.info(ValidationMessages.NO_BUSINESS_IMPACT)
