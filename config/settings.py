"""Centralized configuration for the ad inventory forecasting project."""

from __future__ import annotations

import os
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
DATASET = "ad_inventory"
LOCATION = "US"

DATE_START = "2023-01-01"
DATE_END = "2024-12-31"

MAX_BYTES_BILLED = 3500 * 1024**3  # 3.5 TB extraction guard (~$17.50)

ARTICLE_VERTICALS: dict[str, list[str]] = {
    "Technology": [
        "Python_(programming_language)",
        "Artificial_intelligence",
        "ChatGPT",
        "IPhone",
        "Google",
        "Microsoft",
        "Tesla,_Inc.",
    ],
    "Sports": [
        "NFL",
        "NBA",
        "Super_Bowl",
        "Premier_League",
        "LeBron_James",
        "UFC",
    ],
    "Entertainment": [
        "Taylor_Swift",
        "Netflix",
        "YouTube",
        "Barbie_(film)",
        "Oppenheimer_(film)",
        "Beyonce",
        "Spotify",
    ],
    "Finance": [
        "Bitcoin",
        "Stock_market",
        "Federal_Reserve",
        "Amazon_(company)",
        "Apple_Inc.",
        "Inflation",
        "S&P_500",
    ],
    "Health": [
        "Mental_health",
        "Ozempic",
        "Cancer",
        "Diabetes",
        "Exercise",
        "Influenza",
        "Sleep",
    ],
}

# Flat list for backward compatibility
ARTICLES: list[str] = [
    article for articles in ARTICLE_VERTICALS.values() for article in articles
]

# Walk-forward validation folds
FOLD_CONFIGS = [
    {
        "name": "fold_1",
        "train_start": "2023-01-01",
        "train_end": "2024-06-30",
        "test_start": "2024-07-01",
        "test_end": "2024-09-30",
    },
    {
        "name": "fold_2",
        "train_start": "2023-01-01",
        "train_end": "2024-09-30",
        "test_start": "2024-10-01",
        "test_end": "2024-12-31",
    },
]

# Pre-indexed lookup for O(1) fold config access
FOLD_CONFIGS_BY_NAME = {f["name"]: f for f in FOLD_CONFIGS}

# Type alias for fold names - enables static type checking
FoldName = Literal["fold_1", "fold_2"]

MODEL_NAMES = ["timesfm_2_5", "arima_plus", "arima_plus_xreg"]
METRIC_NAMES = ["mape", "rmse", "mae", "mase", "coverage"]
FORECAST_HORIZON = 92
FORECAST_HORIZON_OPTIONS = [30, 60, 90]
CONFIDENCE_LEVEL = 0.95

# Volatility thresholds for model routing recommendation
# CV < 0.3: Stable content, XREG eligible
# 0.3 <= CV < 0.5: Moderate volatility, ARIMA+ or TimesFM
# CV >= 0.5: Event-driven content, TimesFM preferred
VOLATILITY_CV_XREG_ELIGIBLE = 0.3
VOLATILITY_CV_THRESHOLD = 0.5

# Pre-model check thresholds
DISTRIBUTION_MISSING_PCT_THRESHOLD = 20.0
STEP_CHANGE_WINDOW_DAYS = 30
STEP_CHANGE_RATIO_THRESHOLD = 2.0

# CI analysis thresholds (percentage of forecast value)
CI_NARROW_THRESHOLD_PCT = 20
CI_WIDE_THRESHOLD_PCT = 100

# Display limits for CLI output (avoid hardcoded .head(10) or [:10])
MAX_DISPLAY_ITEMS = 10

# Visualization colors (centralized theme)
PLOT_COLORS = {
    "primary": "#2E86AB",
    "secondary": "#E94F37",
    "dark": "#1B1B1E",
    "desktop": "#44AF69",
    "mobile": "#F8333C",
    "above_median": "#2E86AB",
    "below_median": "#A23B72",
}

# Plotting constants
PLOT_FIGURE_SIZE = (14, 10)
PLOT_ANNOTATION_HEIGHT_PCT = 0.95

# Spot-check validation thresholds
WEEKEND_EFFECT_THRESHOLD = 0.10       # 10% weekend dip for tech articles
WEEKLY_AMPLITUDE_THRESHOLD = 0.20     # 20% weekly swing for sports
WEEKEND_EFFECT_STRONG_THRESHOLD = 0.15  # 15% weekend dip for finance
YEARLY_AMPLITUDE_THRESHOLD = 0.30     # 30% annual swing for health

# Anomaly diagnosis thresholds (diagnose_anomalies.py)
YEAR_SHIFT_SIGNIFICANT_PCT = 5        # Significant year-over-year shift
STOCK_WEEKEND_THRESHOLD_PCT = -15     # Stock market weekend bias
BITCOIN_WEEKEND_THRESHOLD_PCT = -10   # Bitcoin weekend threshold
TREND_SIGNIFICANT_PCT = 20            # Significant overall trend
BASELINE_SHIFT_SIGNIFICANT_PCT = 15   # Significant baseline shift

def _parse_bool_env(key: str, default: bool = True) -> bool:
    """Parse boolean from environment variable.

    Accepts: true, True, TRUE, 1, yes, Yes, YES (and inverses for False).

    Args:
        key: Environment variable name.
        default: Default value if not set.

    Returns:
        Parsed boolean value.
    """
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes")


# TimesFM 2.5 requires BigQuery Enterprise or Enterprise Plus edition.
# Set to False to skip TimesFM and avoid cryptic "AI.FORECAST not available" errors.
# See: https://cloud.google.com/bigquery/docs/ai-forecast
TIMESFM_ENABLED = _parse_bool_env("TIMESFM_ENABLED", default=True)

@lru_cache(maxsize=1)
def validate_horizon_alignment() -> bool:
    """Validate FORECAST_HORIZON matches each fold's test period length.

    Call this explicitly before running pipelines. Raises ValueError if
    any fold's test period length doesn't match FORECAST_HORIZON.

    This validation is idempotent via lru_cache - runs once per process.

    Returns:
        True if validation passes.

    Raises:
        ValueError: If any fold's test period doesn't match FORECAST_HORIZON.
    """
    for fold in FOLD_CONFIGS:
        test_start = date.fromisoformat(fold["test_start"])
        test_end = date.fromisoformat(fold["test_end"])
        test_days = (test_end - test_start).days + 1
        if test_days != FORECAST_HORIZON:
            raise ValueError(
                f"Fold '{fold['name']}' has {test_days} test days but "
                f"FORECAST_HORIZON={FORECAST_HORIZON}. These must match for "
                f"proper forecast alignment."
            )
    return True
