"""Canonical event definitions for ad units.

These dates have legitimate traffic anomalies that models should capture,
not treat as outliers to be cleaned.
"""

from __future__ import annotations

# Event categories for forecastability analysis
CATEGORY_SPORTS = "Sports Event"
CATEGORY_ENTERTAINMENT = "Entertainment Release"
CATEGORY_PRODUCT = "Product Launch"
CATEGORY_VIRAL = "Viral-Unpredictable"
CATEGORY_SEASONAL = "Seasonal"

# Forecastability guidance by category
FORECASTABILITY: dict[str, dict[str, str]] = {
    CATEGORY_SPORTS: {
        "level": "High",
        "guidance": "Add custom holiday regressors for game days",
    },
    CATEGORY_ENTERTAINMENT: {
        "level": "Medium",
        "guidance": "Release dates known, magnitude uncertain",
    },
    CATEGORY_PRODUCT: {
        "level": "Medium",
        "guidance": "Announcement dates predictable, impact varies",
    },
    CATEGORY_VIRAL: {
        "level": "Low",
        "guidance": "Wide confidence intervals appropriate",
    },
    CATEGORY_SEASONAL: {
        "level": "High",
        "guidance": "Annual patterns captured by yearly seasonality",
    },
}

# Events structure: (date, description, category)
KNOWN_EVENTS: dict[str, list[tuple[str, str, str]]] = {
    "Taylor_Swift": [
        ("2023-03-17", "Eras Tour kickoff", CATEGORY_ENTERTAINMENT),
        ("2023-07-22", "Eras Tour film announcement", CATEGORY_ENTERTAINMENT),
        ("2024-02-04", "Super Bowl LVIII attendance", CATEGORY_SPORTS),
    ],
    "NFL": [
        ("2023-02-12", "Super Bowl LVII (Chiefs vs Eagles)", CATEGORY_SPORTS),
        ("2023-09-07", "2023 season kickoff", CATEGORY_SPORTS),
        ("2024-02-11", "Super Bowl LVIII (Chiefs vs 49ers)", CATEGORY_SPORTS),
        ("2024-09-05", "2024 season kickoff", CATEGORY_SPORTS),
    ],
    "Bitcoin": [
        ("2024-01-10", "Bitcoin ETF approval", CATEGORY_VIRAL),
        ("2024-03-14", "All-time high $73K", CATEGORY_VIRAL),
        ("2024-04-19", "Bitcoin halving", CATEGORY_VIRAL),
    ],
    "Super_Bowl": [
        ("2023-02-12", "Super Bowl LVII", CATEGORY_SPORTS),
        ("2024-02-11", "Super Bowl LVIII", CATEGORY_SPORTS),
    ],
    "Barbie_(film)": [
        ("2023-07-21", "Theatrical release", CATEGORY_ENTERTAINMENT),
        ("2023-07-22", "Opening weekend peak", CATEGORY_ENTERTAINMENT),
    ],
    "Oppenheimer_(film)": [
        ("2023-07-21", "Theatrical release (Barbenheimer)", CATEGORY_ENTERTAINMENT),
    ],
    "ChatGPT": [
        ("2023-03-14", "GPT-4 release", CATEGORY_VIRAL),
        ("2023-11-06", "GPT-4 Turbo announcement", CATEGORY_VIRAL),
    ],
    "Ozempic": [
        ("2023-06-01", "Celebrity weight loss coverage peaks", CATEGORY_VIRAL),
    ],
    "Influenza": [
        ("2023-01-15", "Winter peak 2023", CATEGORY_SEASONAL),
        ("2024-01-15", "Winter peak 2024", CATEGORY_SEASONAL),
    ],
    "IPhone": [
        ("2023-09-12", "iPhone 15 announcement", CATEGORY_PRODUCT),
        ("2023-09-22", "iPhone 15 release", CATEGORY_PRODUCT),
    ],
    "Apple_Inc.": [
        ("2023-09-12", "iPhone 15 announcement", CATEGORY_PRODUCT),
        ("2023-06-05", "WWDC 2023 (Vision Pro)", CATEGORY_PRODUCT),
    ],
}

SAMPLE_AD_UNITS: list[str] = [
    "Taylor_Swift",
    "NFL",
    "Bitcoin",
    "Influenza",
    "ChatGPT",
]

# Separator for building event lookup keys (ad_unit + separator + date)
EVENT_KEY_SEPARATOR = "_"


def build_event_key(ad_unit: str, date: str) -> str:
    """Build a lookup key for event matching.

    Args:
        ad_unit: Ad unit name.
        date: Date string (YYYY-MM-DD format).

    Returns:
        Combined key for O(1) event lookup.
    """
    return f"{ad_unit}{EVENT_KEY_SEPARATOR}{date}"


def get_forecastability_guidance(category: str) -> str:
    """Get forecastability guidance for an event category.

    Extracts the 'guidance' field from FORECASTABILITY dict,
    handling missing categories gracefully.

    Args:
        category: Event category (e.g., "Sports Event", "Viral-Unpredictable").

    Returns:
        Guidance string, or empty string if category not found.
    """
    if not category:
        return ""
    return FORECASTABILITY.get(category, {}).get("guidance", "")


# Pre-built event keys for O(1) lookup (avoids rebuilding on every page load)
KNOWN_EVENT_KEYS: frozenset[str] = frozenset(
    build_event_key(ad_unit, date)
    for ad_unit, events in KNOWN_EVENTS.items()
    for date, _, _ in events
)
