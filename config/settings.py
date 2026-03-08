"""Centralized configuration for the ad inventory forecasting project."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
DATASET = "ad_inventory"
LOCATION = "US"

DATE_START = "2023-01-01"
DATE_END = "2024-12-31"

MAX_BYTES_BILLED = 3500 * 1024**3  # 3.5 TB extraction guard (~$17.50)

ARTICLES = [
    # Technology (7)
    "Python_(programming_language)",
    "Artificial_intelligence",
    "ChatGPT",
    "IPhone",
    "Google",
    "Microsoft",
    "Tesla,_Inc.",
    # Sports (7)
    "NFL",
    "NBA",
    "Super_Bowl",
    "Premier_League",
    "LeBron_James",
    "UFC",
    "2023_MLB_season",
    # Entertainment (7)
    "Taylor_Swift",
    "Netflix",
    "YouTube",
    "Barbie_(film)",
    "Oppenheimer_(film)",
    "Beyonce",
    "Spotify",
    # Finance (7)
    "Bitcoin",
    "Stock_market",
    "Federal_Reserve",
    "Amazon_(company)",
    "Apple_Inc.",
    "Inflation",
    "S&P_500",
    # Health (7)
    "Mental_health",
    "Ozempic",
    "Cancer",
    "Diabetes",
    "Exercise",
    "Influenza",
    "Sleep",
]
