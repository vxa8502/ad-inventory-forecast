"""Canonical event definitions for ad units.

These dates have legitimate traffic anomalies that models should capture,
not treat as outliers to be cleaned.
"""

KNOWN_EVENTS: dict[str, list[tuple[str, str]]] = {
    "Taylor_Swift": [
        ("2023-03-17", "Eras Tour kickoff"),
        ("2023-07-22", "Eras Tour film announcement"),
        ("2024-02-04", "Super Bowl LVIII attendance"),
    ],
    "NFL": [
        ("2023-02-12", "Super Bowl LVII (Chiefs vs Eagles)"),
        ("2023-09-07", "2023 season kickoff"),
        ("2024-02-11", "Super Bowl LVIII (Chiefs vs 49ers)"),
        ("2024-09-05", "2024 season kickoff"),
    ],
    "Bitcoin": [
        ("2024-01-10", "Bitcoin ETF approval"),
        ("2024-03-14", "All-time high $73K"),
        ("2024-04-19", "Bitcoin halving"),
    ],
    "Super_Bowl": [
        ("2023-02-12", "Super Bowl LVII"),
        ("2024-02-11", "Super Bowl LVIII"),
    ],
    "Barbie_(film)": [
        ("2023-07-21", "Theatrical release"),
        ("2023-07-22", "Opening weekend peak"),
    ],
    "Oppenheimer_(film)": [
        ("2023-07-21", "Theatrical release (Barbenheimer)"),
    ],
    "ChatGPT": [
        ("2023-03-14", "GPT-4 release"),
        ("2023-11-06", "GPT-4 Turbo announcement"),
    ],
    "Ozempic": [
        ("2023-06-01", "Celebrity weight loss coverage peaks"),
    ],
    "Influenza": [
        ("2023-01-15", "Winter peak 2023"),
        ("2024-01-15", "Winter peak 2024"),
    ],
}

SAMPLE_AD_UNITS: list[str] = [
    "Taylor_Swift",
    "NFL",
    "Bitcoin",
    "Influenza",
    "ChatGPT",
]
