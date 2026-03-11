# Data Card: Wikipedia Pageviews Ad Inventory Proxy

## Overview

| Property | Value |
|----------|-------|
| Dataset Name | Wikipedia Daily Pageviews (Ad Inventory Proxy) |
| Source | BigQuery Public Dataset: `bigquery-public-data.wikipedia.pageviews_*` |
| Date Range | 2023-01-01 to 2024-12-31 (731 days) |
| Granularity | Daily aggregation from hourly source |
| Total Records | 24,854 rows |
| Ad Units (Articles) | 35 extracted, 34 forecasted (2023_MLB_season excluded due to low traffic) |
| Verticals | 5 (Technology, Sports, Entertainment, Finance, Health) |

---

## Dataset Purpose

This dataset serves as a **proxy for ad inventory impressions** in a simulated
publisher environment. Wikipedia pageviews exhibit statistical properties
analogous to real ad server traffic:

- Day-of-week seasonality (weekday vs. weekend patterns)
- Annual seasonality (holiday effects, seasonal events)
- Trend components (growth/decay over time)
- Device segmentation (desktop vs. mobile)
- Event-driven anomalies (product launches, sports events, breaking news)

**Intended Use**: Training and evaluating time series forecasting models
(TimesFM 2.5, ARIMA+, ARIMA+ XREG) for ad inventory prediction.

---

## Article Selection

### Selection Criteria

1. **Traffic volume**: Minimum ~1,000 daily pageviews to ensure signal over noise
2. **Seasonal patterns**: Articles with known weekly/monthly/annual cycles preferred
3. **Vertical diversity**: 6-7 articles per vertical for balanced coverage
4. **Data quality**: Exclude articles with known anomalies (redirects, title changes, bot traffic)

### Technology (7 articles)

| Article | Rationale | Traffic Tier | Quality Notes |
|---------|-----------|--------------|---------------|
| Python_(programming_language) | Evergreen developer interest, weekday spikes | High | Clean |
| Artificial_intelligence | Sustained growth through 2023, ChatGPT halo effect | High | Clean |
| ChatGPT | Breakout 2023 story, demonstrates trend capture | Very High | New article - limited pre-2023 history |
| IPhone | Consistent traffic, product launch spikes (Sept) | Very High | Clean |
| Google | Major tech company, steady baseline | Very High | Clean |
| Microsoft | Major tech company, AI news driver in 2023 | High | Clean |
| Tesla,_Inc. | Tech/auto crossover, volatile but high volume | Very High | Elon news creates spikes |

### Sports (6 articles)

| Article | Rationale | Traffic Tier | Quality Notes |
|---------|-----------|--------------|---------------|
| NFL | US sport, strong Sunday/Monday spikes, Sept-Feb season | Very High | Clean, clear seasonality |
| NBA | US sport, Oct-June season, playoff spikes | Very High | Clean |
| Super_Bowl | Massive single-day event (Feb), tests anomaly detection | High | Extreme spike pattern |
| Premier_League | International football, year-round with summer break | High | Clean |
| LeBron_James | Individual athlete, consistent baseline + game spikes | High | Clean |
| UFC | Growing combat sport, PPV event spikes | Medium | Clean |
| ~~2023_MLB_season~~ | ~~Baseball season coverage~~ | ~~Medium~~ | Excluded: insufficient traffic for forecasting |

### Entertainment (7 articles)

| Article | Rationale | Traffic Tier | Quality Notes |
|---------|-----------|--------------|---------------|
| Taylor_Swift | Massive 2023 presence (Eras Tour), cultural phenomenon | Very High | Clean |
| Netflix | Streaming platform, show release spikes | Very High | Clean |
| YouTube | Video platform, extremely consistent traffic | Very High | Clean |
| Barbie_(film) | Summer 2023 blockbuster, tests movie release patterns | High | Spike concentrated July-Aug |
| Oppenheimer_(film) | Summer 2023 blockbuster, "Barbenheimer" phenomenon | High | Spike concentrated July-Aug |
| Beyonce | Renaissance World Tour 2023 | High | Clean |
| Spotify | Music streaming, year-end Wrapped spikes | High | December spike pattern |

### Finance (7 articles)

| Article | Rationale | Traffic Tier | Quality Notes |
|---------|-----------|--------------|---------------|
| Bitcoin | Cryptocurrency, highly volatile, weekday trading pattern | Very High | High variance is feature not bug |
| Stock_market | General finance interest, market hours correlation | High | Clean |
| Federal_Reserve | Economic policy, FOMC meeting spikes | Medium | Event-driven spikes |
| Amazon_(company) | E-commerce giant, Prime Day + holiday spikes | Very High | Clean |
| Apple_Inc. | Tech/finance crossover, product + earnings cycles | Very High | Clean |
| Inflation | 2023 economic concern, news-driven | Medium | Policy-dependent |
| S&P_500 | Market index reference, trading day patterns | Medium | Clean |

### Health (7 articles)

| Article | Rationale | Traffic Tier | Quality Notes |
|---------|-----------|--------------|---------------|
| Mental_health | Growing awareness, steady interest | High | Clean |
| Ozempic | Breakout 2023 health story (weight loss drug) | High | Rapid growth curve |
| Cancer | Consistent medical reference traffic | Very High | Clean |
| Diabetes | Chronic condition, steady baseline | High | Clean |
| Exercise | General wellness, January spike (resolutions) | Medium | New Year pattern |
| Influenza | Seasonal illness, Oct-Mar spike pattern | Medium | Clear seasonality |
| Sleep | Health/wellness topic, steady interest | Medium | Clean |

---

## Data Schema

### raw_pageviews

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Calendar date (partition key) |
| wiki | STRING | Always `en` after aggregation (device split captured in impression columns) |
| ad_unit | STRING | Article title (cluster key) |
| daily_impressions | INT64 | Total pageviews for date |
| desktop_impressions | INT64 | Pageviews from `en` (desktop) wiki |
| mobile_impressions | INT64 | Pageviews from `en.m` (mobile) wiki |

### daily_impressions (feature-enriched)

Extends raw_pageviews with:

| Column | Type | Description |
|--------|------|-------------|
| day_of_week | INT64 | 1 (Sunday) to 7 (Saturday) |
| is_weekend | BOOL | Saturday or Sunday |
| quarter | INT64 | 1-4 |
| week_of_year | INT64 | 1-53 |
| is_holiday | BOOL | US holiday indicator |
| holiday_name | STRING | Holiday name (nullable) |
| days_to_next_holiday | INT64 | Countdown to next US holiday |

---

## Data Collection Process

1. **Source Query**: UNION of `pageviews_2023` and `pageviews_2024` tables
2. **Wiki Filter**: `en` (desktop) and `en.m` (mobile) editions only
3. **Article Filter**: 34 pre-selected articles across 5 verticals
4. **Aggregation**: SUM(views) per date x article
5. **Partition Pruning**: Date range filter applied at query time

**Extraction Cost**: ~$6.23 (partition pruning reduced from $16 estimate)

---

## Known Limitations

### Source Data Gaps

| Gap Type | Affected Records | Cause | Mitigation |
|----------|------------------|-------|------------|
| 2024-02-18 | 34 rows (all articles) | Wikipedia source outage | Single-day gap; models handle gracefully |

### Excluded Articles

| Article | Reason |
|---------|--------|
| 2023_MLB_season | Extremely low traffic (mean 16 impressions/day); insufficient signal for forecasting |

### External Regressors: Google Trends (Scoped Out)

The BigQuery public Google Trends dataset was evaluated but **not included**:

| Dataset | Assessment |
|---------|------------|
| `international_top_terms` | 0 rows for US country_code |
| `top_terms` | 1/34 article matches (only "inflation") |

**Reason**: Google Trends BQ captures "top trending" breakout events, not persistent
topic interest. Our 34 Wikipedia articles are evergreen topics that don't appear in
trending search rankings. Even fuzzy matches like "bitcoin price" or "super bowl
commercials" are event-specific queries rather than the base topic signal we need.

**Alternative**: Calendar features (`day_of_week`, `is_weekend`, `is_holiday`,
`days_to_next_holiday`) provide the cyclical seasonality signal that ARIMA+ XREG
external regressors require. These are deterministic and always available at
forecast time—a cleaner approach than sparse, misaligned Trends data.

### Proxy Limitations

This dataset is a **simulation**, not real ad server data:

- No actual ad serving decisions or fill rates
- No targeting dimensions beyond article (geography, demographics)
- No auction dynamics or bid landscape
- No advertiser demand signals

### Article-Specific Considerations

| Article | Limitation |
|---------|------------|
| ChatGPT | Limited pre-2023 history (new article) |
| Barbie_(film), Oppenheimer_(film) | Spike-dominated; limited baseline signal |
| 2023_MLB_season | Year-specific; sparse late-2024 data |
| Tesla,_Inc. | High variance from Elon Musk news cycles |

---

## Excluded Data

The following articles were explicitly excluded:

| Article | Reason |
|---------|--------|
| COVID-19_pandemic | 2020-2021 spike dominates, distorts seasonality |
| Deaths_in_2023 | Unpredictable celebrity death spikes |
| Russia-Ukraine articles | Geopolitical volatility |
| Elon_Musk | Avoids over-indexing on single individual |
| United_States | Too generic, not advertiser-targetable |
| Christmas | Only 6 weeks of relevance per year |

---

## Distribution Analysis

*Validated 2026-03-08 (data foundation checkpoint)*

### Traffic Imbalance

**Traffic Range**: 2023_MLB_season (11,520 total) to YouTube (105,524,546 total)
**Max/Min Ratio**: 9,160x

Ad units fall into approximate tiers for evaluation stratification:

- **Very High Traffic** (~12 articles): YouTube, Google, Apple_Inc., etc.
- **High Traffic** (~15 articles): Taylor_Swift, NFL, Bitcoin, etc.
- **Medium Traffic** (~8 articles): UFC, Ozempic, Federal_Reserve, etc.

**Implication**: Aggregate metrics (MAPE, RMSE) will be dominated by high-traffic units.
Per-ad-unit evaluation or traffic-tier stratification is recommended.

### Per-Ad-Unit Statistics

| Ad Unit | Days | Mean | Median | Std | Min | Max |
|---------|------|------|--------|-----|-----|-----|
| YouTube | 730 | 144,554 | 118,231 | 147,548 | 35,097 | 3,533,034 |
| ChatGPT | 730 | 97,475 | 65,709 | 78,297 | 12,612 | 386,624 |
| Oppenheimer_(film) | 730 | 56,463 | 20,730 | 127,722 | 7,520 | 1,362,179 |
| Taylor_Swift | 730 | 54,937 | 44,597 | 45,344 | 18,447 | 713,558 |
| Premier_League | 730 | 40,714 | 23,918 | 44,699 | 6,833 | 265,312 |
| Barbie_(film) | 730 | 32,539 | 10,510 | 81,194 | 3,654 | 833,305 |
| Google | 730 | 27,080 | 25,192 | 11,826 | 13,514 | 208,148 |
| LeBron_James | 730 | 25,420 | 19,990 | 24,520 | 8,851 | 452,661 |
| Artificial_intelligence | 730 | 15,653 | 15,506 | 4,880 | 6,537 | 28,437 |
| Netflix | 730 | 14,941 | 14,243 | 4,482 | 7,689 | 39,444 |
| Amazon_(company) | 730 | 12,432 | 11,999 | 2,488 | 7,115 | 23,309 |
| Apple_Inc. | 730 | 12,217 | 11,965 | 4,591 | 5,972 | 70,447 |
| Super_Bowl | 730 | 9,721 | 3,866 | 39,480 | 1,917 | 789,800 |
| Microsoft | 730 | 8,775 | 7,701 | 19,689 | 5,202 | 537,426 |
| Tesla,_Inc. | 730 | 7,826 | 7,582 | 1,851 | 4,644 | 19,463 |
| Bitcoin | 730 | 6,463 | 5,426 | 3,096 | 3,558 | 38,829 |
| Python_(programming_language) | 730 | 6,316 | 5,953 | 2,071 | 3,180 | 22,789 |
| Spotify | 730 | 5,773 | 5,315 | 1,569 | 3,529 | 12,883 |
| IPhone | 730 | 5,625 | 5,375 | 1,456 | 3,935 | 21,567 |
| S&P_500 | 730 | 3,395 | 3,239 | 1,104 | 1,526 | 15,230 |
| Federal_Reserve | 730 | 2,271 | 2,004 | 1,183 | 1,341 | 20,895 |
| Cancer | 730 | 2,247 | 2,198 | 353 | 1,595 | 5,665 |
| Diabetes | 730 | 1,980 | 1,955 | 320 | 1,424 | 6,590 |
| Inflation | 730 | 1,707 | 1,638 | 431 | 909 | 3,195 |
| Stock_market | 730 | 1,602 | 1,608 | 436 | 808 | 3,016 |
| Influenza | 730 | 1,409 | 1,323 | 388 | 765 | 3,371 |
| Sleep | 730 | 1,233 | 1,204 | 217 | 828 | 2,262 |
| Exercise | 730 | 1,066 | 1,060 | 303 | 480 | 1,914 |
| Mental_health | 730 | 1,039 | 888 | 1,564 | 469 | 36,894 |
| Beyonce | 730 | 342 | 185 | 556 | 95 | 6,955 |
| NBA | 730 | 248 | 186 | 152 | 103 | 2,153 |
| UFC | 730 | 178 | 144 | 215 | 80 | 4,617 |
| NFL | 730 | 177 | 164 | 56 | 88 | 557 |
| Ozempic | 730 | 138 | 109 | 132 | 34 | 1,429 |
| 2023_MLB_season | 724 | 16 | 11 | 22 | 1 | 375 |

### Device Distribution

**Validation**: `desktop_impressions + mobile_impressions = daily_impressions`
for all records (0 discrepancies).

Desktop and mobile splits vary by article category:

- Technology articles: Higher desktop share (developer audience)
- Entertainment articles: Higher mobile share (casual browsing)
- Finance articles: Mixed (professional + retail investors)

| Ad Unit | Total | Desktop | Mobile | Desktop % |
|---------|-------|---------|--------|-----------|
| Python_(programming_language) | 4,610,589 | 3,209,817 | 1,400,772 | 69.6% |
| Amazon_(company) | 9,075,589 | 6,185,654 | 2,889,935 | 68.2% |
| 2023_MLB_season | 11,520 | 7,315 | 4,205 | 63.5% |
| Netflix | 10,907,251 | 6,599,692 | 4,307,559 | 60.5% |
| Google | 19,768,427 | 11,801,089 | 7,967,338 | 59.7% |
| S&P_500 | 2,478,655 | 1,424,164 | 1,054,491 | 57.5% |
| Ozempic | 100,488 | 57,679 | 42,809 | 57.4% |
| NBA | 180,732 | 99,740 | 80,992 | 55.2% |
| Spotify | 4,214,651 | 2,318,588 | 1,896,063 | 55.0% |
| Microsoft | 6,405,703 | 3,343,501 | 3,062,202 | 52.2% |
| ChatGPT | 71,156,814 | 36,582,234 | 34,574,580 | 51.4% |
| Mental_health | 758,407 | 380,733 | 377,674 | 50.2% |
| Bitcoin | 4,717,855 | 2,353,444 | 2,364,411 | 49.9% |
| Artificial_intelligence | 11,426,525 | 5,321,779 | 6,104,746 | 46.6% |
| Beyonce | 249,666 | 113,411 | 136,255 | 45.4% |
| Tesla,_Inc. | 5,712,649 | 2,444,236 | 3,268,413 | 42.8% |
| NFL | 129,142 | 54,576 | 74,566 | 42.3% |
| YouTube | 105,524,546 | 43,550,889 | 61,973,657 | 41.3% |
| Apple_Inc. | 8,918,558 | 3,691,279 | 5,227,279 | 41.4% |
| Federal_Reserve | 1,657,839 | 661,887 | 995,952 | 39.9% |
| Inflation | 1,246,051 | 466,456 | 779,595 | 37.4% |
| IPhone | 4,106,476 | 1,523,926 | 2,582,550 | 37.1% |
| Influenza | 1,028,208 | 381,870 | 646,338 | 37.1% |
| Diabetes | 1,445,041 | 536,700 | 908,341 | 37.1% |
| Cancer | 1,640,070 | 569,239 | 1,070,831 | 34.7% |
| Sleep | 899,989 | 310,472 | 589,517 | 34.5% |
| Exercise | 778,405 | 245,218 | 533,187 | 31.5% |
| Stock_market | 1,169,483 | 355,242 | 814,241 | 30.4% |
| Oppenheimer_(film) | 41,217,873 | 11,440,887 | 29,776,986 | 27.8% |
| Barbie_(film) | 23,753,617 | 6,556,977 | 17,196,640 | 27.6% |
| Super_Bowl | 7,096,048 | 1,839,164 | 5,256,884 | 25.9% |
| UFC | 130,264 | 32,016 | 98,248 | 24.6% |
| Premier_League | 29,721,299 | 6,892,328 | 22,828,971 | 23.2% |
| Taylor_Swift | 40,103,773 | 9,247,349 | 30,856,424 | 23.1% |
| LeBron_James | 18,556,493 | 4,216,088 | 14,340,405 | 22.7% |

---

## Holiday and Event Coverage

### US Holidays Included (83 total, 2022-2025)

- Federal holidays (New Year, MLK Day, Presidents Day, Memorial Day, Independence Day, Labor Day, Columbus Day, Veterans Day, Thanksgiving, Christmas)
- Commercial holidays (Easter, Mother's Day, Father's Day, Halloween, Black Friday, Cyber Monday)
- Covered years: 2022-2025 (2022 for look-back features, 2025 for forecast horizon)
- Note: Father's Day 2022 coincided with Juneteenth (June 19), counted once

### Known High-Impact Events

Key events that create legitimate traffic spikes (not outliers to clean).
See `config/events.py` for the full programmatic event registry.

| Date | Event | Affected Articles |
|------|-------|-------------------|
| 2023-02-12 | Super Bowl LVII (Chiefs vs Eagles) | NFL, Super_Bowl |
| 2023-03-14 | GPT-4 release | ChatGPT |
| 2023-03-17 | Eras Tour kickoff | Taylor_Swift |
| 2023-06-01 | Celebrity weight loss coverage peaks | Ozempic |
| 2023-07-21 | Barbenheimer release | Barbie_(film), Oppenheimer_(film) |
| 2023-09-07 | NFL 2023 season kickoff | NFL |
| 2023-11-06 | GPT-4 Turbo announcement | ChatGPT |
| 2024-01-10 | Bitcoin ETF approval | Bitcoin |
| 2024-02-04 | Taylor Swift at Super Bowl | Taylor_Swift, Super_Bowl |
| 2024-02-11 | Super Bowl LVIII (Chiefs vs 49ers) | NFL, Super_Bowl |
| 2024-03-14 | Bitcoin all-time high $73K | Bitcoin |
| 2024-04-19 | Bitcoin halving | Bitcoin |
| 2024-09-05 | NFL 2024 season kickoff | NFL |

---

## Ethical Considerations

- **No PII**: Wikipedia pageviews are aggregated counts with no individual tracking
- **Public Data**: Source dataset is freely available via BigQuery Public Datasets
- **Proxy Transparency**: This dataset simulates ad inventory but does not represent
  actual advertising decisions, revenue, or user behavior

---

## Versioning

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-03-08 | Initial 2-year extraction (2023-2024) |
| 1.1 | 2026-03-08 | Merged article selection rationale |
| 1.2 | 2026-03-08 | Absorbed validation report (per-ad-unit stats, device mix, events) |
| 1.3 | 2026-03-08 | Consistency audit fixes (holiday count 62, cross-references) |
| 1.4 | 2026-03-09 | Cohesion audit: article count (34), holiday count (83), MAPE precision, date formats |
| 1.5 | 2026-03-10 | Display name consistency: ARIMA_PLUS -> ARIMA+ across all docs |
| 1.6 | 2026-03-10 | CV threshold alignment: unified to two-tier (0.5) across docs and UI |
| 1.7 | 2026-03-10 | Absorbed model_findings.md into README.md; removed duplicate doc |
| 1.8 | 2026-03-10 | Clarify article count: 35 extracted, 34 forecasted (MLB excluded) |

---

## Contact

**Author**: Victoria Alabi
**Project**: Ad Inventory Demand Forecasting System
**Repository**: github.com/vxa8502/ad-inventory-forecast
