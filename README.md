# steam_player_retention_analysis

This project analyzes and predicts player retention patterns for Steam games using a full data pipeline built with **Python, SQL, and Tableau**. The goal is to understand how player engagement evolves over time and identify game characteristics associated with stronger long-term retention.

The project collects player statistics from SteamCharts, enriches them with Steam Store metadata, builds a time-series panel dataset, and trains machine learning models to forecast future player counts.

---

# Project Overview

Steam games often experience rapid changes in player activity after release. While some titles maintain strong communities, many decline quickly.

This project attempts to answer:

* How does player activity typically evolve during a game's lifecycle?
* Which game characteristics are associated with stronger retention?
* Can we forecast future player engagement using historical data?

The analysis focuses on games with **at least 12 months of player data** to capture a full yearly cycle of activity.

---

# Data Sources

### SteamCharts

Provides monthly player statistics for each game.

Collected metrics include:

* Monthly average players
* Monthly peak players
* Player history over the past 12 months

### Steam Store API

Provides game metadata such as:

* Price
* Free-to-play status
* Genres
* Categories
* Release date

---

# Project Architecture

The project is organized as a multi-stage data pipeline.

```
SteamCharts + Steam Store API
            │
            ▼
Step01  Collect candidate Steam app IDs
            │
Step02  Scrape SteamCharts monthly player metrics
            │
Step02b Fetch Steam Store metadata
            │
Step03  Build panel dataset for ML modeling
            │
Step04  Train forecasting models
            │
SQL warehouse
            │
Tableau dashboards
```

---

# Technology Stack

Python
Pandas
NumPy
Scikit-learn
SQL Server / T-SQL
Tableau
Parquet data storage

---

# Data Pipeline

## Step 1 – Collect App IDs

Candidate Steam game IDs are collected and stored for processing.

```
01_collect_appids.py
```

Output

```
data/processed/candidate_appids.csv
```

---

## Step 2 – Scrape Player Metrics

SteamCharts pages are scraped to collect monthly player statistics.

```
02_pull_steamcharts_metrics.py
```

Metrics extracted:

* monthly average players
* monthly peak players
* last 12 months of activity

Data is stored as **partitioned parquet files** for efficient processing.

---

## Step 2b – Fetch Store Metadata

The Steam Store API is used to retrieve metadata for each game.

```
02b_pull_store_meta.py
```

Collected attributes:

* price
* free-to-play status
* genres
* categories
* release date

The scraper includes:

* retry logic
* caching
* resume capability
* failure logging

Output

```
data/processed/steam_store_meta.parquet
```

---

## Step 3 – Build Panel Dataset

Monthly player history is converted into a **panel dataset** suitable for machine learning.

```
03_build_panel_h3.py
```

Each row represents

```
Game at time k → predict player count at k + 6 months
```

### Engineered Features

Lifecycle features

* game age
* months since release

Trend features

* 3-month player growth
* momentum

Volatility features

* short-term player fluctuations

Engagement features

* peak vs average player ratios
* player spikes

Game attributes

* genres (multi-hot encoded)
* categories
* price
* free-to-play flag

Output dataset

```
data/processed/model_panel_h3.parquet
```

---

# Machine Learning Model

The model forecasts **future average player counts six months ahead**.

Instead of predicting raw player counts directly, the model predicts the change relative to the current level:

```
delta = log(players_future) − log(players_current)
```

This approach stabilizes the prediction problem across games with very different player bases.

---

## Model Type

Gradient Boosting Regressor with quantile loss.

The model predicts three quantiles:

```
P10  pessimistic forecast
P50  median forecast
P90  optimistic forecast
```

This provides an uncertainty range for player forecasts.

---

## Training Strategy

Data is split using **group-based splitting by game ID** to prevent data leakage.

Baseline comparison:

```
Persistence model
future_players = current_players
```

The ML model must outperform this baseline to demonstrate predictive value.

---

# Model Performance

The model forecasts player activity **six months into the future** using a quantile regression approach.
Predictions are evaluated against a **persistence baseline**, where future player counts are assumed to remain equal to current levels.

### Dataset

```
Training rows: 4,188
Test rows:     1,048
Features:      24
```

The dataset contains panel observations across multiple games and time points.
A **group-based split by game ID** is used to prevent data leakage between training and testing sets.

---

# Forecast Accuracy

Model performance is evaluated in log space using RMSE.

```
Persistence RMSE (log): 0.5532
Forecast RMSE (q50):    0.5018
Improvement:            +0.0514
```

The quantile regression model improves forecasting accuracy compared to the persistence baseline.

---

# Quantile Calibration

The model predicts three quantiles to represent uncertainty in player forecasts.

```
P10–P90 coverage: 0.734
Target coverage:  0.80
```

Additional calibration checks:

```
P(y < P10): 0.162   (target ≈ 0.10)
P(y < P50): 0.550   (target ≈ 0.50)
```

These results indicate reasonable calibration for median predictions while slightly underestimating lower-tail risk.

---

# SQL Data Warehouse

The project includes a small analytical data warehouse built using **SQL Server and a star schema design**.

The warehouse separates raw ingestion, core transformations, and analytics-ready tables to support downstream analysis and visualization.

### Schema Layers

```
stg   staging layer (raw ingested data)
dim   dimension tables
fact  player activity metrics
mart  analytics tables
```

### Star Schema Design

A **small star schema** is used to organize the analytical data model.

Fact table:

```
fact_player_metrics
```

Dimension tables:

```
dim_app
dim_genre
dim_category
dim_price_tier
```

The fact table stores player activity metrics, while dimension tables contain descriptive attributes about each game.

This structure allows efficient analytical queries such as:

* retention by genre
* retention by category
* retention by price range

### Example Relationships

```
              dim_genre
                  │
                  │
dim_category ── fact_player_metrics ── dim_app
                  │
                  │
            dim_price_tier
```

Genres and categories are normalized from the Steam Store metadata using SQL transformations (e.g., splitting multi-value fields and mapping them into dimension tables).

This warehouse structure supports both **Tableau dashboards and downstream analysis queries**.

---

# Tableau Dashboard

Two dashboards summarize the analysis.

## Player Lifecycle

Shows average player activity across a 12-month timeline.

Insights

* Player counts often spike around major Steam sales
* Most games show declining engagement over time
* The market distribution is highly skewed toward smaller games

---

## Retention Rankings

Retention is compared across game characteristics.

Dimensions analyzed

* Genres
* Platform categories
* Price ranges

Example insight

Low-priced games ($0–10) tend to maintain player engagement more effectively than completely free games.

---

# Key Findings

Most games experience declining player activity after release.

Player activity often increases during major Steam sales events.

The distribution of player counts is highly skewed, with a small number of titles dominating total player activity.

Lower-priced games tend to show stronger retention patterns than free titles in this dataset.

---

# Repository Structure

```
project_root
│
├─ src
│  ├─ 01_collect_appids.py
│  ├─ 02_pull_steamcharts_metrics.py
│  ├─ 02b_pull_store_meta.py
│  ├─ 03_build_panel_h3.py
│  ├─ 04_model.py
│  └─ config.py
│
├─ sql
│  ├─ 00_create_database.sql
│  ├─ 01_create_schemas_and_tables.sql
│  ├─ 02_load_staging.sql
│  ├─ 03_transform_core.sql
│  └─ 04_create_marts.sql
│
├─ tableau
│  └─ dashboard.twb
│
├─ data
│  ├─ raw
│  └─ processed
│
└─ README.md
```

---


# License

This project is intended for educational and portfolio purposes.

SteamCharts and Steam Store data belong to their respective platforms.

