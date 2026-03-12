# 🚕 NYC Taxi Data Pipeline

An end-to-end data engineering portfolio project that ingests, transforms, and analyses NYC Yellow Taxi trip data using a modern data stack.

---

## 🏗️ Architecture

```
Raw Parquet Files (NYC TLC)
        │
        ▼
┌─────────────────┐
│   Bronze Layer  │  ← Raw .parquet files (3 months, ~3M rows)
│  data/bronze/   │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  Staging Layer  │  ← stg_taxi_trips (cleaned, renamed columns)
│   dbt views     │
└─────────────────┘
        │
        ▼
┌──────────────────────┐
│ Intermediate Layer   │  ← int_trips_enriched (payment desc, time buckets, tip %)
│     dbt views        │
└──────────────────────┘
        │
        ▼
┌─────────────────┐
│   Marts Layer   │  ← 3 analytical tables (revenue, tips, routes)
│   dbt tables    │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│    DuckDB       │  ← Local analytical database
│   taxi.duckdb   │
└─────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Data ingestion |
| dbt-duckdb | Data transformation |
| DuckDB | Local analytical database |
| Apache Airflow | Pipeline orchestration |
| Git + GitHub | Version control |

---

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── ingestion/
│   └── download_taxi_data.py     # Downloads parquet files from NYC TLC
├── data/
│   └── bronze/                   # Raw parquet files (gitignored)
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_taxi_trips.sql
│   │   │   └── schema.yml        # dbt tests
│   │   ├── intermediate/
│   │   │   └── int_trips_enriched.sql
│   │   └── marts/
│   │       ├── fct_revenue_by_zone.sql
│   │       ├── fct_tip_analysis.sql
│   │       └── fct_route_efficiency.sql
│   └── dbt_project.yml
├── airflow/
│   └── dags/
│       └── taxi_pipeline_dag.py  # Airflow DAG definition
└── README.md
```

---

## 📊 Business Questions Answered

### Q1: Which pickup zones generate the most revenue per hour?
**Model:** `fct_revenue_by_zone`
Aggregates total revenue, trip count and average fare by pickup zone and hour — helps identify peak revenue zones for fleet optimisation.

### Q2: How does tip percentage vary by payment type and time of day?
**Model:** `fct_tip_analysis`
Analyses tipping behaviour across credit card vs cash payments, broken down by morning/afternoon/evening/night — useful for driver earnings insights.

### Q3: Which routes are the most inefficient (slowest minutes per mile)?
**Model:** `fct_route_efficiency`
Identifies route combinations with the highest minutes-per-mile ratio — highlights congestion hotspots across NYC.

---

## 🔄 Pipeline Flow (Airflow DAG)

```
download_taxi_data → run_dbt_models → run_dbt_tests → log_completion
```

- Scheduled daily at 6:00 AM
- 2 retries on failure with 5-minute delay
- dbt tests run automatically after every transformation

---

## ✅ dbt Tests

All 6 data quality tests pass:

| Test | Column | Result |
|------|--------|--------|
| not_null | pickup_datetime | ✅ PASS |
| not_null | pickup_location_id | ✅ PASS |
| not_null | fare_amount | ✅ PASS |
| not_null | trip_distance | ✅ PASS |
| not_null | payment_type | ✅ PASS |
| accepted_values | payment_type (1-5) | ✅ PASS |

---

## 🚀 How to Run

### Prerequisites
- Python 3.11+
- dbt-duckdb

### Setup

```bash
# Clone the repo
git clone https://github.com/ashritvj-blip/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

# Create virtual environment
python3.11 -m venv dbt-env
source dbt-env/bin/activate

# Install dependencies
pip install dbt-duckdb duckdb requests

# Download data
python ingestion/download_taxi_data.py

# Run dbt models
cd dbt_project
dbt run

# Run dbt tests
dbt test
```

---

## 📈 Dataset

- **Source:** NYC Taxi & Limousine Commission (TLC)
- **Period:** January – March 2024
- **Size:** ~3 million rows across 3 Parquet files
- **Data:** Yellow taxi trip records including pickup/dropoff times, locations, fares, tips and payment types

---

## 👤 Author

**John** — Data Engineer
[GitHub](https://github.com/ashritvj-blip)
