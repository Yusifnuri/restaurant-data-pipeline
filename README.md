# Restaurant Data Engineering Pipeline

This project implements a complete end-to-end ETL/ELT data pipeline for a restaurant business. 
The pipeline ingests multiple data sources, applies a layered transformation approach 
(Bronze → Silver → Gold), and produces analytics-ready datasets and KPI reports.

The project is built for learning, portfolio demonstration, and real-world data engineering practice.

---

## Features

- Ingestion of six CSV files (orders, customers, items, products, stores, supplies)
- Ingestion of support tickets from Azure Blob Storage (JSONL) using a SAS link
- Layered data processing:
  - Bronze: raw data
  - Silver: cleaned, standardized, schema-aligned models
  - Gold: business-level marts for analytics
- Final analytics outputs:
  - Average order value
  - Number of tickets per order
- Fully orchestrated using Dagster

---

## Project Structure

```
restaurant_pipeline/
│
├── data/
│   ├── bronze/      # raw inputs (CSV + JSONL)
│   ├── silver/      # cleaned staging models
│   └── gold/        # final marts
│
├── src/
│   ├── transform_silver.py
│   ├── transform_gold.py
│   ├── extract_tickets.py
│   └── analytics_models.py
│
└── dagster_project/
    └── etl_job.py   # Orchestration pipeline
```

---

## Pipeline Flow

1. Extract:
   - Load CSV files
   - Download support tickets from Azure Blob via SAS URL

2. Silver (Staging):
   - Clean columns
   - Standardize schemas
   - Normalize relationships

3. Gold (Marts):
   - Join customers, orders, stores, items, tickets
   - Calculate computed totals and number of tickets
   - Produce orders_mart.csv

4. Analytics:
   - Generate high-level metrics as CSV reports

5. Dagster Orchestration:
   - Automates all steps end-to-end

---

## How to Run the Pipeline

### 1. Install requirements
```
pip install -r requirements.txt
```

### 2. Run Dagster ETL Pipeline
```
py -m dagster_project.etl_job
```

This will automatically:

- extract data
- transform Bronze → Silver → Gold
- generate analytics reports

Final outputs appear in:
```
data/gold/orders_mart.csv
data/reports/
```

---

## Notes

- The Azure Blob SAS link must be valid.  
  If expired, update the link inside:  
  `src/extract_tickets.py`

- Pipeline steps are modular and can be run independently for debugging.

---

## Technologies Used

- Python (Pandas)
- Dagster (orchestration)
- Azure Blob Storage (external raw data)
- Layered Data Architecture (bronze/silver/gold modeling)

---

## Purpose

This project was built to demonstrate:

- practical ETL/ELT experience
- data modeling skills
- orchestration with Dagster
- production-like data engineering patterns

It is suitable for showcasing in CV, LinkedIn, and portfolio.
