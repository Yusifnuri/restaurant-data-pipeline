# Restaurant Data Pipeline

A modular, production‑style data pipeline designed for a restaurant business.  
The project follows a layered architecture (Bronze → Silver → Gold) and processes both CSV datasets and support ticket data retrieved from Azure Blob Storage.  
All stages are orchestrated with **Dagster**, enabling reproducible and maintainable workflows.

---

## 1. Project Structure

```
restaurant_pipeline/
│
├── data/
│   ├── bronze/        # Raw data (CSV + JSONL from Azure)
│   ├── silver/        # Cleaned staging data
│   └── gold/          # Final curated models
│
├── reports/
│   ├── tables/        # Analytics outputs (KPI tables)
│   └── tickets_per_order.csv
│
├── src/
│   ├── extract_tickets.py      # Azure Blob ingestion
│   ├── transform_silver.py     # Silver layer processing
│   ├── transform_gold.py       # Gold model creation
│   └── analytics_models.py     # KPI calculations
│
├── dagster_project/
│   └── etl_job.py              # Orchestration logic
│
├── .env.template               # Environment variables (sample)
├── .gitignore
└── README.md
```

---

## 2. How the Pipeline Works

### Bronze Layer
- Loads 6 raw CSV files.
- Downloads support tickets from Azure Blob Storage (JSONL).
- No transformations are applied.

### Silver Layer
- Cleans and standardizes all fields.
- Renames inconsistent columns.
- Converts timestamps and numeric fields.
- Produces `stg_*` tables.

### Gold Layer
- Combines orders, customers, items, stores, products, and ticket counts.
- Produces a fully curated model for analytics:  
  **`orders_mart.csv`**

### Analytics Layer
- Computes essential KPIs:
  - Average order value  
  - Number of support tickets per order
- Stores outputs in `reports/`.

---

## 3. Running the Full Pipeline

```
py -m dagster_project.etl_job
```

Dagster executes:
1. Extract Layer  
2. Silver Layer  
3. Gold Layer  
4. Analytics Layer

---

## 4. Environment Setup

Install dependencies:

```
pip install -r requirements.txt
```

Create your `.env`:

```
cp .env.template .env
```

Add your Azure Blob SAS container URL:

```
AZURE_BLOB_CONTAINER_URL="your_url_here"
```

---

## 5. Notes

- Sensitive credentials (e.g., `.env`) are excluded via `.gitignore`.
- Processed data is not committed to the repository.
- The project follows best practices for reproducible ETL workflows.
