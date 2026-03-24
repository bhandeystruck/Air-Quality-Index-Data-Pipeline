# Open-Meteo Air Quality ETL Pipeline

## Project Overview
This project implements a robust, end-to-end ELT (Extract, Load, Transform) pipeline designed to ingest, process, and visualize air quality data from the Open-Meteo API. The architecture follows the Medallion (Bronze/Silver/Gold) design pattern, leveraging a local Data Lakehouse approach.

## Architecture
The pipeline is structured into three distinct layers of data maturity:
1. **Bronze (Raw):** Ingestion of nested JSON responses from the Open-Meteo API into MinIO S3-compatible storage.
2. **Silver (Staging):** Flattening and cleansing of raw JSON into relational tables using dbt and DuckDB.
3. **Gold (Analytics):** Aggregated metrics and health-index calculations optimized for BI consumption.

## Technology Stack
* **Orchestration:** Apache Airflow (Containerized)
* **Ingestion:** Python (Boto3, Requests, Tenacity)
* **Storage (Object Store):** MinIO (S3-Compatible)
* **Transformation:** dbt (Data Build Tool)
* **Database Engine:** DuckDB
* **Infrastructure:** Docker & Docker Compose
* **Environment Management:** Python venv, python-dotenv

---

## Key Engineering Features
* **Fault-Tolerant Ingestion:** Implementation of exponential backoff retries using the Tenacity library to handle transient API and network failures.
* **Environment Isolation:** Complete separation of configuration and secrets from source code using environment variables and `.env` files.
* **Data Quality Gating:** Pre-load validation logic that inspects API payloads for schema integrity and record volume before committing to storage.
* **Timezone Standardization:** Utilization of timezone-aware UTC objects to ensure temporal consistency across the pipeline.
* **Containerized Infrastructure:** Orchestration of Postgres, Airflow, and MinIO services through a modular Docker Compose configuration.

---