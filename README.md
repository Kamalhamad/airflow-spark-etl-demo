# Airflow + Spark ETL Demo (Portfolio)

A small, **project-based** demo showing **Apache Airflow orchestrating a PySpark ETL job** end-to-end.
Designed to be **recruiter-friendly**: reproducible, quick to run, and easy to explain.

## Proof (successful run)
![Airflow DAG run (all tasks successful)](docs/screenshots/airflow_graph_success.png)
![Parquet output folder](docs/screenshots/parquet_output_folder.png)

## What it does
1. Generates a small raw dataset (CSV) via an Airflow task
2. Runs a **PySpark** ETL job via `spark-submit` (Spark local mode)
3. Runs a simple **data quality check** (row count + expected columns)
4. Writes curated output as **Parquet**

## Skills demonstrated (why this matters)
- Airflow DAG orchestration: dependencies, retries, logs
- Spark / PySpark batch processing and transformations
- Containerized, reproducible local environment (Docker Compose)
- Basic data quality validation on outputs

## Quick start (Docker)
**Prerequisites:** Docker + Docker Compose

```bash
docker compose up -d --build
