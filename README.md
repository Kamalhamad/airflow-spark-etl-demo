# Airflow + Spark Mini Project (Portfolio Demo)

This is a small, **project-based** demo showing **Apache Airflow orchestrating a PySpark ETL job**.
It is designed to be recruiter-friendly: simple, reproducible, and easy to explain in an interview.

## What it does
1. Generates a small raw dataset (CSV) as an Airflow task
2. Runs a **Spark job** (PySpark) via `spark-submit` (Spark local mode)
3. Runs a simple data quality check (row count + schema expectations)
4. Writes output as Parquet

## Quick start (Docker)
Prereqs: Docker + Docker Compose

```bash
docker compose up -d --build
```

Open Airflow UI:
- http://localhost:8080
- user: `airflow`
- pass: `airflow`

Enable and trigger the DAG: `spark_airflow_etl_demo`

## Notes
- This uses a custom Airflow image that includes **OpenJDK** and **Apache Spark** so `spark-submit` works.
- Spark runs in **local mode** (`local[*]`) which keeps the demo fast and easy.

## How to explain in an interview (30 seconds)
“I built an Airflow DAG that orchestrates a PySpark ETL job end-to-end: extract/generate, transform with Spark, write Parquet, and validate output. The goal was to demonstrate orchestration patterns (dependencies, retries, logging) and Spark batch processing in a reproducible containerized setup.”
