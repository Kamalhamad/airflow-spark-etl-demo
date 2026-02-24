from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import pandas as pd

BASE_DIR = "/opt/airflow"
RAW_PATH = f"{BASE_DIR}/data/raw/customers.csv"
OUT_PATH = f"{BASE_DIR}/data/curated/customers_parquet"
JOB_PATH = f"{BASE_DIR}/jobs/spark_etl_job.py"

def dq_check():
    if not os.path.exists(OUT_PATH):
        raise FileNotFoundError(f"Output path not found: {OUT_PATH}")

    df = pd.read_parquet(OUT_PATH)
    if df.shape[0] == 0:
        raise ValueError("DQ check failed: output has 0 rows")

    required_cols = {"customer_id", "country", "signup_date", "is_active"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"DQ check failed: missing columns {missing}")

default_args = {
    "owner": "kamal",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spark_airflow_etl_demo",
    default_args=default_args,
    description="Airflow orchestrates a PySpark ETL job and validates the Parquet output",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "pyspark", "airflow", "portfolio"],
) as dag:

    generate_raw = BashOperator(
        task_id="generate_raw_data",
        bash_command=f"python {BASE_DIR}/jobs/generate_raw_data.py --out {RAW_PATH}",
    )

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command=f"/opt/spark/bin/spark-submit --master local[*] {JOB_PATH} --in {RAW_PATH} --out {OUT_PATH}",
    )

    validate = PythonOperator(
        task_id="data_quality_check",
        python_callable=dq_check,
    )

    generate_raw >> run_spark >> validate
