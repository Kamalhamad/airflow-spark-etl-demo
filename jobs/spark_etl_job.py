from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit

def main(in_path: str, out_path: str):
    spark = (
        SparkSession.builder
        .appName("airflow_spark_etl_demo")
        .getOrCreate()
    )

    df = spark.read.option("header", "true").csv(in_path)

    df2 = (
        df
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("is_active", when(col("status") == lit("active"), lit(True)).otherwise(lit(False)))
        .drop("status")
    )

    df2.write.mode("overwrite").parquet(out_path)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", required=True)
    parser.add_argument("--out", dest="out_path", required=True)
    args = parser.parse_args()
    main(args.in_path, args.out_path)
