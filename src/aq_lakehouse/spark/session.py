from __future__ import annotations

import os

from pyspark.sql import SparkSession

from aq_lakehouse.config import S3Settings


def spark_conf(s3: S3Settings) -> dict[str, str]:
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.databricks.delta.schema.autoMerge.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": s3.endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.access.key": s3.access_key,
        "spark.hadoop.fs.s3a.secret.key": s3.secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(s3.endpoint.startswith("https")).lower(),
    }


def get_spark(app_name: str, master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    master = master or os.getenv("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)
    driver_host = os.getenv("SPARK_DRIVER_HOST")
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    for key, value in spark_conf(S3Settings.from_env()).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
