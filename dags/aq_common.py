from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS_DIR = "/opt/aq/src/aq_lakehouse/spark/jobs"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.memory": "2g",
    "spark.executor.memory": "3g",
    "spark.sql.shuffle.partitions": "64",
}


def spark_job(task_id: str, job: str, args: list[str] | None = None, **kwargs):
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"{JOBS_DIR}/{job}.py",
        application_args=args or [],
        name=f"aq_{task_id}",
        conf=SPARK_CONF,
        # one spark app at a time - the local cluster is small
        pool="spark",
        **kwargs,
    )


def store_and_config():
    from aq_lakehouse.config import load_config
    from aq_lakehouse.storage import ObjectStore

    cfg = load_config()
    store = ObjectStore(cfg.s3)
    store.ensure_bucket()
    return cfg, store
