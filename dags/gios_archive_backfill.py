"""Loads whole years from the GIOŚ archive: bronze -> silver -> gold -> quality checks."""

from datetime import datetime

from airflow.sdk import DAG, Param, task
from aq_common import DEFAULT_ARGS, spark_job, store_and_config

with DAG(
    dag_id="gios_archive_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "years": Param([2023, 2024], type="array", items={"type": "integer"}),
        "force_download": Param(False, type="boolean"),
    },
    tags=["gios", "backfill"],
) as dag:

    @task
    def station_metadata():
        from aq_lakehouse.ingestion.gios_archive import GiosArchiveIngestor

        cfg, store = store_and_config()
        GiosArchiveIngestor(cfg, store).ingest_station_metadata()

    @task
    def years(params=None) -> list[int]:
        return sorted(set(params["years"]))

    @task(max_active_tis_per_dag=2)
    def ingest_year(year: int, params=None) -> int:
        from aq_lakehouse.ingestion.gios_archive import GiosArchiveIngestor

        cfg, store = store_and_config()
        sheets = GiosArchiveIngestor(cfg, store).ingest_year(
            year, force_download=params["force_download"]
        )
        return len(sheets)

    year_list = years()
    years_arg = "{{ ti.xcom_pull(task_ids='years') | join(',') }}"

    bronze = ingest_year.expand(year=year_list)
    silver_stations = spark_job("silver_stations", "silver_stations")
    silver = spark_job("silver_measurements", "silver_archive_measurements", ["--years", years_arg])
    gold = spark_job("gold", "gold", ["--years", years_arg])
    quality = spark_job("quality_checks", "quality_checks", ["--years", years_arg])

    station_metadata() >> silver_stations
    [bronze, silver_stations] >> silver >> gold >> quality
