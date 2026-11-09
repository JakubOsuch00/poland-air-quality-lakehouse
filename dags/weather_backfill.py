"""Open-Meteo history for whole years. The free API allows roughly one year per hour,
so failed years are retried after the quota resets."""

from datetime import date, datetime, timedelta

from airflow.sdk import DAG, Param, task
from aq_common import DEFAULT_ARGS, spark_job, store_and_config

with DAG(
    dag_id="weather_backfill",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"years": Param([2023, 2024], type="array", items={"type": "integer"})},
    tags=["weather", "backfill"],
) as dag:

    @task
    def years(params=None) -> list[int]:
        return sorted(set(params["years"]))

    @task(max_active_tis_per_dag=1, retries=4, retry_delay=timedelta(minutes=65))
    def fetch_year(year: int) -> str:
        from aq_lakehouse.ingestion.open_meteo import OpenMeteoIngestor, stations_for_period

        cfg, store = store_and_config()
        start, end = date(year, 1, 1), min(date(year, 12, 31), date.today() - timedelta(days=5))
        meta = cfg.layer_key("bronze", "gios_archive", "metadata")
        snapshot = store.latest_partition(meta)
        points = stations_for_period(
            store.get_bytes(f"{meta}/snapshot={snapshot}/stations.csv").decode(),
            store.get_bytes(f"{meta}/snapshot={snapshot}/sensors.csv").decode(),
            set(cfg.section("open_meteo")["pollutants_of_interest"]),
            start,
            end,
        )
        OpenMeteoIngestor(cfg, store).ingest(points, start, end)
        return start.isoformat()

    year_list = years()
    starts = fetch_year.expand(year=year_list)

    silver = spark_job(
        "silver_weather",
        "silver_weather",
        ["--starts", "{{ ti.xcom_pull(task_ids='fetch_year') | join(',') }}"],
    )
    gold = spark_job(
        "gold", "gold", ["--years", "{{ ti.xcom_pull(task_ids='years') | join(',') }}"]
    )
    starts >> silver >> gold
