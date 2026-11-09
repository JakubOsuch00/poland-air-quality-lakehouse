"""Daily incremental run: live API + last week of weather, merged into the lakehouse."""

from datetime import UTC, date, datetime, timedelta

from airflow.sdk import DAG, task
from aq_common import DEFAULT_ARGS, spark_job, store_and_config

with DAG(
    dag_id="air_quality_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gios", "daily"],
) as dag:

    @task
    def ingest_api() -> str:
        from aq_lakehouse.ingestion.gios_api import GiosApiIngestor

        cfg, store = store_and_config()
        run_ts = datetime.now(UTC)
        ingestor = GiosApiIngestor(cfg, store)
        sensors = ingestor.ingest_stations(run_ts.strftime("%Y-%m-%d"))
        ingestor.ingest_measurements(sensors, run_ts)
        return run_ts.strftime("%Y-%m-%d")

    @task(retries=2, retry_delay=timedelta(minutes=65))
    def ingest_weather() -> str:
        from aq_lakehouse.ingestion.open_meteo import OpenMeteoIngestor, stations_for_period

        cfg, store = store_and_config()
        # archive API lags a few days behind
        end = date.today() - timedelta(days=3)
        start = end - timedelta(days=7)
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

    @task
    def affected_years() -> str:
        today = date.today()
        # weather window can reach back into the previous year in early january
        return ",".join(sorted({str(today.year), str((today - timedelta(days=10)).year)}))

    api_date = ingest_api()
    weather_start = ingest_weather()
    years = affected_years()

    silver_api = spark_job(
        "silver_api", "silver_api", ["--dates", "{{ ti.xcom_pull(task_ids='ingest_api') }}"]
    )
    silver_weather = spark_job(
        "silver_weather",
        "silver_weather",
        ["--starts", "{{ ti.xcom_pull(task_ids='ingest_weather') }}"],
    )
    years_arg = "{{ ti.xcom_pull(task_ids='affected_years') }}"
    gold = spark_job("gold", "gold", ["--years", years_arg])
    quality = spark_job(
        "quality_checks", "quality_checks", ["--years", years_arg, "--check-freshness"]
    )

    api_date >> silver_api
    weather_start >> silver_weather
    [silver_api, silver_weather, years] >> gold >> quality
