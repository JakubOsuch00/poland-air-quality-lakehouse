import logging
import sys
import uuid
from datetime import timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from aq_lakehouse.spark import quality as q
from aq_lakehouse.spark.jobs.common import setup
from aq_lakehouse.spark.session import get_spark
from aq_lakehouse.spark.transforms.measurements import MAX_PLAUSIBLE, MEASUREMENT_KEY

log = logging.getLogger("quality_checks")


def main() -> None:
    def args(p):
        p.add_argument("--years", nargs="+", required=True)
        p.add_argument("--check-freshness", action="store_true", help="for the daily run")

    opts, cfg, tables = setup("Data quality checks", args)
    spark = get_spark("quality_checks")
    years = F.col("year").isin(opts.years)
    load = spark.read.format("delta").load

    max_value = F.create_map([F.lit(x) for kv in MAX_PLAUSIBLE.items() for x in kv])

    measurement_checks = [
        q.min_rows(1),
        q.not_null("station_code", "pollutant", "measured_at", "measured_date"),
        q.unique(*MEASUREMENT_KEY),
        q.expression(
            "valid values within plausible range",
            ~F.col("is_valid") | F.col("value").between(F.lit(0), max_value[F.col("pollutant")]),
        ),
        q.max_ratio("unknown station codes", ~F.col("station_known"), 0.02),
        q.max_ratio("flagged values", ~F.col("is_valid"), 0.01),
    ]
    if opts.check_freshness:
        api = load(tables.measurements).filter("source = 'api'")
        results = q.run_checks(
            "silver.measurements[api]", api, [q.fresh("measured_at", timedelta(hours=6))]
        )
    else:
        results = []

    results += q.run_checks(
        "silver.measurements", load(tables.measurements).filter(years), measurement_checks
    )
    results += q.run_checks(
        "gold.dim_station",
        load(tables.gold("dim_station")),
        [q.unique("station_code"), q.not_null("station_code", "station_name")],
    )
    results += q.run_checks(
        "gold.daily_station",
        load(tables.gold("daily_station")).filter(years),
        [
            q.unique("station_code", "pollutant", "measured_date"),
            q.expression("daily_mean >= 0", F.col("daily_mean") >= 0),
            q.expression(
                "n_hours between 1 and 24",
                F.col("n_hours").isNull() | F.col("n_hours").between(1, 24),
            ),
        ],
    )
    if DeltaTable.isDeltaTable(spark, tables.weather):
        results += q.run_checks(
            "silver.weather_hourly",
            load(tables.weather).filter(years),
            [
                q.unique("station_code", "observed_at"),
                q.expression(
                    "temperature in [-45, 45]",
                    F.col("temperature_c").between(-45, 45),
                    severity=q.WARN,
                ),
            ],
        )

    run_id = str(uuid.uuid4())
    q.results_df(spark, run_id, results).write.format("delta").mode("append").save(
        tables.gold("dq_results")
    )

    failed_errors = []
    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.severity == q.ERROR else "WARN")
        log.info("%-5s %-24s %-55s %s %s", status, r.table, r.check, r.failed_rows, r.details)
        if not r.passed and r.severity == q.ERROR:
            failed_errors.append(r)

    spark.stop()
    if failed_errors:
        log.error("%d blocking checks failed (run %s)", len(failed_errors), run_id)
        sys.exit(1)


if __name__ == "__main__":
    main()
