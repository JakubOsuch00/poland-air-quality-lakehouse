import logging
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from aq_lakehouse.spark.jobs.common import latest_snapshot, object_store, setup
from aq_lakehouse.spark.scd2 import scd2_merge
from aq_lakehouse.spark.session import get_spark
from aq_lakehouse.spark.transforms import api

log = logging.getLogger("silver_api")

TRACKED = ["station_code", "station_name", "latitude", "longitude", "city", "commune",
           "district", "voivodeship", "street"]  # fmt: skip


def main() -> None:
    def args(p):
        p.add_argument("--dates", nargs="+", required=True, help="YYYY-MM-DD, bronze partitions")

    opts, cfg, tables = setup("GIOŚ API bronze -> silver", args)
    store = object_store(cfg)
    spark = get_spark("silver_api")

    snapshot = latest_snapshot(store, cfg.layer_key("bronze", "gios_api", "stations"))
    stations = api.clean_api_stations(
        spark.read.option("multiLine", True).json(
            f"{tables.bronze_api_stations(snapshot)}/stations.json"
        )
    ).cache()

    log.info("Applying station snapshot %s", snapshot)
    scd2_merge(
        spark,
        tables.station_history,
        stations,
        key="station_id",
        tracked=TRACKED,
        snapshot_date=date.fromisoformat(snapshot),
    )

    paths = [tables.bronze_api_measurements(d) for d in opts.dates]
    exploded = api.explode_payload(spark.read.json(paths))
    updates = api.to_silver(exploded, stations).cache()
    if updates.isEmpty():
        log.warning("Nothing to merge")
        return

    # prune the merge to the partitions that are actually touched
    years = [r.year for r in updates.select("year").distinct().collect()]
    pollutants = [r.pollutant for r in updates.select("pollutant").distinct().collect()]
    prune = (
        f"t.year IN ({', '.join(map(str, years))}) AND "
        f"t.pollutant IN ({', '.join(repr(p) for p in pollutants)})"
    )
    key = " AND ".join(
        f"t.{c} = s.{c}" for c in ["station_code", "pollutant", "averaging_period", "measured_at"]
    )

    target = DeltaTable.forPath(spark, tables.measurements)
    (
        target.alias("t")
        .merge(updates.alias("s"), f"{prune} AND {key}")
        # archive data is the verified version, never overwrite it with the live feed
        .whenMatchedUpdateAll(condition="t.source = 'api'")
        .whenNotMatchedInsertAll()
        .execute()
    )

    metrics = target.history(1).select(F.col("operationMetrics")).first()[0]
    log.info(
        "merge: inserted=%s updated=%s",
        metrics.get("numTargetRowsInserted"),
        metrics.get("numTargetRowsUpdated"),
    )
    spark.stop()


if __name__ == "__main__":
    main()
