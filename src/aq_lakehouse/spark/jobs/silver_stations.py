import logging

from aq_lakehouse.spark.jobs.common import latest_snapshot, object_store, setup
from aq_lakehouse.spark.session import get_spark
from aq_lakehouse.spark.transforms.stations import build_code_map, clean_sensors, clean_stations

log = logging.getLogger("silver_stations")


def main() -> None:
    _, cfg, tables = setup("Station and sensor reference data -> silver")
    store = object_store(cfg)
    snapshot = latest_snapshot(store, cfg.layer_key("bronze", "gios_archive", "metadata"))
    log.info("Using metadata snapshot %s", snapshot)

    spark = get_spark("silver_stations")
    src = tables.bronze_metadata(snapshot)
    read = spark.read.option("header", True).option("multiLine", True).csv

    stations = clean_stations(read(f"{src}/stations.csv")).cache()
    stations.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(
        tables.stations
    )
    build_code_map(stations).write.format("delta").mode("overwrite").option(
        "overwriteSchema", True
    ).save(tables.station_code_map)
    clean_sensors(read(f"{src}/sensors.csv")).write.format("delta").mode("overwrite").option(
        "overwriteSchema", True
    ).save(tables.sensors)

    log.info("stations: %d", stations.count())
    spark.stop()


if __name__ == "__main__":
    main()
