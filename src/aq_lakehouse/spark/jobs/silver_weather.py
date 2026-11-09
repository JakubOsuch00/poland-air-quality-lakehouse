import logging

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from aq_lakehouse.spark.jobs.common import setup
from aq_lakehouse.spark.session import get_spark
from aq_lakehouse.spark.transforms.weather import explode_hourly

log = logging.getLogger("silver_weather")


def main() -> None:
    def args(p):
        p.add_argument("--starts", nargs="+", required=True, help="start= partitions in bronze")

    opts, cfg, tables = setup("Open-Meteo bronze -> silver.weather_hourly", args)
    spark = get_spark("silver_weather")

    paths = [cfg.layer_uri("bronze", "open_meteo", f"start={s}") for s in opts.starts]
    df = explode_hourly(spark.read.option("multiLine", True).json(paths))
    # overlapping requests -> keep one row per station/hour
    w = Window.partitionBy("station_code", "observed_at").orderBy(F.lit(1))
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    if not DeltaTable.isDeltaTable(spark, tables.weather):
        df.write.format("delta").partitionBy("year").save(tables.weather)
    else:
        (
            DeltaTable.forPath(spark, tables.weather)
            .alias("t")
            .merge(
                df.alias("s"),
                "t.year = s.year AND t.station_code = s.station_code "
                "AND t.observed_at = s.observed_at",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    log.info("weather rows processed: %d", df.count())
    spark.stop()


if __name__ == "__main__":
    main()
