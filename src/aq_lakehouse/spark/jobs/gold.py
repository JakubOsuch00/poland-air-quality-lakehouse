import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from aq_lakehouse.spark.jobs.common import setup
from aq_lakehouse.spark.session import get_spark
from aq_lakehouse.spark.transforms import gold

log = logging.getLogger("gold")


def write_years(df: DataFrame, path: str, years: list[int]) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year IN ({', '.join(map(str, years))})")
        .partitionBy("year")
        .save(path)
    )


def main() -> None:
    def args(p):
        p.add_argument("--years", type=int, nargs="+", required=True)

    opts, cfg, tables = setup("silver -> gold", args)
    spark = get_spark("gold")
    years = sorted(set(opts.years))

    history = (
        spark.read.format("delta").load(tables.station_history)
        if DeltaTable.isDeltaTable(spark, tables.station_history)
        else None
    )
    dim = gold.dim_station(spark.read.format("delta").load(tables.stations), history)
    dim.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(
        tables.gold("dim_station")
    )

    measurements = (
        spark.read.format("delta").load(tables.measurements).filter(F.col("year").isin(years))
    )
    write_years(gold.daily_station(measurements), tables.gold("daily_station"), years)

    # read back what we just wrote instead of recomputing the daily aggregation
    daily = (
        spark.read.format("delta")
        .load(tables.gold("daily_station"))
        .filter(F.col("year").isin(years))
    )
    write_years(gold.annual_compliance(daily), tables.gold("annual_compliance"), years)
    write_years(gold.monthly_city(daily, dim), tables.gold("monthly_city"), years)

    if DeltaTable.isDeltaTable(spark, tables.weather):
        weather = spark.read.format("delta").load(tables.weather).filter(F.col("year").isin(years))
        write_years(gold.daily_pm_weather(daily, weather), tables.gold("daily_pm_weather"), years)

    log.info("gold built for %s", years)
    spark.stop()


if __name__ == "__main__":
    main()
