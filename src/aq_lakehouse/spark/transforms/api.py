from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from aq_lakehouse.spark.transforms import measurements as m

API_TZ = "Europe/Warsaw"

STATION_FIELDS = {
    "Identyfikator stacji": "station_id",
    "Kod stacji": "station_code",
    "Nazwa stacji": "station_name",
    "WGS84 φ N": "latitude",
    "WGS84 λ E": "longitude",
    "Identyfikator miasta": "city_id",
    "Nazwa miasta": "city",
    "Gmina": "commune",
    "Powiat": "district",
    "Województwo": "voivodeship",
    "Ulica": "street",
}


def clean_api_stations(raw: DataFrame) -> DataFrame:
    df = raw.select([F.col(f"`{src}`").alias(dst) for src, dst in STATION_FIELDS.items()])
    return (
        df.withColumn("station_id", F.col("station_id").cast("int"))
        .withColumn("city_id", F.col("city_id").cast("int"))
        .withColumn("latitude", F.col("latitude").try_cast("double"))
        .withColumn("longitude", F.col("longitude").try_cast("double"))
        .withColumn("voivodeship", F.initcap("voivodeship"))
        .dropDuplicates(["station_id"])
    )


def explode_payload(raw: DataFrame) -> DataFrame:
    return raw.select(
        "sensor_id",
        "station_id",
        "pollutant",
        F.col("fetched_at").cast("timestamp").alias("fetched_at"),
        F.explode("values").alias("v"),
    ).select(
        "sensor_id",
        "station_id",
        "pollutant",
        "fetched_at",
        F.col("v").getField("Kod stanowiska").alias("sensor_code"),
        F.col("v").getField("Data").alias("label"),
        F.col("v").getField("Wartość").cast("double").alias("value_original"),
    )


def to_silver(exploded: DataFrame, stations: DataFrame) -> DataFrame:
    """API values -> same shape as archive rows in silver.measurements.

    API timestamps are local time (with DST) and mark the end of the hour.
    """
    df = exploded.filter(F.col("value_original").isNotNull()).join(
        stations.select("station_id", "station_code"), "station_id", "left"
    )
    label_utc = F.to_utc_timestamp(F.to_timestamp("label"), API_TZ)
    measured_at = label_utc - F.expr("INTERVAL 1 HOUR")
    df = (
        df.withColumn("measured_at", measured_at)
        # same convention as the archive: date in UTC+1
        .withColumn("measured_date", F.to_date(F.col("measured_at") + F.expr("INTERVAL 1 HOUR")))
        .withColumn("year", F.year("measured_date"))
        .withColumn("averaging_period", F.lit("1h"))
        .withColumn("unit_original", F.lit("ug/m3"))
        .withColumn("value", F.col("value_original"))
        .withColumn("raw_value", F.col("value_original").cast("string"))
        .withColumn("raw_station_code", F.col("station_code"))
        .withColumn("station_known", F.col("station_code").isNotNull())
        .withColumn("source", F.lit("api"))
        .withColumn("source_file", F.lit(None).cast("string"))
        .withColumn("ingested_at", F.col("fetched_at"))
        .filter(F.col("station_code").isNotNull())
    )
    df = m.flag_quality(df)

    # runs overlap (API returns ~3 days) - the latest fetch wins
    w = Window.partitionBy(*m.MEASUREMENT_KEY).orderBy(F.col("fetched_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return df.select(m.SILVER_COLUMNS)
