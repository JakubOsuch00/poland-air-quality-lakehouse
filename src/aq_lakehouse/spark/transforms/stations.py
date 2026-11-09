from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# STACJE sheet, by position - polish headers with φ/λ are a pain to reference by name
STATION_COLUMNS = [
    "nr",
    "station_code",
    "international_code",
    "station_name",
    "old_codes",
    "launch_date",
    "close_date",
    "station_type",
    "area_type",
    "station_kind",
    "voivodeship",
    "city",
    "address",
    "latitude",
    "longitude",
]

SENSOR_COLUMNS = [
    "nr",
    "sensor_code",
    "station_code",
    "station_name",
    "pollutant_code",
    "pollutant_name",
    "averaging_time",
    "measurement_type",
    "launch_date",
    "close_date",
    "voivodeship",
    "zone_name",
]

STATION_TYPES = {"tło": "background", "przemysłowa": "industrial", "komunikacyjna": "traffic"}
AREA_TYPES = {"miejski": "urban", "podmiejski": "suburban", "pozamiejski": "rural"}


def _rename_by_position(df: DataFrame, names: list[str]) -> DataFrame:
    return df.select([F.col(f"`{c}`").alias(n) for c, n in zip(df.columns, names, strict=False)])


def _map_values(col: Column, mapping: dict[str, str]) -> Column:
    expr = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
    return F.coalesce(expr[F.lower(col)], F.lit("unknown"))


def _blank_to_null(col: Column) -> Column:
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))


def clean_stations(raw: DataFrame) -> DataFrame:
    df = _rename_by_position(raw, STATION_COLUMNS).drop("nr")
    df = df.select(
        F.trim("station_code").alias("station_code"),
        _blank_to_null(F.col("international_code")).alias("international_code"),
        F.trim("station_name").alias("station_name"),
        _blank_to_null(F.col("old_codes")).alias("old_codes"),
        F.col("launch_date").try_cast("date").alias("launch_date"),
        F.col("close_date").try_cast("date").alias("close_date"),
        _map_values(F.col("station_type"), STATION_TYPES).alias("station_type"),
        _map_values(F.col("area_type"), AREA_TYPES).alias("area_type"),
        F.trim("station_kind").alias("station_kind"),
        F.initcap(F.trim("voivodeship")).alias("voivodeship"),
        F.trim("city").alias("city"),
        _blank_to_null(F.col("address")).alias("address"),
        F.col("latitude").try_cast("double").alias("latitude"),
        F.col("longitude").try_cast("double").alias("longitude"),
    ).filter(F.col("station_code").isNotNull())

    # a couple of codes appear twice in the source - keep the most recent one
    w = Window.partitionBy("station_code").orderBy(
        F.col("close_date").desc_nulls_first(), F.col("launch_date").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_code_map(stations: DataFrame) -> DataFrame:
    """source_code -> station_code, including identity rows for current codes."""
    old = stations.select(
        F.explode(F.split("old_codes", r"\s*,\s*")).alias("source_code"),
        "station_code",
        "launch_date",
        "close_date",
    )
    current = stations.select(
        F.col("station_code").alias("source_code"), "station_code", "launch_date", "close_date"
    )
    return (
        old.unionByName(current)
        .withColumn("source_code", F.trim("source_code"))
        .filter(F.col("source_code") != "")
        .withColumn("is_current_code", F.col("source_code") == F.col("station_code"))
        .dropDuplicates(["source_code", "station_code"])
    )


def resolve_station_codes(pairs: DataFrame, code_map: DataFrame) -> DataFrame:
    """Pick a station for each (raw_station_code, year).

    Old codes are not unique - the same code was reused for stations that ran in different
    periods, so we prefer the station that was active in the given year.
    """
    year_start = F.make_date(F.col("year"), F.lit(1), F.lit(1))
    year_end = F.make_date(F.col("year"), F.lit(12), F.lit(31))
    active = (F.col("launch_date").isNull() | (F.col("launch_date") <= year_end)) & (
        F.col("close_date").isNull() | (F.col("close_date") >= year_start)
    )
    joined = pairs.join(
        code_map, pairs.raw_station_code == code_map.source_code, "left"
    ).withColumn("_active", active)
    w = Window.partitionBy("raw_station_code", "year").orderBy(
        F.col("is_current_code").desc_nulls_last(),
        F.col("_active").desc_nulls_last(),
        F.col("station_code"),
    )
    return (
        joined.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "raw_station_code",
            "year",
            F.coalesce("station_code", "raw_station_code").alias("station_code"),
            F.col("station_code").isNotNull().alias("station_known"),
        )
    )


def clean_sensors(raw: DataFrame) -> DataFrame:
    df = _rename_by_position(raw, SENSOR_COLUMNS)
    return df.select(
        F.trim("sensor_code").alias("sensor_code"),
        F.trim("station_code").alias("station_code"),
        F.trim("pollutant_code").alias("pollutant_code"),
        F.trim("pollutant_name").alias("pollutant_name"),
        F.trim("averaging_time").alias("averaging_time"),
        F.trim("measurement_type").alias("measurement_type"),
        F.col("launch_date").try_cast("date").alias("launch_date"),
        F.col("close_date").try_cast("date").alias("close_date"),
        F.trim("zone_name").alias("zone_name"),
    ).filter(F.col("sensor_code").isNotNull())
