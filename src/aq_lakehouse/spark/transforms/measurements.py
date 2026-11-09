from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# archive timestamps are fixed UTC+1 (no DST - every year has exactly 8760/8784 hours)
ARCHIVE_UTC_OFFSET_H = 1

# units are missing in older files, these were the units used back then
DEFAULT_UNITS = {"CO": "mg/m3"}
UNIT_FACTORS = {"ug/m3": 1.0, "µg/m3": 1.0, "mg/m3": 1000.0}

# sanity limits in ug/m3, anything above is almost certainly an instrument error
MAX_PLAUSIBLE = {
    "PM10": 3000.0,
    "PM2.5": 2000.0,
    "NO2": 2000.0,
    "SO2": 3000.0,
    "O3": 1000.0,
    "CO": 100000.0,
    "C6H6": 500.0,
}

PERIODS = {"1g": "1h", "24g": "24h"}

MEASUREMENT_KEY = ["station_code", "pollutant", "averaging_period", "measured_at"]


def _literal_map(mapping: dict) -> Column:
    return F.create_map([F.lit(x) for kv in mapping.items() for x in kv])


def unpivot_sheet(df: DataFrame, value_columns: list[str]) -> DataFrame:
    return df.unpivot(
        ids=[F.col("timestamp")],
        values=[F.col(f"`{c}`") for c in value_columns],
        variableColumnName="column",
        valueColumnName="raw_value",
    ).filter(F.col("raw_value").isNotNull())


def parse_value(col: Column) -> Column:
    # 2016-2018 sheets store numbers as text with a decimal comma
    return F.regexp_replace(F.trim(col), ",", ".").try_cast("double")


def round_to_hour(ts: Column) -> Column:
    # excel timestamps drift by a few ms per row
    return F.timestamp_seconds((F.round(F.unix_micros(ts) / 3_600_000_000) * 3600).cast("long"))


def add_time_columns(df: DataFrame) -> DataFrame:
    """measured_at = start of the averaging period in UTC, measured_date = local (UTC+1) date.

    Hourly values are labelled with the END of the hour, daily values with the day itself.
    """
    label = round_to_hour(F.to_timestamp("timestamp"))
    is_hourly = F.col("averaging_period") == "1h"
    local_start = F.when(is_hourly, label - F.expr("INTERVAL 1 HOUR")).otherwise(label)
    return df.withColumn(
        "measured_at", local_start - F.expr(f"INTERVAL {ARCHIVE_UTC_OFFSET_H} HOUR")
    ).withColumn("measured_date", F.to_date(local_start))


def normalize_units(df: DataFrame) -> DataFrame:
    unit = F.coalesce(
        F.lower(F.trim("unit_original")),
        _literal_map(DEFAULT_UNITS)[F.col("pollutant")],
        F.lit("ug/m3"),
    )
    factor = _literal_map(UNIT_FACTORS)[unit]
    return df.withColumn("unit_original", unit).withColumn(
        "value", F.col("value_original") * factor
    )


def flag_quality(df: DataFrame) -> DataFrame:
    limit = _literal_map(MAX_PLAUSIBLE)[F.col("pollutant")]
    flag = (
        F.when(F.col("value").isNull(), "unparseable")
        .when(F.col("value") < 0, "negative")
        .when(limit.isNotNull() & (F.col("value") > limit), "implausible")
    )
    return df.withColumn("qa_flag", flag).withColumn("is_valid", F.col("qa_flag").isNull())


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per station/pollutant/period/timestamp.

    Duplicates come from stations with two analysers for the same pollutant
    and from old codes collapsing into one current code.
    """
    w = Window.partitionBy(*MEASUREMENT_KEY).orderBy(
        F.col("is_valid").desc(), F.col("sensor_code").asc_nulls_last(), F.col("source_file")
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


SILVER_COLUMNS = [
    "station_code",
    "raw_station_code",
    "station_known",
    "sensor_code",
    "pollutant",
    "averaging_period",
    "measured_at",
    "measured_date",
    "value",
    "unit_original",
    "value_original",
    "raw_value",
    "qa_flag",
    "is_valid",
    "source",
    "source_file",
    "year",
    "ingested_at",
]
