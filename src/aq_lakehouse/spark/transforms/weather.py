from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# open-meteo name -> silver column
VARIABLES = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "relative_humidity_pct",
    "precipitation": "precipitation_mm",
    "surface_pressure": "pressure_hpa",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg",
    "boundary_layer_height": "boundary_layer_height_m",
}


def explode_hourly(raw: DataFrame) -> DataFrame:
    """One row per station and hour. Requested with timezone=GMT so times are UTC."""
    present = [v for v in VARIABLES if v in raw.schema["hourly"].dataType.names]
    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        *[F.col(f"hourly.{v}").alias(v) for v in present],
    )
    df = raw.select("station_code", "elevation", F.explode(zipped).alias("h"))
    return df.select(
        "station_code",
        F.to_timestamp("h.time").alias("observed_at"),
        F.col("elevation").cast("double").alias("grid_elevation_m"),
        *[F.col(f"h.{v}").cast("double").alias(VARIABLES[v]) for v in present],
    ).withColumn("year", F.year("observed_at"))
