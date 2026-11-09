from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from aq_lakehouse.spark import standards as std


def _lit_map(d: dict) -> F.Column:
    return F.create_map([F.lit(x) for kv in d.items() for x in kv])


def dim_station(stations: DataFrame, history: DataFrame | None) -> DataFrame:
    dim = stations.select(
        "station_code",
        "station_name",
        "international_code",
        "station_type",
        "area_type",
        "voivodeship",
        "city",
        "address",
        "latitude",
        "longitude",
        "launch_date",
        "close_date",
        F.col("close_date").isNull().alias("is_active"),
    )
    if history is None:
        return dim.withColumn("api_station_id", F.lit(None).cast("int")).withColumn(
            "district", F.lit(None).cast("string")
        )
    current = history.filter("is_current").select(
        "station_code", F.col("station_id").alias("api_station_id"), "district"
    )
    return dim.join(current, "station_code", "left")


def daily_station(measurements: DataFrame) -> DataFrame:
    """Daily values per station and pollutant.

    Hourly series are aggregated (day valid with >= 18 hours). Daily (24h) series are
    used as-is, but only where there is no complete hourly day for the same station.
    """
    valid = measurements.filter("is_valid")

    from_hourly = (
        valid.filter(F.col("averaging_period") == "1h")
        .groupBy("station_code", "pollutant", "measured_date", "year")
        .agg(
            F.avg("value").alias("daily_mean"),
            F.max("value").alias("daily_max_1h"),
            F.count("*").alias("n_hours"),
        )
        .withColumn("is_complete", F.col("n_hours") >= std.MIN_HOURS_PER_DAY)
        .withColumn("source_period", F.lit("1h"))
    )
    from_daily = valid.filter(F.col("averaging_period") == "24h").select(
        "station_code",
        "pollutant",
        "measured_date",
        "year",
        F.col("value").alias("daily_mean"),
        F.lit(None).cast("double").alias("daily_max_1h"),
        F.lit(None).cast("long").alias("n_hours"),
        F.lit(True).alias("is_complete"),
        F.lit("24h").alias("source_period"),
    )

    w = Window.partitionBy("station_code", "pollutant", "measured_date").orderBy(
        F.col("is_complete").desc(), F.col("source_period")
    )
    daily = (
        from_hourly.unionByName(from_daily)
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )

    eu = _lit_map(std.EU_DAILY_LIMIT)[F.col("pollutant")]
    who = _lit_map(std.WHO_DAILY)[F.col("pollutant")]
    return daily.withColumn(
        "exceeds_eu_daily", F.when(eu.isNotNull(), F.col("daily_mean") > eu)
    ).withColumn("exceeds_who_daily", F.when(who.isNotNull(), F.col("daily_mean") > who))


def annual_compliance(daily: DataFrame) -> DataFrame:
    complete = daily.filter("is_complete")
    days_in_year = F.dayofyear(F.make_date(F.col("year"), F.lit(12), F.lit(31)))
    df = complete.groupBy("station_code", "pollutant", "year").agg(
        F.avg("daily_mean").alias("annual_mean"),
        F.count("*").alias("valid_days"),
        F.sum(F.col("exceeds_eu_daily").cast("int")).alias("eu_exceedance_days"),
        F.sum(F.col("exceeds_who_daily").cast("int")).alias("who_exceedance_days"),
        F.percentile_approx("daily_mean", 0.904).alias("p90_4_daily"),
    )
    df = df.withColumn("coverage", F.col("valid_days") / days_in_year).withColumn(
        "is_representative", F.col("coverage") >= std.MIN_ANNUAL_COVERAGE
    )

    eu_annual = _lit_map(std.EU_ANNUAL_LIMIT)[F.col("pollutant")]
    who_annual = _lit_map(std.WHO_ANNUAL)[F.col("pollutant")]
    allowed = _lit_map(std.EU_DAILY_ALLOWED_EXCEEDANCES)[F.col("pollutant")]

    meets_annual = F.when(eu_annual.isNotNull(), F.col("annual_mean") <= eu_annual)
    meets_daily = F.when(allowed.isNotNull(), F.col("eu_exceedance_days") <= allowed)
    return (
        df.withColumn("eu_annual_limit", eu_annual)
        .withColumn("who_annual_guideline", who_annual)
        .withColumn("meets_eu_annual", meets_annual)
        .withColumn("meets_eu_daily", meets_daily)
        .withColumn(
            "eu_compliant",
            F.when(
                meets_annual.isNotNull() | meets_daily.isNotNull(),
                F.coalesce(meets_annual, F.lit(True)) & F.coalesce(meets_daily, F.lit(True)),
            ),
        )
        .withColumn(
            "meets_who_annual", F.when(who_annual.isNotNull(), F.col("annual_mean") <= who_annual)
        )
    )


def monthly_city(daily: DataFrame, dim: DataFrame) -> DataFrame:
    df = daily.filter("is_complete").join(
        dim.select("station_code", "city", "voivodeship"), "station_code"
    )
    return (
        df.withColumn("month", F.trunc("measured_date", "month"))
        .groupBy("voivodeship", "city", "pollutant", "year", "month")
        .agg(
            F.avg("daily_mean").alias("mean"),
            F.max("daily_mean").alias("max_daily_mean"),
            F.countDistinct("station_code").alias("n_stations"),
            # a day counts as exceeded if any station in the city exceeded
            F.countDistinct(F.when(F.col("exceeds_eu_daily"), F.col("measured_date"))).alias(
                "eu_exceedance_days"
            ),
        )
    )


def daily_pm_weather(daily: DataFrame, weather: DataFrame) -> DataFrame:
    pm = (
        daily.filter("is_complete")
        .filter(F.col("pollutant").isin("PM10", "PM2.5"))
        .groupBy("station_code", "measured_date", "year")
        .pivot("pollutant", ["PM10", "PM2.5"])
        .agg(F.first("daily_mean"))
        .withColumnRenamed("PM10", "pm10")
        .withColumnRenamed("PM2.5", "pm25")
    )
    # weather is in UTC, measurement dates in UTC+1
    w = (
        weather.withColumn(
            "measured_date", F.to_date(F.col("observed_at") + F.expr("INTERVAL 1 HOUR"))
        )
        .groupBy("station_code", "measured_date")
        .agg(
            F.avg("temperature_c").alias("temp_mean_c"),
            F.min("temperature_c").alias("temp_min_c"),
            F.avg("wind_speed_kmh").alias("wind_mean_kmh"),
            F.sum("precipitation_mm").alias("precipitation_mm"),
            F.avg("relative_humidity_pct").alias("humidity_mean_pct"),
            F.avg("boundary_layer_height_m").alias("blh_mean_m"),
            F.count("*").alias("weather_hours"),
        )
    )
    return (
        pm.join(w, ["station_code", "measured_date"], "inner")
        .withColumn("is_heating_season", F.month("measured_date").isin(10, 11, 12, 1, 2, 3, 4))
        .withColumn("low_wind", F.col("wind_mean_kmh") < 10)
    )
