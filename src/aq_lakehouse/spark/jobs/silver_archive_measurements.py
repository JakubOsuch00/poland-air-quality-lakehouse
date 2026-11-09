import logging
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from aq_lakehouse.spark.jobs.common import object_store, setup
from aq_lakehouse.spark.session import get_spark
from aq_lakehouse.spark.tables import Tables
from aq_lakehouse.spark.transforms import measurements as m
from aq_lakehouse.spark.transforms.stations import resolve_station_codes
from aq_lakehouse.storage import ObjectStore

log = logging.getLogger("silver_archive_measurements")


def read_sheet(spark: SparkSession, tables: Tables, year: int, meta: dict) -> DataFrame:
    stem = meta["_stem"]
    path = f"{tables.bronze_archive_sheets(year)}/{stem}.csv"
    # everything as string, parsing is done explicitly
    df = spark.read.option("header", True).option("inferSchema", False).csv(path)

    columns = spark.createDataFrame(
        [(c["column"], c["station_code"], c["sensor_code"], c["unit"]) for c in meta["columns"]],
        "column string, raw_station_code string, sensor_code string, unit_original string",
    )
    long = m.unpivot_sheet(df, [c["column"] for c in meta["columns"]])
    return long.join(F.broadcast(columns), "column").select(
        "timestamp",
        "raw_station_code",
        "sensor_code",
        "unit_original",
        "raw_value",
        F.lit(meta["pollutant"]).alias("pollutant"),
        F.lit(m.PERIODS.get(meta["averaging_period"], meta["averaging_period"])).alias(
            "averaging_period"
        ),
        F.lit(meta["source_file"]).alias("source_file"),
        F.lit(meta["ingested_at"]).cast("timestamp").alias("ingested_at"),
    )


def load_sheet_metadata(store: ObjectStore, prefix: str) -> list[dict]:
    metas = []
    for key in sorted(store.list_keys(prefix)):
        if key.endswith(".meta.json"):
            meta = store.get_json(key)
            meta["_stem"] = key.rsplit("/", 1)[-1].removesuffix(".meta.json")
            metas.append(meta)
    return metas


def build_year(spark: SparkSession, tables: Tables, store: ObjectStore, cfg, year: int):
    prefix = cfg.layer_key("bronze", "gios_archive", "sheets", f"year={year}") + "/"
    metas = load_sheet_metadata(store, prefix)
    if not metas:
        log.warning("No bronze sheets for %s, skipping", year)
        return None

    raw = reduce(DataFrame.unionByName, (read_sheet(spark, tables, year, meta) for meta in metas))
    raw = raw.withColumn("year", F.lit(year))

    code_map = spark.read.format("delta").load(tables.station_code_map)
    pairs = raw.select("raw_station_code", "year").distinct()
    resolved = resolve_station_codes(pairs, code_map)

    df = raw.join(F.broadcast(resolved), ["raw_station_code", "year"])
    df = df.withColumn("value_original", m.parse_value(F.col("raw_value")))
    df = m.add_time_columns(df)
    df = m.normalize_units(df)
    df = m.flag_quality(df)
    df = df.withColumn("source", F.lit("archive"))
    return m.deduplicate(df).select(m.SILVER_COLUMNS)


def main() -> None:
    def args(p):
        p.add_argument("--years", nargs="+", required=True)

    opts, cfg, tables = setup("GIOŚ archive bronze -> silver.measurements", args)
    store = object_store(cfg)
    spark = get_spark("silver_archive_measurements")

    for year in opts.years:
        df = build_year(spark, tables, store, cfg, year)
        if df is None:
            continue
        log.info("Writing year %s", year)
        (
            df.repartition("pollutant")
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"year = {year}")
            .partitionBy("year", "pollutant")
            .save(tables.measurements)
        )

    spark.stop()


if __name__ == "__main__":
    main()
