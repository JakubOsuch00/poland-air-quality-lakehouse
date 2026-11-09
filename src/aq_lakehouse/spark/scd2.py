from __future__ import annotations

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def with_row_hash(df: DataFrame, tracked: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in tracked]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def scd2_merge(
    spark: SparkSession,
    path: str,
    snapshot: DataFrame,
    key: str,
    tracked: list[str],
    snapshot_date: date,
) -> None:
    """Type 2 history from full snapshots.

    - changed rows: current version closed, new version inserted
    - new keys: inserted
    - keys missing from the snapshot: current version closed
    Snapshots have to be applied in order.
    """
    updates = with_row_hash(snapshot, tracked).withColumn("valid_from", F.lit(snapshot_date))

    if not DeltaTable.isDeltaTable(spark, path):
        (
            updates.withColumn("valid_to", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta")
            .save(path)
        )
        return

    target = DeltaTable.forPath(spark, path)
    current = target.toDF().filter("is_current").select(key, F.col("row_hash").alias("_old_hash"))

    # rows whose attributes changed need to be inserted as a new version, so they go in
    # twice: once with a null merge key (-> insert) and once with the real key (-> close)
    changed = (
        updates.join(current, key)
        .filter(F.col("row_hash") != F.col("_old_hash"))
        .drop("_old_hash")
        .withColumn("_merge_key", F.lit(None).cast(updates.schema[key].dataType))
    )
    staged = updates.withColumn("_merge_key", F.col(key)).unionByName(changed)

    insert_values = {c: f"s.`{c}`" for c in updates.columns}
    insert_values |= {"valid_to": "CAST(NULL AS DATE)", "is_current": "true"}

    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.`{key}` = s._merge_key AND t.is_current")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "valid_to": "s.valid_from"},
        )
        .whenNotMatchedInsert(values=insert_values)
        .whenNotMatchedBySourceUpdate(
            condition="t.is_current",
            set={"is_current": "false", "valid_to": F.lit(snapshot_date)},
        )
        .execute()
    )
