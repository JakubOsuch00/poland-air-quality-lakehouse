from datetime import date

import pytest
from pyspark.sql import functions as F

from aq_lakehouse.spark.scd2 import scd2_merge

pytestmark = pytest.mark.spark

SCHEMA = "station_id int, name string, city string"


def apply(spark, path, rows, day):
    scd2_merge(
        spark, path, spark.createDataFrame(rows, SCHEMA), "station_id", ["name", "city"], day
    )


def history(spark, path, station_id):
    return (
        spark.read.format("delta")
        .load(path)
        .filter(F.col("station_id") == station_id)
        .orderBy("valid_from", "is_current")
        .collect()
    )


def test_scd2_lifecycle(spark, tmp_path):
    path = str(tmp_path / "station_history")

    apply(spark, path, [(1, "Kraków Bujaka", "Kraków"), (2, "Gdańsk", "Gdańsk")], date(2024, 1, 1))
    # 1 renamed, 2 gone, 3 new
    apply(spark, path, [(1, "Kraków ul. Bujaka", "Kraków"), (3, "Łódź", "Łódź")], date(2024, 2, 1))
    # same snapshot again - nothing should change
    apply(spark, path, [(1, "Kraków ul. Bujaka", "Kraków"), (3, "Łódź", "Łódź")], date(2024, 2, 1))

    s1 = history(spark, path, 1)
    assert [(r.name, r.valid_from, r.valid_to, r.is_current) for r in s1] == [
        ("Kraków Bujaka", date(2024, 1, 1), date(2024, 2, 1), False),
        ("Kraków ul. Bujaka", date(2024, 2, 1), None, True),
    ]

    s2 = history(spark, path, 2)
    assert len(s2) == 1 and not s2[0].is_current and s2[0].valid_to == date(2024, 2, 1)

    s3 = history(spark, path, 3)
    assert len(s3) == 1 and s3[0].is_current

    total = spark.read.format("delta").load(path).count()
    assert total == 4
