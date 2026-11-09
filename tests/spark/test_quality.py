import pytest
from pyspark.sql import functions as F

from aq_lakehouse.spark import quality as q

pytestmark = pytest.mark.spark


def test_checks(spark):
    df = spark.createDataFrame(
        [(1, "a", 5.0), (1, "b", -1.0), (2, None, 3.0)], "id int, name string, v double"
    )
    results = {
        r.check: r
        for r in q.run_checks(
            "t",
            df,
            [
                q.unique("id"),
                q.not_null("name"),
                q.expression("v >= 0", F.col("v") >= 0),
                q.max_ratio("negative", F.col("v") < 0, 0.5),
                q.min_rows(5),
            ],
        )
    }
    assert not results["unique(id)"].passed and results["unique(id)"].failed_rows == 1
    assert results["not_null(name)"].failed_rows == 1
    assert results["v >= 0"].failed_rows == 1
    assert results["negative"].passed
    assert not results["min_rows(5)"].passed
