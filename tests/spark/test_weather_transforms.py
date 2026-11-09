from datetime import datetime

import pytest

from aq_lakehouse.spark.transforms.weather import explode_hourly

pytestmark = pytest.mark.spark


def test_explode_hourly(spark):
    raw = spark.createDataFrame(
        [("DsA", 120.0, (["2023-01-01T00:00", "2023-01-01T01:00"], [1.5, 2.0], [800.0, None]))],
        "station_code string, elevation double, "
        "hourly struct<time: array<string>, temperature_2m: array<double>, "
        "boundary_layer_height: array<double>>",
    )
    rows = explode_hourly(raw).orderBy("observed_at").collect()
    assert len(rows) == 2
    assert rows[0].observed_at == datetime(2023, 1, 1, 0)
    assert rows[0].temperature_c == 1.5
    assert rows[1].boundary_layer_height_m is None
    assert rows[0].year == 2023
    # variables that weren't requested simply aren't there
    assert "wind_speed_kmh" not in rows[0].asDict()
