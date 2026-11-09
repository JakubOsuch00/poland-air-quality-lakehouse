from datetime import date, datetime

import pytest

from aq_lakehouse.spark.transforms import api

pytestmark = pytest.mark.spark

PAYLOAD_SCHEMA = (
    "sensor_id int, station_id int, pollutant string, fetched_at string, "
    "values array<struct<`Kod stanowiska`: string, `Data`: string, `Wartość`: double>>"
)


def test_to_silver_handles_dst_and_overlapping_runs(spark):
    stations = spark.createDataFrame([(16, "DsDziePilsud")], "station_id int, station_code string")
    rows = [
        (
            101, 16, "PM10", "2026-07-01T10:00:00",
            [("DsDziePilsud-PM10-1g", "2026-07-01 12:00:00", 20.0),
             ("DsDziePilsud-PM10-1g", "2026-07-01 11:00:00", None)],
        ),
        # later run, same hour, corrected value
        (
            101, 16, "PM10", "2026-07-01T13:00:00",
            [("DsDziePilsud-PM10-1g", "2026-07-01 12:00:00", 22.0)],
        ),
        (
            101, 16, "PM10", "2026-01-15T10:00:00",
            [("DsDziePilsud-PM10-1g", "2026-01-15 01:00:00", 40.0)],
        ),
    ]  # fmt: skip
    raw = spark.createDataFrame(rows, PAYLOAD_SCHEMA)

    out = api.to_silver(api.explode_payload(raw), stations).orderBy("measured_at").collect()

    assert len(out) == 2
    winter, summer = out
    # 01:00 CET is the end of 00:00-01:00 local = 23:00 UTC the day before
    assert winter.measured_at == datetime(2026, 1, 14, 23)
    assert winter.measured_date == date(2026, 1, 15)
    # 12:00 CEST -> 10:00 UTC end of hour -> 09:00 UTC start
    assert summer.measured_at == datetime(2026, 7, 1, 9)
    assert summer.value == 22.0
    assert summer.source == "api"
