from datetime import date, datetime, timedelta

import pytest

from aq_lakehouse.spark.transforms import gold

pytestmark = pytest.mark.spark

MEAS_SCHEMA = (
    "station_code string, pollutant string, averaging_period string, measured_at timestamp, "
    "measured_date date, year int, value double, is_valid boolean"
)


def hourly(station, pollutant, day, values):
    start = datetime(day.year, day.month, day.day)
    return [
        (station, pollutant, "1h", start + timedelta(hours=i), day, day.year, v, True)
        for i, v in enumerate(values)
    ]


def test_daily_station_completeness_and_fallback(spark):
    d1, d2 = date(2023, 1, 1), date(2023, 1, 2)
    rows = (
        hourly("A", "PM10", d1, [60.0] * 20)  # complete, exceeds 50
        + hourly("A", "PM10", d2, [10.0] * 10)  # incomplete hourly day...
        + [("A", "PM10", "24h", datetime(2023, 1, 2), d2, 2023, 30.0, True)]  # ...but 24h exists
        + [("A", "PM10", "24h", datetime(2023, 1, 1), d1, 2023, 99.0, True)]  # hourly wins here
        + [("A", "PM10", "1h", datetime(2023, 1, 1, 21), d1, 2023, 999.0, False)]  # invalid
    )
    df = spark.createDataFrame(rows, MEAS_SCHEMA)
    out = {r.measured_date: r for r in gold.daily_station(df).collect()}

    assert out[d1].source_period == "1h"
    assert out[d1].daily_mean == 60.0
    assert out[d1].n_hours == 20
    assert out[d1].exceeds_eu_daily is True
    assert out[d2].source_period == "24h"
    assert out[d2].daily_mean == 30.0
    assert out[d2].exceeds_eu_daily is False


def test_annual_compliance_pm10_exceedance_days(spark):
    days = [date(2023, 1, 1) + timedelta(days=i) for i in range(365)]
    rows = [
        ("A", "PM10", d, 2023, 60.0 if i < 36 else 20.0, True, "24h", i < 36, True)
        for i, d in enumerate(days)
    ]
    daily = spark.createDataFrame(
        rows,
        "station_code string, pollutant string, measured_date date, year int, daily_mean double, "
        "is_complete boolean, source_period string, exceeds_eu_daily boolean, "
        "exceeds_who_daily boolean",
    )
    r = gold.annual_compliance(daily).first()
    assert r.valid_days == 365
    assert r.is_representative
    assert r.eu_exceedance_days == 36
    assert r.meets_eu_annual  # mean ~24.4 < 40
    assert r.meets_eu_daily is False  # 36 > 35 allowed
    assert r.eu_compliant is False
    assert r.meets_who_annual is False
