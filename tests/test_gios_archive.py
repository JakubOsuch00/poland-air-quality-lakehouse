import csv
import io
from datetime import datetime

import pytest

from aq_lakehouse.ingestion.gios_archive import (
    normalize_pollutant,
    sheet_to_csv,
    wanted_files,
)


def read_csv(buf):
    return list(csv.reader(io.StringIO(buf.getvalue())))


def test_old_layout():
    rows = iter(
        [
            ("Kod stacji", "DsBialka", "DsCzar07", None),
            ("Wskaźnik", "PM10", "PM10", None),
            ("Czas uśredniania", "1g", "1g", None),
            (datetime(2005, 1, 1, 1), 11.3, None, None),
            (datetime(2005, 1, 1, 2), 9.6, 20, None),
        ]
    )
    buf = io.StringIO()
    parsed = sheet_to_csv(rows, "2005_PM10_1g.xlsx", buf)

    assert parsed.year == 2005
    assert parsed.pollutant == "PM10"
    assert parsed.header_rows == 3
    assert parsed.data_rows == 2
    assert [c["station_code"] for c in parsed.columns] == ["DsBialka", "DsCzar07"]
    assert parsed.columns[0]["unit"] is None

    out = read_csv(buf)
    assert out[0] == ["timestamp", "DsBialka", "DsCzar07"]
    assert out[1] == ["2005-01-01T01:00:00", "11.3", ""]


def test_2018_layout_with_mislabelled_sensor_row():
    rows = iter(
        [
            ("Nr", 1, 2),
            ("Kod stacji", "DsCzerStraza", "DsCzerStraza"),
            ("Wskaźnik", "NO2", "NO2"),
            ("Czas uśredniania", "1g", "1g"),
            ("Jednostka", "ug/m3", "ug/m3"),
            ("Czas pomiaru", "DsCzerStraza-NO2-1g", "DsCzerStraza-NO2-1g-2"),
            (datetime(2018, 1, 1, 1), "2,04478", "5,03918"),
        ]
    )
    buf = io.StringIO()
    parsed = sheet_to_csv(rows, "2018_NO2_1g.xlsx", buf)

    assert parsed.header_rows == 6
    assert parsed.columns[0]["sensor_code"] == "DsCzerStraza-NO2-1g"
    assert parsed.columns[1]["column"] == "DsCzerStraza__2"
    assert parsed.columns[1]["unit"] == "ug/m3"
    # raw values are kept as they are, silver deals with the comma
    assert read_csv(buf)[1][1] == "2,04478"


def test_pollutant_name_from_header_wins():
    rows = iter(
        [
            ("Kod stacji", "DsLegPolarna"),
            ("Wskaźnik", "PM2.5"),
            ("Czas uśredniania", "24g"),
            (datetime(2023, 1, 1), 13.6),
        ]
    )
    parsed = sheet_to_csv(rows, "2023_PM25_24g.xlsx", io.StringIO())
    assert parsed.pollutant == "PM2.5"
    assert parsed.averaging_period == "24g"


def test_missing_station_row_raises():
    rows = iter([("Wskaźnik", "PM10"), (datetime(2023, 1, 1), 1.0)])
    with pytest.raises(ValueError):
        sheet_to_csv(rows, "2023_PM10_1g.xlsx", io.StringIO())


@pytest.mark.parametrize("raw,expected", [("PM25", "PM2.5"), ("pm25", "PM2.5"), ("NO2", "NO2")])
def test_normalize_pollutant(raw, expected):
    assert normalize_pollutant(raw) == expected


def test_wanted_files():
    names = [
        "2015_PM10_1g.xlsx",
        "2015_PM25_24g.xlsx",
        "2015_Pb(PM10)_24g.xlsx",
        "2015_depozycja.xlsx",
        "2015_NOx_1g.xlsx",
    ]
    assert wanted_files(names, {"PM10", "PM2.5"}, {"1g", "24g"}) == [
        "2015_PM10_1g.xlsx",
        "2015_PM25_24g.xlsx",
    ]
