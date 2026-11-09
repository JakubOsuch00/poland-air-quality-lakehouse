"""Hourly weather from the Open-Meteo archive API for stations measuring PM."""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime

from aq_lakehouse.config import PipelineConfig
from aq_lakehouse.ingestion.http import get_json, make_session
from aq_lakehouse.storage import ObjectStore

log = logging.getLogger(__name__)

# open-meteo accepts many coordinates in one call, but bigger batches eat the rate limit faster
BATCH_SIZE = 25


@dataclass(frozen=True)
class StationPoint:
    station_code: str
    latitude: float
    longitude: float


def _parse_date(value: str) -> date | None:
    # metadata has a few typos like "31.12.20025" - treat anything unparseable as unknown
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _overlaps(launch: str, close: str, start: date, end: date) -> bool:
    launch_d, close_d = _parse_date(launch), _parse_date(close)
    if launch_d and launch_d > end:
        return False
    return not (close_d and close_d < start)


def stations_for_period(
    stations_csv: str, sensors_csv: str, pollutants: set[str], start: date, end: date
) -> list[StationPoint]:
    """Stations that had a sensor for one of `pollutants` running between start and end."""
    sensors = csv.DictReader(io.StringIO(sensors_csv))
    wanted = {
        row["Kod stacji"]
        for row in sensors
        if row["Wskaźnik - kod"] in pollutants
        and _overlaps(row["Data uruchomienia"], row["Data zamknięcia"], start, end)
    }
    # φ/λ in the coordinate headers - easier to go by position
    reader = csv.reader(io.StringIO(stations_csv))
    next(reader)
    points = []
    for row in reader:
        code, lat, lon = row[1], row[13], row[14]
        if code in wanted and lat and lon:
            points.append(StationPoint(code, float(lat), float(lon)))
    return sorted(points, key=lambda p: p.station_code)


class OpenMeteoIngestor:
    def __init__(self, config: PipelineConfig, store: ObjectStore) -> None:
        self.cfg = config.section("open_meteo")
        self.config = config
        self.store = store
        self.session = make_session()

    def fetch(self, points: list[StationPoint], start: date, end: date) -> list[dict]:
        params = {
            "latitude": ",".join(f"{p.latitude:.5f}" for p in points),
            "longitude": ",".join(f"{p.longitude:.5f}" for p in points),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "hourly": ",".join(self.cfg["hourly_variables"]),
            "timezone": "GMT",
        }
        body = get_json(self.session, self.cfg["archive_url"], params, timeout=120)
        # single location -> object, many -> list
        return body if isinstance(body, list) else [body]

    def ingest(self, points: list[StationPoint], start: date, end: date) -> int:
        # free tier quota is ~1 year x 250 stations per hour, so a backfill has to be
        # resumable - batches already in bronze are skipped
        base = self.config.layer_key("bronze", "open_meteo", f"start={start.isoformat()}")
        for i in range(0, len(points), BATCH_SIZE):
            key = f"{base}/batch_{i // BATCH_SIZE:03d}.json"
            if self.store.exists(key):
                continue
            batch = points[i : i + BATCH_SIZE]
            results = self.fetch(batch, start, end)
            payload = [
                {"station_code": p.station_code, "end_date": end.isoformat(), **res}
                for p, res in zip(batch, results, strict=True)
            ]
            self.store.put_json(key, payload)
        log.info("Weather %s..%s for %d stations", start, end, len(points))
        return len(points)
