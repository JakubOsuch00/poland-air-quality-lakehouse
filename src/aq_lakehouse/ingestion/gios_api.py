"""GIOŚ REST API (v1) -> bronze. Field names in responses are in polish."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime

import requests

from aq_lakehouse.config import PipelineConfig
from aq_lakehouse.ingestion.http import get_json, make_session
from aq_lakehouse.storage import ObjectStore

log = logging.getLogger(__name__)

STATIONS_KEY = "Lista stacji pomiarowych"
SENSORS_KEY = "Lista stanowisk pomiarowych dla podanej stacji"
DATA_KEY = "Lista danych pomiarowych"


class GiosApiClient:
    def __init__(self, base_url: str, page_size: int = 500, timeout: int = 30, min_interval=0.3):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.timeout = timeout
        self.min_interval = min_interval
        self.session = make_session()
        self._last_call = 0.0

    def _get(self, path: str, params: dict | None = None) -> dict:
        # the API starts returning 429s pretty quickly without this
        wait = self.min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()
        return get_json(self.session, f"{self.base_url}/{path}", params, timeout=self.timeout)

    def _paginate(self, path: str, key: str) -> Iterator[dict]:
        page = 0
        while True:
            body = self._get(path, {"page": page, "size": self.page_size})
            yield from body.get(key, [])
            if page + 1 >= body.get("totalPages", 1):
                return
            page += 1

    def stations(self) -> list[dict]:
        return list(self._paginate("station/findAll", STATIONS_KEY))

    def sensors(self, station_id: int) -> list[dict]:
        return list(self._paginate(f"station/sensors/{station_id}", SENSORS_KEY))

    def sensor_data(self, sensor_id: int) -> list[dict] | None:
        try:
            return list(self._paginate(f"data/getData/{sensor_id}", DATA_KEY))
        except requests.HTTPError as exc:
            # manual (gravimetric) sensors return 400 - their data shows up weeks later
            if exc.response is not None and exc.response.status_code == 400:
                return None
            raise


class GiosApiIngestor:
    def __init__(
        self, config: PipelineConfig, store: ObjectStore, client: GiosApiClient | None = None
    ):
        cfg = config.section("gios_api")
        self.config = config
        self.store = store
        self.client = client or GiosApiClient(
            cfg["base_url"], cfg["page_size"], cfg["request_timeout_s"]
        )
        self.pollutants = set(config.section("gios_archive")["pollutants"])

    def _key(self, *parts: str) -> str:
        return self.config.layer_key("bronze", "gios_api", *parts)

    def ingest_stations(self, snapshot_date: str) -> list[dict]:
        stations = self.client.stations()
        sensors = []
        for st in stations:
            sensors.extend(self.client.sensors(st["Identyfikator stacji"]))
        base = self._key("stations", f"snapshot={snapshot_date}")
        self.store.put_json(f"{base}/stations.json", stations)
        self.store.put_json(f"{base}/sensors.json", sensors)
        log.info(
            "Stations snapshot %s: %d stations, %d sensors",
            snapshot_date,
            len(stations),
            len(sensors),
        )
        return sensors

    def ingest_measurements(self, sensors: list[dict], run_ts: datetime | None = None) -> int:
        # API returns ~3 days per sensor, overlap between runs is deduped in silver
        run_ts = run_ts or datetime.now(UTC)
        lines = []
        failed = skipped = 0
        for sensor in sensors:
            if sensor.get("Wskaźnik - kod") not in self.pollutants:
                continue
            try:
                values = self.client.sensor_data(sensor["Identyfikator stanowiska"])
            except Exception as exc:
                # one broken sensor shouldn't kill the whole run
                log.warning("Sensor %s failed: %s", sensor["Identyfikator stanowiska"], exc)
                failed += 1
                continue
            if values is None:
                skipped += 1
                continue
            lines.append(
                json.dumps(
                    {
                        "sensor_id": sensor["Identyfikator stanowiska"],
                        "station_id": sensor["Identyfikator stacji"],
                        "pollutant": sensor["Wskaźnik - kod"],
                        "fetched_at": run_ts.isoformat(timespec="seconds"),
                        "values": values,
                    },
                    ensure_ascii=False,
                )
            )

        if failed > len(lines):
            raise RuntimeError(f"{failed} sensors failed, only {len(lines)} ok")

        key = self._key("measurements", f"date={run_ts:%Y-%m-%d}", f"run_{run_ts:%H%M%S}.jsonl")
        self.store.put_bytes(key, "\n".join(lines).encode("utf-8"), "application/x-ndjson")
        log.info(
            "Wrote %d sensor payloads to %s (%d manual skipped, %d failed)",
            len(lines),
            key,
            skipped,
            failed,
        )
        return len(lines)
