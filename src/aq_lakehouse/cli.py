# python -m aq_lakehouse.cli archive --years 2022 2023
from __future__ import annotations

import argparse
import logging

from aq_lakehouse.config import load_config
from aq_lakehouse.storage import ObjectStore


def _store():
    cfg = load_config()
    store = ObjectStore(cfg.s3)
    store.ensure_bucket()
    return cfg, store


def cmd_archive(args: argparse.Namespace) -> None:
    from aq_lakehouse.ingestion.gios_archive import GiosArchiveIngestor

    cfg, store = _store()
    ingestor = GiosArchiveIngestor(cfg, store)
    for year in args.years:
        ingestor.ingest_year(year, force_download=args.force)


def cmd_station_metadata(_: argparse.Namespace) -> None:
    from aq_lakehouse.ingestion.gios_archive import GiosArchiveIngestor

    cfg, store = _store()
    GiosArchiveIngestor(cfg, store).ingest_station_metadata()


def cmd_api(args: argparse.Namespace) -> None:
    from datetime import UTC, datetime

    from aq_lakehouse.ingestion.gios_api import GiosApiIngestor

    cfg, store = _store()
    ingestor = GiosApiIngestor(cfg, store)
    sensors = ingestor.ingest_stations(datetime.now(UTC).strftime("%Y-%m-%d"))
    if not args.stations_only:
        ingestor.ingest_measurements(sensors)


def cmd_weather(args: argparse.Namespace) -> None:
    from datetime import date

    from aq_lakehouse.ingestion.open_meteo import OpenMeteoIngestor, stations_for_period

    cfg, store = _store()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    meta = cfg.layer_key("bronze", "gios_archive", "metadata")
    snapshot = store.latest_partition(meta)
    points = stations_for_period(
        store.get_bytes(f"{meta}/snapshot={snapshot}/stations.csv").decode("utf-8"),
        store.get_bytes(f"{meta}/snapshot={snapshot}/sensors.csv").decode("utf-8"),
        set(cfg.section("open_meteo")["pollutants_of_interest"]),
        start,
        end,
    )
    OpenMeteoIngestor(cfg, store).ingest(points, start, end)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="aq_lakehouse")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(required=True)

    p = sub.add_parser("archive", help="load yearly GIOŚ archive into bronze")
    p.add_argument("--years", type=int, nargs="+", required=True)
    p.add_argument("--force", action="store_true", help="re-download even if cached")
    p.set_defaults(func=cmd_archive)

    p = sub.add_parser("station-metadata", help="load station/sensor reference data")
    p.set_defaults(func=cmd_station_metadata)

    p = sub.add_parser("api", help="current stations + last 3 days of measurements from the API")
    p.add_argument("--stations-only", action="store_true")
    p.set_defaults(func=cmd_api)

    p = sub.add_parser("weather", help="hourly weather for PM stations")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.set_defaults(func=cmd_weather)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args.func(args)


if __name__ == "__main__":
    main()
