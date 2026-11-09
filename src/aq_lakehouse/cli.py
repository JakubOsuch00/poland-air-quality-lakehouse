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

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args.func(args)


if __name__ == "__main__":
    main()
