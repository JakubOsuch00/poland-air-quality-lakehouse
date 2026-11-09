"""GIOŚ yearly archive -> bronze.

Keeps the original zip and converts each xlsx sheet to csv + meta.json so spark can read it.
No cleaning here, that's done in silver.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

import openpyxl

from aq_lakehouse.config import PipelineConfig
from aq_lakehouse.ingestion.http import get_bytes, make_session
from aq_lakehouse.storage import ObjectStore

log = logging.getLogger(__name__)

FILENAME_RE = re.compile(r"^(?P<year>\d{4})_(?P<pollutant>.+?)_(?P<period>\d+g)\.xlsx$", re.I)

# label in first column -> attribute name in SheetHeader
HEADER_LABELS = {
    "kod stacji": "station_codes",
    "wskaźnik": "pollutants",
    "czas uśredniania": "periods",
    "jednostka": "units",
    "kod stanowiska": "sensor_codes",
    "czas pomiaru": "sensor_codes",  # 2016-2018 files label sensor codes like this
    "nr": None,
}

POLLUTANT_ALIASES = {"PM25": "PM2.5", "PM2,5": "PM2.5"}


def normalize_pollutant(name: str) -> str:
    name = name.strip()
    return POLLUTANT_ALIASES.get(name.upper(), name)


@dataclass
class SheetHeader:
    station_codes: list[str] = field(default_factory=list)
    pollutants: list[str] = field(default_factory=list)
    periods: list[str] = field(default_factory=list)
    units: list[str | None] = field(default_factory=list)
    sensor_codes: list[str | None] = field(default_factory=list)


@dataclass
class ParsedSheet:
    source_file: str
    year: int
    pollutant: str
    averaging_period: str
    columns: list[dict]
    header_rows: int
    data_rows: int = 0


def _cell_str(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe(names: Iterable[str]) -> list[str]:
    # same station can show up twice in one sheet
    seen: dict[str, int] = {}
    out = []
    for name in names:
        n = seen.get(name, 0)
        out.append(name if n == 0 else f"{name}__{n + 1}")
        seen[name] = n + 1
    return out


def parse_header(rows: Iterator[tuple]) -> tuple[SheetHeader, int, tuple | None]:
    header = SheetHeader()
    consumed = 0
    for row in rows:
        first = row[0] if row else None
        if isinstance(first, datetime):
            return header, consumed, row
        consumed += 1
        label = _cell_str(first)
        if label is None:
            continue
        attr = HEADER_LABELS.get(label.lower())
        if attr is None:
            if label.lower() not in HEADER_LABELS:
                log.warning("Unknown header row %r - ignoring", label)
            continue
        setattr(header, attr, [_cell_str(v) for v in row[1:]])
    return header, consumed, None


def sheet_to_csv(rows: Iterator[tuple], source_file: str, out: io.StringIO) -> ParsedSheet:
    match = FILENAME_RE.match(source_file.rsplit("/", 1)[-1])
    if not match:
        raise ValueError(f"Unexpected archive file name: {source_file}")

    header, header_rows, first_data_row = parse_header(rows)
    if not header.station_codes:
        raise ValueError(f"{source_file}: no 'Kod stacji' row found")

    n_cols = max(i for i, code in enumerate(header.station_codes) if code) + 1
    stations = [c or f"unknown_{i}" for i, c in enumerate(header.station_codes[:n_cols])]
    csv_columns = _dedupe(stations)

    def pick(values: list, i: int):
        return values[i] if i < len(values) else None

    columns = [
        {
            "column": csv_columns[i],
            "station_code": stations[i],
            "sensor_code": pick(header.sensor_codes, i),
            "unit": pick(header.units, i),
        }
        for i in range(n_cols)
    ]

    pollutant = normalize_pollutant(
        next((p for p in header.pollutants if p), None) or match["pollutant"]
    )
    period = next((p for p in header.periods if p), None) or match["period"]

    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["timestamp", *csv_columns])

    data_rows = 0

    def emit(row: tuple) -> None:
        nonlocal data_rows
        ts = row[0]
        if not isinstance(ts, datetime):
            return
        values = ["" if v is None else str(v) for v in row[1 : n_cols + 1]]
        values += [""] * (n_cols - len(values))
        writer.writerow([ts.isoformat(), *values])
        data_rows += 1

    if first_data_row is not None:
        emit(first_data_row)
    for row in rows:
        emit(row)

    return ParsedSheet(
        source_file=source_file,
        year=int(match["year"]),
        pollutant=pollutant,
        averaging_period=period,
        columns=columns,
        header_rows=header_rows,
        data_rows=data_rows,
    )


def wanted_files(names: Iterable[str], pollutants: set[str], periods: set[str]) -> list[str]:
    selected = []
    for name in names:
        m = FILENAME_RE.match(name.rsplit("/", 1)[-1])
        if not m:
            continue
        if normalize_pollutant(m["pollutant"]) in pollutants and m["period"].lower() in periods:
            selected.append(name)
    return sorted(selected)


class GiosArchiveIngestor:
    def __init__(self, config: PipelineConfig, store: ObjectStore) -> None:
        self.cfg = config.section("gios_archive")
        self.config = config
        self.store = store
        self.session = make_session()

    def _url(self, file_id: int) -> str:
        return f"{self.cfg['base_url']}/{file_id}"

    def raw_zip_key(self, year: int) -> str:
        return self.config.layer_key("bronze", "gios_archive", "raw", f"year={year}", f"{year}.zip")

    def fetch_year(self, year: int, force: bool = False) -> bytes:
        key = self.raw_zip_key(year)
        if not force and self.store.exists(key):
            log.info("Using cached archive for %s", year)
            return self.store.get_bytes(key)
        file_id = self.cfg["yearly_file_ids"][year]
        log.info("Downloading GIOŚ archive for %s (file id %s)", year, file_id)
        data = get_bytes(self.session, self._url(file_id))
        self.store.put_bytes(key, data, content_type="application/zip")
        return data

    def ingest_year(self, year: int, force_download: bool = False) -> list[ParsedSheet]:
        archive = zipfile.ZipFile(io.BytesIO(self.fetch_year(year, force=force_download)))
        files = wanted_files(
            archive.namelist(),
            pollutants=set(self.cfg["pollutants"]),
            periods={p.lower() for p in self.cfg["averaging_periods"]},
        )
        ingested_at = datetime.now(UTC).isoformat(timespec="seconds")
        results = []
        for name in files:
            log.info("Converting %s", name)
            wb = openpyxl.load_workbook(io.BytesIO(archive.read(name)), read_only=True)
            try:
                rows = wb[wb.sheetnames[0]].iter_rows(values_only=True)
                buf = io.StringIO()
                parsed = sheet_to_csv(rows, name, buf)
            finally:
                wb.close()

            stem = f"{parsed.pollutant}_{parsed.averaging_period}".replace(".", "")
            base = self.config.layer_key("bronze", "gios_archive", "sheets", f"year={year}")
            self.store.put_bytes(f"{base}/{stem}.csv", buf.getvalue().encode("utf-8"), "text/csv")
            self.store.put_json(
                f"{base}/{stem}.meta.json", {**asdict(parsed), "ingested_at": ingested_at}
            )
            log.info("%s: %d rows x %d stations", name, parsed.data_rows, len(parsed.columns))
            results.append(parsed)
        return results

    def ingest_station_metadata(self) -> None:
        data = get_bytes(self.session, self._url(self.cfg["metadata_file_id"]))
        snapshot = datetime.now(UTC).strftime("%Y-%m-%d")
        base = self.config.layer_key("bronze", "gios_archive", "metadata", f"snapshot={snapshot}")
        self.store.put_bytes(f"{base}/metadata.xlsx", data)

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True)
        try:
            for sheet, target in (("STACJE", "stations"), ("STANOWISKA", "sensors")):
                buf = io.StringIO()
                writer = csv.writer(buf, lineterminator="\n")
                rows = wb[sheet].iter_rows(values_only=True)
                header = next(rows)
                width = max(i for i, v in enumerate(header) if v is not None) + 1
                writer.writerow([_clean_text(v) for v in header[:width]])
                for row in rows:
                    if not any(v is not None for v in row):
                        continue
                    writer.writerow(
                        [
                            v.date().isoformat() if isinstance(v, datetime) else _clean_text(v)
                            for v in row[:width]
                        ]
                    )
                self.store.put_bytes(f"{base}/{target}.csv", buf.getvalue().encode("utf-8"))
        finally:
            wb.close()


def _clean_text(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())
