from __future__ import annotations

import argparse
import logging

from aq_lakehouse.config import PipelineConfig, load_config
from aq_lakehouse.spark.tables import Tables
from aq_lakehouse.storage import ObjectStore


def setup(description: str, add_args=None) -> tuple[argparse.Namespace, PipelineConfig, Tables]:
    parser = argparse.ArgumentParser(description=description)
    if add_args:
        add_args(parser)
    args = parser.parse_args()
    for name in ("years", "dates", "starts"):
        if getattr(args, name, None):
            values = split_list(getattr(args, name))
            setattr(args, name, [int(v) for v in values] if name == "years" else values)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    cfg = load_config()
    return args, cfg, Tables(cfg)


def split_list(values: list[str]) -> list[str]:
    # both "--years 2022 2023" and "--years 2022,2023" (easier from airflow templates)
    return [v for raw in values for v in raw.replace(",", " ").split() if v]


def object_store(cfg: PipelineConfig) -> ObjectStore:
    return ObjectStore(cfg.s3)


def latest_snapshot(store: ObjectStore, prefix: str) -> str:
    return store.latest_partition(prefix, "snapshot")
