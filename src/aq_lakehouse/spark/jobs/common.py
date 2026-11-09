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
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    cfg = load_config()
    return args, cfg, Tables(cfg)


def object_store(cfg: PipelineConfig) -> ObjectStore:
    return ObjectStore(cfg.s3)


def latest_snapshot(store: ObjectStore, prefix: str) -> str:
    snapshots = {
        part.split("=", 1)[1]
        for key in store.list_keys(prefix)
        for part in key.split("/")
        if part.startswith("snapshot=")
    }
    if not snapshots:
        raise RuntimeError(f"No snapshots under {prefix}")
    return max(snapshots)
