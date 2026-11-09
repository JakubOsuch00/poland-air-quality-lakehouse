from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


@dataclass(frozen=True)
class S3Settings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str

    @classmethod
    def from_env(cls) -> S3Settings:
        return cls(
            endpoint=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
            access_key=os.getenv("S3_ACCESS_KEY", "lakehouse"),
            secret_key=os.getenv("S3_SECRET_KEY", "lakehouse-secret"),
            bucket=os.getenv("S3_BUCKET", "lakehouse"),
        )


@dataclass(frozen=True)
class PipelineConfig:
    raw: dict[str, Any]
    s3: S3Settings = field(default_factory=S3Settings.from_env)

    def section(self, name: str) -> dict[str, Any]:
        return self.raw[name]

    def layer_uri(self, layer: str, *parts: str) -> str:
        prefix = self.raw["storage"][f"{layer}_prefix"]
        path = "/".join(p.strip("/") for p in (prefix, *parts) if p)
        return f"s3a://{self.s3.bucket}/{path}"

    def layer_key(self, layer: str, *parts: str) -> str:
        prefix = self.raw["storage"][f"{layer}_prefix"]
        return "/".join(p.strip("/") for p in (prefix, *parts) if p)


@lru_cache(maxsize=1)
def load_config(path: str | os.PathLike | None = None) -> PipelineConfig:
    config_path = Path(path or os.getenv("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH))
    with config_path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return PipelineConfig(raw=raw)
