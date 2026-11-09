from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from aq_lakehouse.config import S3Settings

log = logging.getLogger(__name__)


class ObjectStore:
    def __init__(self, settings: S3Settings, client: Any | None = None) -> None:
        self.bucket = settings.bucket
        self._client = client or boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            log.info("Creating bucket %s", self.bucket)
            self._client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, data: bytes, content_type: str | None = None) -> None:
        extra = {"ContentType": content_type} if content_type else {}
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)
        log.debug("Wrote s3://%s/%s (%d bytes)", self.bucket, key, len(data))

    def put_json(self, key: str, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.put_bytes(key, body, content_type="application/json")

    def get_bytes(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def get_json(self, key: str) -> Any:
        return json.loads(self.get_bytes(key))

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def latest_partition(self, prefix: str, name: str = "snapshot") -> str:
        values = {
            part.split("=", 1)[1]
            for key in self.list_keys(prefix)
            for part in key.split("/")
            if part.startswith(f"{name}=")
        }
        if not values:
            raise FileNotFoundError(f"No {name}= partitions under {prefix}")
        return max(values)

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]
