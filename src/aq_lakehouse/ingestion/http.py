from __future__ import annotations

import logging

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

USER_AGENT = "aq-lakehouse/0.1 (+https://github.com/JakubOsuch00/poland-air-quality-lakehouse)"


class RetryableHTTPError(Exception):
    pass


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


_retry = retry(
    retry=retry_if_exception_type((RetryableHTTPError, requests.ConnectionError, requests.Timeout)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(6),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)


def _check(resp: requests.Response) -> requests.Response:
    # 429 and 5xx are worth retrying, anything else 4xx is our fault
    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableHTTPError(f"{resp.status_code} for {resp.url}")
    resp.raise_for_status()
    return resp


@_retry
def get_json(session: requests.Session, url: str, params: dict | None = None, timeout: int = 30):
    return _check(session.get(url, params=params, timeout=timeout)).json()


@_retry
def get_bytes(session: requests.Session, url: str, timeout: int = 300) -> bytes:
    return _check(session.get(url, timeout=timeout)).content
