import pytest
import requests

from aq_lakehouse.ingestion import gios_api
from aq_lakehouse.ingestion.gios_api import GiosApiClient

BASE = "https://api.example/rest"


@pytest.fixture
def client():
    return GiosApiClient(BASE, page_size=2, min_interval=0)


def test_paginates_until_total_pages(client, requests_mock):
    url = f"{BASE}/station/findAll"
    requests_mock.get(
        url,
        [
            {"json": {gios_api.STATIONS_KEY: [{"id": 1}, {"id": 2}], "totalPages": 2}},
            {"json": {gios_api.STATIONS_KEY: [{"id": 3}], "totalPages": 2}},
        ],
    )
    assert [s["id"] for s in client.stations()] == [1, 2, 3]
    assert [r.qs["page"] for r in requests_mock.request_history] == [["0"], ["1"]]


def test_manual_sensor_returns_none(client, requests_mock):
    requests_mock.get(f"{BASE}/data/getData/5", status_code=400, json={"error_result": "x"})
    assert client.sensor_data(5) is None


def test_other_client_errors_raise(client, requests_mock):
    requests_mock.get(f"{BASE}/data/getData/5", status_code=404)
    with pytest.raises(requests.HTTPError):
        client.sensor_data(5)
