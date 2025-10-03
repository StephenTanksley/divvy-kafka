import pytest
from unittest.mock import MagicMock
from divvy_producer import produce_station_status_records


def test_produce_station_status_records_sends_data(monkeypatch):
    """Should call producer.send for each station in API response."""
    fake_stations = [{"station_id": "123"}, {"station_id": "456"}]

    class FakeResponse:
        def json(self):
            return {"data": {"stations": fake_stations}}

    mock_producer = MagicMock()
    monkeypatch.setattr("requests.get", lambda url: FakeResponse())

    produce_station_status_records(station_status_producer=mock_producer)

    assert mock_producer.send.call_count == len(fake_stations)
    mock_producer.send.assert_any_call(topic="station-status", value=fake_stations[0])
    mock_producer.send.assert_any_call(topic="station-status", value=fake_stations[-1])


def test_produce_station_status_records_empty(monkeypatch):
    """Should not crash when station list is empty."""
    class FakeResponse:
        def json(self):
            return {"data": {"stations": []}}

    mock_producer = MagicMock()
    monkeypatch.setattr("requests.get", lambda url: FakeResponse())

    produce_station_status_records(station_status_producer=mock_producer)

    mock_producer.send.assert_not_called()


def test_produce_station_status_records_invalid_json(monkeypatch):
    """Should raise KeyError if schema is wrong (missing data)."""
    class FakeResponse:
        def json(self):
            return {"test_key": "test_value"}

    mock_producer = MagicMock()
    monkeypatch.setattr("requests.get", lambda url: FakeResponse())

    with pytest.raises(KeyError):
        produce_station_status_records(station_status_producer=mock_producer)


def test_producer_send_failure(monkeypatch):
    """Should propagate exceptions from Kafka producer."""
    class FakeResponse:
        def json(self):
            return {"data": {"stations": [{"station_id": "123"}]}}

    def fake_send(*args, **kwargs):
        raise RuntimeError("Kafka error")

    mock_producer = MagicMock()
    mock_producer.send.side_effect = fake_send
    monkeypatch.setattr("requests.get", lambda url: FakeResponse())

    with pytest.raises(RuntimeError):
        produce_station_status_records(station_status_producer=mock_producer)
