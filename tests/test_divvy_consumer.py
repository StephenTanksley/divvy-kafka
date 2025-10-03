import pytest
import pandas as pd
from unittest.mock import MagicMock
from divvy_consumer import (
    get_last_offset_from_topic,
    get_station_info,
    get_station_status,
    impute_zero,
    enrich_with_cap_ratio,
    enrich_with_color
)


# Test to make sure that Kafka polling works correctly
def test_get_last_offset_returns(monkeypatch):
    mock_consumer = MagicMock()
    mock_consumer.end_offsets.return_value = {MagicMock(): 99}
    assert get_last_offset_from_topic("station-status", mock_consumer) == 99


def test_get_last_offset_empty(monkeypatch):
    mock_consumer = MagicMock()
    mock_consumer.end_offsets.return_value = {}
    with pytest.raises(IndexError):
        get_last_offset_from_topic("station-status", mock_consumer)


# Test to make sure an empty path raises appropriate error
def test_get_station_info_raises_for_empty_path():
    with pytest.raises(ValueError):
        get_station_info("")


# Test Kafka polling
def test_get_station_status_with_records():
    mock_consumer = MagicMock()
    mock_msg = MagicMock()
    mock_msg.value = {"station_id": "abc"}
    mock_consumer.poll.return_value = {0: [mock_msg]}

    df = get_station_status(mock_consumer)
    assert not df.empty
    assert df.iloc[0]["station_id"] == "abc"


def test_get_station_status_empty_poll():
    mock_consumer = MagicMock()
    mock_consumer.poll.return_value = {}
    df = get_station_status(mock_consumer)
    assert df.empty


# Test enrichment functions
@pytest.mark.parametrize("input_val,expected", [(None, 0), ("null", 0), (5, 5)])
def test_impute_zero(input_val, expected):
    assert impute_zero(input_val) == expected


def test_enrich_with_cap_ratio_division_and_zero():
    bikes = pd.Series([0, 3])
    capacity = pd.Series([0, 6])
    result = enrich_with_cap_ratio(bikes, capacity)
    assert result == [0, 0.5]


def test_enrich_with_color_thresholds():
    series = pd.Series([0.2, 0.5, 0.8])
    result = enrich_with_color(series)
    assert result == ["#0d466c", "#298538", "#d61437"]
