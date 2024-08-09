import pytest
import asyncio
from unittest.mock import Mock, patch
from ..async_script import fetch_data, FetchError, ProcessingError

# Replace 'your_module_name' with the actual name of your Python file containing the fetch_data function


@pytest.fixture
def mock_market_api():
    with patch("your_module_name.marketDataAPI") as mock_api:
        yield mock_api


@pytest.fixture
def instrument_info():
    return {"instId": "BTC-USDT", "intervals": ["1m", "5m"], "limit": 100}


@pytest.mark.asyncio
async def test_fetch_data_success(mock_market_api, instrument_info):
    # Mock successful API responses
    mock_market_api.get_candlesticks.side_effect = [
        {"data": [["timestamp", "open", "high", "low", "close", "volume"]]},
        {"data": [["timestamp", "open", "high", "low", "close", "volume"]]},
    ]

    with patch("your_module_name.utils.list_to_df", return_value=Mock()):
        with patch("your_module_name.Contract.from_dataframe", return_value=Mock()):
            result = await fetch_data(instrument_info)

    assert len(result) == 2
    assert "1m" in result
    assert "5m" in result


@pytest.mark.asyncio
async def test_fetch_data_partial_failure(mock_market_api, instrument_info):
    # Mock one successful response and one failure
    mock_market_api.get_candlesticks.side_effect = [
        {"data": [["timestamp", "open", "high", "low", "close", "volume"]]},
        FetchError("API error"),
    ]

    with patch("your_module_name.utils.list_to_df", return_value=Mock()):
        with patch("your_module_name.Contract.from_dataframe", return_value=Mock()):
            result = await fetch_data(instrument_info)

    assert len(result) == 1
    assert "1m" in result


@pytest.mark.asyncio
async def test_fetch_data_processing_error(mock_market_api, instrument_info):
    # Mock successful API responses but processing error
    mock_market_api.get_candlesticks.return_value = {
        "data": [["timestamp", "open", "high", "low", "close", "volume"]]
    }

    with patch(
        "your_module_name.utils.list_to_df",
        side_effect=ProcessingError("Processing failed"),
    ):
        result = await fetch_data(instrument_info)

    assert len(result) == 0


@pytest.mark.asyncio
async def test_fetch_data_complete_failure(mock_market_api, instrument_info):
    # Mock all API calls failing
    mock_market_api.get_candlesticks.side_effect = FetchError("API error")

    with pytest.raises(Exception):
        await fetch_data(instrument_info)


@pytest.mark.asyncio
async def test_fetch_data_retry_success(mock_market_api, instrument_info):
    # Mock API call failing twice then succeeding
    mock_market_api.get_candlesticks.side_effect = [
        FetchError("API error"),
        FetchError("API error"),
        {"data": [["timestamp", "open", "high", "low", "close", "volume"]]},
    ]

    with patch("your_module_name.utils.list_to_df", return_value=Mock()):
        with patch("your_module_name.Contract.from_dataframe", return_value=Mock()):
            result = await fetch_data(instrument_info)

    assert len(result) == 2
