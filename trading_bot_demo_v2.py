import pytest
import pandas as pd
from unittest.mock import Mock, patch
from your_main_script import DataFetcher, MarketAPI

@pytest.fixture
def mock_market_api():
    return Mock(spec=MarketAPI)

@pytest.fixture
def data_fetcher(mock_market_api):
    return DataFetcher(mock_market_api)

def test_list_to_df():
    data = [
        [1625097600000, "35000", "35100", "34900", "35050", "1000", "0", "0", "0"],
        [1625097700000, "35050", "35150", "34950", "35100", "1100", "0", "0", "0"]
    ]
    df = DataFetcher.list_to_df(data)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume']
    assert df.index.name == 'time'
    assert df.index[0] == pd.Timestamp('2021-07-01 00:00:00')

@pytest.mark.asyncio
async def test_fetch_data(data_fetcher, mock_market_api):
    instrument_info = {
        'instId': 'BTC-USD-SWAP',
        'intervals': ['1m', '5m'],
        'limit': 100
    }
    
    mock_response = {
        "data": [
            [1625097600000, "35000", "35100", "34900", "35050", "1000", "0", "0", "0"],
            [1625097700000, "35050", "35150", "34950", "35100", "1100", "0", "0", "0"]
        ]
    }
    
    mock_market_api.get_candlesticks.return_value = mock_response
    
    data_dict = await data_fetcher.fetch_data(instrument_info)
    
    assert len(data_dict) == 2
    assert '1m' in data_dict
    assert '5m' in data_dict
    assert mock_market_api.get_candlesticks.call_count == 2

@pytest.mark.asyncio
async def test_fetch_data_error(data_fetcher, mock_market_api):
    instrument_info = {
        'instId': 'BTC-USD-SWAP',
        'intervals': ['1m'],
        'limit': 100
    }
    
    mock_market_api.get_candlesticks.side_effect = Exception("API Error")
    
    with pytest.raises(Exception):
        await data_fetcher.fetch_data(instrument_info)