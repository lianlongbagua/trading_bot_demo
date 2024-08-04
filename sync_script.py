import time
import logging

import okx.MarketData as MarketData
from Core import utils
from Core.datas import Contract

def fetch_data(instrument_info):
    instId = instrument_info['instId']
    intervals = instrument_info['intervals']
    limit = instrument_info['limit']

    print("fetching data")
    
    # for feeding data into data_dict
    global data_dict

    start = time.time()
    for interval in intervals:
        try:
            raw_result = marketDataAPI.get_candlesticks(
                instId=instId, bar=interval, limit=limit
            )

            result_df = utils.list_to_df(raw_result["data"])
            result_df.attrs["symbol"] = instId

            contract = Contract.from_dataframe(result_df)

            data_dict[interval] = contract
        except Exception as e:
            logging.error(f"Error fetching data for interval {interval}: {str(e)}")
            raise
    end = time.time()
    print(f"Fetched data for {len(data_dict)} intervals in {end - start:.2f} seconds")

instrument_info = utils.load_configs("bot_config.toml")["instrument_info"]

data_dict = {}
marketDataAPI = MarketData.MarketAPI(debug="False")

fetch_data(instrument_info)