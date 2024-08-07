import asyncio
from typing import Dict, List
import logging
import time

import okx.MarketData as MarketData

from Core import utils
from Core.datas import Contract
from Core.logger import setup_logging


# Set up logging
logger = setup_logging("async_app.log")

logging.getLogger("httpx").setLevel(logging.CRITICAL)


async def fetch_interval_data(instId: str, interval: str, limit: int) -> Dict:
    try:
        # Wrap the synchronous API call in an asynchronous function
        return await asyncio.to_thread(
            marketDataAPI.get_candlesticks, instId=instId, bar=interval, limit=limit
        )
    except Exception as e:
        logger.error(
            f"Error fetching data for {instId} at interval {interval}: {str(e)}"
        )
        raise


async def convert_to_Contract(raw_result: Dict, interval: str, instId: str) -> tuple:
    # Wrap the processing in an asynchronous function
    df = await asyncio.to_thread(utils.list_to_df, raw_result["data"])
    df.attrs["symbol"] = instId
    contract = await asyncio.to_thread(Contract.from_dataframe, df)
    return interval, contract


async def fetch_data(instrument_info: Dict) -> Dict[str, Contract]:
    instId = instrument_info["instId"]
    intervals = instrument_info["intervals"]
    limit = instrument_info["limit"]

    print("fetching data")

    data_dict = {}
    start_time = time.time()

    # Create tasks for each interval
    tasks = [fetch_interval_data(instId, interval, limit) for interval in intervals]

    # Run all tasks concurrently and wait for them to complete
    raw_results = await asyncio.gather(*tasks)

    # Process the fetched data
    process_tasks = [
        convert_to_Contract(raw_result, interval, instId)
        for raw_result, interval in zip(raw_results, intervals)
    ]
    processed_results = await asyncio.gather(*process_tasks)

    data_dict = dict(processed_results)

    end_time = time.time()

    logger.info(
        f"Fetched data for {len(data_dict)} intervals in {end_time - start_time:.2f} seconds"
    )
    return data_dict


async def main(instrument_info):
    data = await fetch_data(instrument_info)
    print(f"Fetched data for {len(data)} intervals")


if __name__ == "__main__":
    config = utils.load_configs("bot_config.toml")
    instrument_info = config["instrument_info"]
    marketDataAPI = MarketData.MarketAPI(debug=False)

    asyncio.run(main(instrument_info))
