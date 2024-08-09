import asyncio
from typing import Dict
import logging
import time
import os

import okx.MarketData as MarketData
from tenacity import retry, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

from Core import utils
from Core.datas import Contract
from Core.logger import setup_logging


# Set up logging
logger = setup_logging("async_app.log")

logging.getLogger("httpx").setLevel(logging.CRITICAL)


# Define custom exceptions
class FetchError(Exception):
    pass


class ProcessingError(Exception):
    pass


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(FetchError),
    reraise=True,
)
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
        raise FetchError(f"Failed to fetch data: {str(e)}") from e


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(ProcessingError),
    reraise=True,
)
async def convert_to_Contract(raw_result: Dict, interval: str, instId: str) -> tuple:
    # Wrap the processing in an asynchronous function
    try:
        df = await asyncio.to_thread(utils.list_to_df, raw_result["data"])
        df.attrs["symbol"] = instId
        contract = await asyncio.to_thread(Contract.from_dataframe, df)
        return interval, contract
    except Exception as e:
        logger.error(
            f"Error processing data for {instId} at interval {interval}: {str(e)}"
        )
        raise ProcessingError(f"Failed to process data: {str(e)}") from e


async def fetch_data(instrument_info: Dict) -> Dict[str, Contract]:
    instId = instrument_info["instId"]
    intervals = instrument_info["intervals"]
    limit = instrument_info["limit"]

    print("fetching data")

    data_dict = {}
    start_time = time.time()

    # Create tasks for each interval
    tasks = [fetch_interval_data(instId, interval, limit) for interval in intervals]

    try:
        # Run all tasks concurrently and wait for them to complete
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process the fetched data
        process_tasks = [
            convert_to_Contract(raw_result, interval, instId)
            for raw_result, interval in zip(raw_results, intervals)
            if not isinstance(raw_result, Exception)
        ]
        processed_results = await asyncio.gather(*process_tasks, return_exceptions=True)

        data_dict = {
            interval: contract
            for (interval, contract) in processed_results
            if not isinstance(contract, Exception)
        }
    except Exception as e:
        logger.error(f"Unexpected error during data fetching and processing: {str(e)}")
        raise

    end_time = time.time()

    logger.info(
        f"Fetched data for {len(data_dict)} intervals in {end_time - start_time:.2f} seconds"
    )
    return data_dict


async def main(instrument_info):
    try:
        data = await fetch_data(instrument_info)
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")


if __name__ == "__main__":
    try:
        load_dotenv()
        config = utils.load_configs("bot_config.toml")
        instrument_info = config["instrument_info"]
        marketDataAPI = MarketData.MarketAPI(debug=False)
        push_url = os.environ.get("PUSH_URL")

        utils.push_to_device(push_url, "Application status", "started")
        asyncio.run(main(instrument_info))
    except Exception as e:
        logger.critical(f"Application failed to start: {str(e)}")
        utils.push_to_device(push_url, "Application failed to start", "Error")
