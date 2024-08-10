#!/usr/bin/env python3

import time
import logging
from functools import wraps
import httpx
import random

import okx.MarketData as MarketData
import schedule
import requests

from Core.datas import Contract
from Core.custom_factors import *  # noqa
from Core import utils


logging.basicConfig(
    filename="signals.log", level=logging.INFO, format="%(asctime)s - %(message)s"
)


# Retry decorator with exponential backoff
def retry_with_backoff(retries=3, backoff_in_seconds=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (
                    httpx.ReadTimeout,
                    httpx.ConnectTimeout,
                    httpx.HTTPStatusError,
                    requests.RequestException,
                ) as e:
                    if x == retries:
                        logging.error(f"All retries failed: {str(e)}")
                        raise
                    sleep = backoff_in_seconds * 2**x + random.uniform(0, 1)
                    logging.warning(
                        f"Attempt {x + 1} failed: {str(e)}. Retrying in {sleep:.2f} seconds"
                    )
                    time.sleep(sleep)
                    x += 1

        return wrapper

    return decorator


@retry_with_backoff(retries=3, backoff_in_seconds=1)
def fetch_data(instrument_info):
    instId = instrument_info["instId"]
    intervals = instrument_info["intervals"]
    limit = instrument_info["limit"]

    print("fetching data")

    # for feeding data into data_dict
    global data_dict

    for interval in intervals:
        try:
            raw_result = marketDataAPI.get_candlesticks(
                instId=instId, bar=interval, limit=limit
            )

            result_df = utils.list_to_df(raw_result["data"])
            result_df.attrs["symbol"] = instId

            contract = Contract.from_dataframe(result_df)
            print(contract.datetime[-5:])

            data_dict[interval] = contract
        except Exception as e:
            logging.error(f"Error fetching data for interval {interval}: {str(e)}")
            raise


logging.info("bot started")

print("loading strategies")
config = utils.load_configs("bot_config.toml")

strategy_configs = config["strategies"]
instrument_info = config["instrument_info"]
instId = instrument_info["instId"]
marketDataAPI = MarketData.MarketAPI(debug=False)
push_url = config["push_url"]
data_dict = {}

# Dict for storing strategies (MAOverlap, RSICrossover, etc..)
strategies = {}

# load all available strategies
for strategy in strategy_configs:
    signal_generators = []
    for param in strategy_configs[strategy]:
        sg = eval(strategy + "()")
        sg.set_params(param)
        signal_generators.append(sg)
    strategies[strategy] = signal_generators
    print(f"{len(strategies[strategy])} {strategy} strategies loaded")


def job():
    try:
        fetch_data(instrument_info)

        for strategy in strategies:
            final_signals = []

            for sg in strategies[strategy]:
                signal = sg.generate_signals(data_dict[sg.interval])[-1]
                logging.info(f"signal: {signal} from {sg}")
                final_signals.append(signal)

            output = utils.combine_signals(final_signals)

            # logging.info(f"{strategy} total signals: {final_signals}")
    except Exception as e:
        logging.error(f"Error in job execution: {str(e)}")
        utils.push_to_device(push_url, "ERROR OCCURRED", str(e))


if __name__ == "__main__":
    schedule.every(1).minute.do(job)

    print("starting job")
    job()

    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            time.sleep(30)
