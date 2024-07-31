#!/usr/bin/env python3

import ast
import time
import toml
import logging

import okx.MarketData as MarketData
import pandas as pd
import schedule
import requests

from DataHandling import Contract
from SignalFactory import *


logging.basicConfig(
    filename="signals.log", level=logging.INFO, format="%(asctime)s - %(message)s"
)


def list_to_df(data):
    data = pd.DataFrame(
        data,
        columns=["time", "open", "high", "low", "close", "volume", "n1", "n2", "n3"],
    )
    data = data.drop(columns=["n1", "n2", "n3"])
    data = data.astype(float)
    data["time"] = pd.to_datetime(data["time"], unit="ms")
    data.set_index("time", inplace=True)
    data = data[::-1]
    return data


def combine_signals(signals):
    sum_s = sum(signals)
    long_thresh = len(signals) * 10 / 2
    short_thresh = len(signals) * -10 / 2
    if sum_s >= long_thresh:
        return 1
    elif sum_s <= short_thresh:
        return -1
    return 0


def load_signal_generators(strategy_name):
    signal_generators = []
    for param in ast.literal_eval(selected_params[strategy_name]):
        sg = eval(strategy_name+"()")
        sg.set_params(param)
        signal_generators.append(sg)
    return signal_generators


def push_to_device(title, content):
    url = "https://api.day.app/sTc3n8MBtjjjftx5kYPBHg"
    url = url + f"/{title}/{content}"
    requests.post(url)


def fetch_data(instrument_info):
    instId = instrument_info['instId']
    intervals = instrument_info['intervals']
    limit = instrument_info['limit']

    print("fetching data")
    
    # for feeding data into data_dict
    global data_dict

    for interval in intervals:
        raw_result = marketDataAPI.get_candlesticks(
            instId=instId, bar=interval, limit=limit
        )

        result_df = list_to_df(raw_result["data"])
        result_df.attrs["symbol"] = instId

        contract = Contract.from_dataframe(result_df)

        data_dict[interval] = contract


logging.info("bot started")

print("loading strategies")
with open("config.toml") as f:
    config = toml.load(f)

selected_params = config["selected_params"]
instrument_info = config["instrument_info"]

instId = instrument_info['instId']
marketDataAPI = MarketData.MarketAPI(debug="False")
data_dict = {}

signal_generators = []

# Dict for storing strategies (MAOverlap, RSICrossover, etc..)
strategies = {}


# load all available strategies
for strategy in selected_params:
    strategies[strategy] = load_signal_generators(strategy)
    print(f"{len(strategies[strategy])} {strategy} strategies loaded")


def job():
    fetch_data(instrument_info)
    
    for strategy in strategies:
        final_signals = []

        for sg in strategies[strategy]:
            signal = sg.generate_signals(data_dict[sg.interval])[-1]
            logging.info(f"signal: {signal} from {sg}")
            final_signals.append(signal)

        output = combine_signals(final_signals)

        logging.info(f"{strategy} total signals: {final_signals}")

        msg = f"final combined {strategy} signal: {output}"

        print(msg)
        logging.info(msg)

        if output:
            push_to_device("signal generated", f"final combined {strategy} signal: {output}")

if __name__ == "__main__":
    schedule.every(1).minute.do(job)

    print("starting job")
    job()

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)
