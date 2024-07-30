#!/usr/bin/env python3

import ast
import time
import toml
import logging

import okx.MarketData as MarketData
import pandas as pd
import numpy as np
import schedule

from SignalFactory import MAOverlap, RSICrossover
from DataHandling import Contract


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


signal_generators = []
ma_sgs = []
rsi_sgs = []

logging.info("bot started")

print("loading selected params")
with open("selected_params.toml") as f:
    selected_params = toml.load(f)["selected_params"]


for strategy in selected_params:
    if "ma" == strategy:
        sg = MAOverlap()
        for param in ast.literal_eval(selected_params[strategy]):
            sg.set_params(param)
            signal_generators.append(sg)
            ma_sgs.append(sg)
        print("ma strategy loaded")

    if "rsi" == strategy:
        sg = RSICrossover()
        for param in ast.literal_eval(selected_params[strategy]):
            sg.set_params(param)
            signal_generators.append(sg)
            rsi_sgs.append(sg)
        print("rsi strategy loaded")

print(f"loaded {len(signal_generators)} signal generators")


instId = "BTC-USDT"
marketDataAPI = MarketData.MarketAPI(debug="False")
data_dict = {}


def fetch_data():
    global data_dict
    intervals = ["1m", "3m", "5m", "15m", "30m", "1H"]

    print("fetching data")

    for interval in intervals:
        raw_result = marketDataAPI.get_candlesticks(
            instId=instId, bar=interval, limit=300
        )

        result_df = list_to_df(raw_result["data"])
        result_df.attrs["symbol"] = instId

        contract = Contract.from_dataframe(result_df)

        data_dict[interval] = contract


def job():
    fetch_data()

    final_signals = []
    final_ma_signals = []
    final_rsi_signals = []

    for sg in signal_generators:
        signal = sg.generate_signals(data_dict[sg.interval])[-1]
        logging.info(f" signal: {signal} " + str(sg))
        final_signals.append(signal)
    
    for sg in ma_sgs:
        signal = sg.generate_signals(data_dict[sg.interval])[-1]
        final_ma_signals.append(signal)
    ma_output = combine_signals(final_ma_signals)
    logging.info(f"combined ma signals: {ma_output}")
    
    for sg in rsi_sgs:
        signal = sg.generate_signals(data_dict[sg.interval])[-1]
        final_rsi_signals.append(signal)
    rsi_output = combine_signals(final_rsi_signals)
    logging.info(f"combined rsi signals: {rsi_output}")

    print(time.ctime())
    print(f"final signals: {final_signals}")
    print(f"combined ma signal: {ma_output}")
    print(f"combined rsi signal: {ma_output}")


schedule.every(1).minute.do(job)


print("starting job")
job()

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
