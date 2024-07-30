#!/usr/bin/env python3

import ast
import time
import toml
import logging

import okx.MarketData as MarketData
import pandas as pd
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


signal_generators = []

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
        print("ma strategy loaded")

    if "rsi" == strategy:
        sg = RSICrossover()
        for param in ast.literal_eval(selected_params[strategy]):
            sg.set_params(param)
            signal_generators.append(sg)
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

    for sg in signal_generators:
        signal = sg.generate_signals(data_dict[sg.interval])[-1]
        logging.info(f" signal: {signal} " + str(sg))
        final_signals.append(signal)

    print(time.ctime())
    print(f"final signals: {final_signals}")


schedule.every(1).minute.do(job)


print("starting job")
job()

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
