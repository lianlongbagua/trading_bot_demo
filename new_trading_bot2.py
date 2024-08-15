#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import os
import time

from dotenv import load_dotenv
import numpy as np
import requests
from tenacity import retry, wait_random_exponential

import gateways
from Core import utils
from Core.strategy_factory import create_strategy
from Core.logger import LoggedClass


class SignalManager(LoggedClass):
    def __init__(self, strategy_configs):
        super().__init__(__name__)
        # organized by resample "bar_interval" key
        self.strategy_dict = asyncio.run(self.load_strategies(strategy_configs))
        strategy_len = sum(len(v) for v in self.strategy_dict.values())
        self.logger.info(f"Loaded {strategy_len} strategies")

        self.final_signal = 0

    @staticmethod
    async def _load_sg(name, params):
        sg = create_strategy(name)
        return sg.set_params(params, return_self=True)

    @staticmethod
    def _load_name_params(strategy_configs):
        for name, params in strategy_configs.items():
            for p in params:
                yield name, p

    async def load_strategies(self, strategy_configs):
        tasks = [
            self._load_sg(name, params)
            for name, params in self._load_name_params(strategy_configs)
        ]
        strategies = await asyncio.gather(*tasks)

        strategy_dict = {}
        for sg in strategies:
            resample_key = sg.resample

            if resample_key == 60:
                resample_key = "1H"
            else:
                resample_key = f"{resample_key}m"

            if resample_key not in strategy_dict:
                strategy_dict[resample_key] = []
            strategy_dict[resample_key].append(sg)

        return strategy_dict

    @staticmethod
    async def _gen_signal(sg, data):
        if sg.resample != data.interval:
            raise
        return sg.generate_signals(data)[-1]

    @staticmethod
    def combine_signals(signals):
        return sum(signals) / len(signals) if signals else 0

    async def generate_signals(self, contracts):
        signals = []

        tasks = []
        for bar_interval, sgs in self.strategy_dict.items():
            if bar_interval in contracts:
                contract = contracts[bar_interval]
                for sg in sgs:
                    tasks.append(self._gen_signal(sg, contract))

        signals = await asyncio.gather(*tasks)
        new_signal = self.combine_signals(signals)
        signal_changed = self.signal_has_changed(new_signal)
        self.final_signal = new_signal
        self.logger.info(f"Final Signal: {self.final_signal}")
        return self.final_signal, signal_changed

    def signal_has_changed(self, new_signal):
        return new_signal != self.final_signal


def calculate_pos_size(max_risk, atr, signal_strength):
    return max_risk / (atr * signal_strength)


def push_to_device(url, title, content):
    # small clever recursion
    if isinstance(url, list):
        for u in url:
            push_to_device(u, title, content)
        return
    url = url + f"/{title}/{content}"
    requests.post(url)


class TradingSystem(LoggedClass):
    def __init__(self, config_path: str):
        super().__init__(__name__)

        self.configs = utils.load_configs(config_path)

        self.instrument_info = self.configs["instrument_info"]
        self.gateway = self.initialize_gateway()
        self.contracts = {
            bar_interval: None for bar_interval in self.instrument_info["bars"]
        }

        self.strategy_configs = self.configs["strategies"]
        self.signal_manager = SignalManager(self.strategy_configs)

        self.risk_info = self.configs["risk_info"]
        self.pos_size = None

        load_dotenv()
        self.push_url = os.environ.get("PUSH_URL")

        self.first_run = True

    def initialize_gateway(self) -> gateways.BaseGateway:
        gateway_type = self.configs.get("gateway")
        if gateway_type == "OKX":
            return gateways.OKXGateway()

    @retry(wait=wait_random_exponential(multiplier=1), reraise=True)
    async def fetch_and_process_data(self, bar_interval: str) -> Dict[str, Any]:
        instId = self.instrument_info["instId"]
        limit = self.instrument_info["limit"]
        polled_contract = await self.gateway.fetch_and_process(
            instId, bar_interval, limit
        )
        self.contracts[bar_interval] = polled_contract

        try:
            if bar_interval == self.risk_info["bar_interval"]:
                atr = polled_contract.atr(int(self.risk_info["bar_interval"][:-1]))[-1]
                signal_strength = self.signal_manager.final_signal
                if self.first_run:
                    signal_strength = 1
                self.pos_size = calculate_pos_size(
                    self.risk_info["max_risk"], atr, signal_strength
                )
                self.logger.info(f"Position Size: {self.pos_size}")
        except Exception as e:
            self.logger.error(f"Failed to calculate position size: {str(e)}")

        if not self.is_data_complete(polled_contract) and not self.first_run:
            self.logger.warning(f"Data for {bar_interval} is incomplete, retrying...")
            raise

    def should_fetch_bars(self) -> List:
        bar_intervals_to_fetch = []
        now_minute = datetime.now(timezone.utc).minute

        if now_minute == 0:
            now_minute = 60

        for bar_interval, contract in self.contracts.items():
            if contract is None:
                bar_intervals_to_fetch.append(bar_interval)

            else:
                interval_number = int(bar_interval[:-1])
                if bar_interval[-1] == "H":
                    interval_number *= 60

                if now_minute % interval_number == 0:
                    bar_intervals_to_fetch.append(bar_interval)

        return bar_intervals_to_fetch

    def is_data_complete(self, contract):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        last_candle_time = contract.datetime[-1].astype(datetime)
        max_delay = timedelta(minutes=contract.interval)
        complete = now - last_candle_time < max_delay
        return complete

    async def job(self):
        bar_intervals_to_fetch = self.should_fetch_bars()
        tasks = [
            self.fetch_and_process_data(bar_interval)
            for bar_interval in bar_intervals_to_fetch
        ]
        self.logger.info(f"Fetching data for {bar_intervals_to_fetch}")
        await asyncio.gather(*tasks)
        final_signal, has_changed = await self.signal_manager.generate_signals(
            self.contracts
        )
        self.first_run = False

        if has_changed:
            push_to_device(self.push_url, "Trading Bot", f"Signal: {final_signal}")
            self.logger.warning(f"Signal strength changed to {final_signal}")

        time.sleep(1)

    async def run(self):
        # run once immediately
        self.logger.info("Starting main loop")
        await self.job()
        push_to_device(self.push_url, "Trading Bot", "Started")

        while True:
            now_seconds = datetime.now(timezone.utc).second
            sleep_seconds = (20 - now_seconds) % 60
            await asyncio.sleep(sleep_seconds)
            await self.job()

    def start(self):
        asyncio.run(self.run())


if __name__ == "__main__":
    trading_system = TradingSystem("bot_config.toml")
    trading_system.start()
