#!/usr/bin/env python3

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Any, List
import os

import schedule
from dotenv import load_dotenv
import numpy as np

from gateways.base import BaseGateway
from gateways.okx_gateway import OKXGateway
from Core.signal_generator import SignalGenerator
from Core.custom_factors import *  # noqa
from Core import utils
from Core.logger import LoggedClass


class SignalManager(LoggedClass):
    def __init__(self, strategy_configs: Dict[str, List[Dict[str, Any]]]):
        super().__init__(__name__)
        load_dotenv()
        self.PUSH_URL = os.environ.get("PUSH_URL")
        self.strategies = self.load_strategies(strategy_configs)
        self.all_signals = {}
        self.final_signal = 0

        # for synchronization in case api calls unreliable
        self.last_complete_time = {interval: None for interval in self.intervals}
        self.max_delay = {
            "1m": np.timedelta64(1, "m"),
            "3m": np.timedelta64(3, "m"),
            "5m": np.timedelta64(5, "m"),
            "15m": np.timedelta64(15, "m"),
            "30m": np.timedelta64(30, "m"),
            "1H": np.timedelta64(1, "h"),
        }

    def is_data_complete(self, contracts, instId: str):
        current_time = np.datetime64(datetime.now(timezone.utc).replace(tzinfo=None))

        for interval in self.intervals:
            key = f"{instId}_{interval}"
            latest_data_time = contracts[key].datetime[-1]
            self.logger.info(f"Latest data time for {interval}: {latest_data_time}")

            if (current_time - latest_data_time) > self.max_delay[interval]:
                self.logger.warning(
                    f"Data for {interval} is not up to date. Latest: {latest_data_time}"
                )
                return False

        return True

    def load_strategies(
        self, strategy_configs: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[SignalGenerator]]:
        strategies = {}

        for strategy_name, params_list in strategy_configs.items():
            signal_generators = []
            for params in params_list:
                sg = eval(strategy_name + "()")
                sg.set_params(params)
                signal_generators.append(sg)
            strategies[strategy_name] = signal_generators

        strategy_len = sum(len(v) for v in strategies.values())
        self.logger.info(f"Loaded {strategy_len} strategies")
        utils.push_to_device(
            self.PUSH_URL, "Strategies Loaded", f"Loaded {strategy_len} strategies"
        )

        # save all intervals as set
        self.intervals = set([sg.interval for sgs in strategies.values() for sg in sgs])
        self.logger.info(f"Strategy Intervals: {self.intervals}")

        return strategies

    def generate_signals(
        self, contracts: Dict[str, Any], instId: str
    ) -> Dict[str, Any]:
        if not self.is_data_complete(contracts, instId):
            self.logger.warning("Data incomplete, skipping signal generation")
            self.all_signals = {}
            return

        for strategy_name, signal_generators in self.strategies.items():
            strategy_signals = []
            for sg in signal_generators:
                contract_key = f"{instId}_{sg.interval}"
                signal = sg.generate_signals(contracts[contract_key])[-1]
                strategy_signals.append(signal)
                self.logger.info(
                    f"{sg.get_params()} - Signal: {signal} for {contract_key}"
                )

            combined_signal = self.combine_signals(strategy_signals)
            self.all_signals[strategy_name] = {
                "individual_signals": strategy_signals,
                "combined_signal": combined_signal,
            }

    @staticmethod
    def combine_signals(signals: List[float]) -> float:
        # You can implement different signal combination methods here
        # For now, we'll use a simple sum
        return sum(signals) / len(signals) if signals else 0

    def get_final_signal(self, contracts: Dict[str, Any], instId: str) -> float:
        self.generate_signals(contracts, instId)

        if not self.all_signals:
            return

        all_signals = []
        for strategy_signals in self.all_signals.values():
            all_signals.extend(strategy_signals["individual_signals"])
        new_final_signal = self.combine_signals(all_signals)

        # Only push to device if the final signal has changed
        if new_final_signal != self.final_signal:
            self.logger.info(f"New final signal: {new_final_signal}")
            self.final_signal = new_final_signal
            utils.push_to_device(
                self.PUSH_URL, "New Signal Generated", str(new_final_signal)
            )

        self.logger.info(f"Final signal across all strategies: {self.final_signal}")


class TradingSystem:
    def __init__(self, config_path: str):
        self.configs = utils.load_configs(config_path)
        self.strategy_configs = self.configs["strategies"]
        self.instrument_info = self.configs["instrument_info"]
        self.gateway = self.initialize_gateway()
        self.signal_manager = SignalManager(self.strategy_configs)

    def initialize_gateway(self) -> BaseGateway:
        gateway_type = self.configs.get("gateway", "OKX")
        try:
            return eval(gateway_type + "Gateway")()
        except NameError:
            raise ValueError(f"Unsupported gateway type: {gateway_type}")

    async def get_data(self) -> Dict[str, Any]:
        instId = self.instrument_info["instId"]
        bars = self.instrument_info["bars"]
        limit = self.instrument_info["limit"]
        results = await self.gateway.fetch_multiple(instId, bars, limit)
        return await self.gateway.process_multiple(results)

    def run_get_data(self) -> Dict[str, Any]:
        return asyncio.run(self.get_data())

    def job(self):
        data = self.run_get_data()
        self.signal_manager.get_final_signal(data, self.instrument_info["instId"])

    @staticmethod
    def process_signals(signals: Dict[str, Any]):
        # Implement your logic to process and act on the signals
        print("Processing signals:", signals)
        # For example, you might want to place orders based on the signals

    def run(self):
        schedule.every(1).minute.do(self.job)
        self.job()  # Run once immediately

        while True:
            schedule.run_pending()
            time.sleep(5)


if __name__ == "__main__":
    trading_system = TradingSystem("bot_config.toml")
    trading_system.run()
