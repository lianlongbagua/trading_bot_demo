import asyncio
import time
from typing import Dict, List, Any

import schedule

from gateways.base import BaseGateway
from gateways.okx_gateway import OKXGateway
from Core import utils
from Core.custom_factors import *  # noqa
from Core.signal_generator import SignalGenerator


class TradingSystem:
    def __init__(self, config_path: str):
        self.configs = utils.load_configs(config_path)
        self.strategy_configs = self.configs["strategies"]
        self.instrument_info = self.configs["instrument_info"]
        self.gateway = self.initialize_gateway()
        self.strategies = self.load_strategies()

    def initialize_gateway(self) -> BaseGateway:
        gateway_type = self.configs.get("gateway", "OKX")
        try:
            return eval(gateway_type + "Gateway")()
        except NameError:
            raise ValueError(f"Unsupported gateway type: {gateway_type}")

    def load_strategies(self) -> Dict[str, List[SignalGenerator]]:
        strategies = {}
        for strategy_name, params_list in self.strategy_configs.items():
            signal_generators = []
            for params in params_list:
                sg = eval(strategy_name + "()")
                sg.set_params(params)
                signal_generators.append(sg)
            strategies[strategy_name] = signal_generators
            print(f"{len(signal_generators)} {strategy_name} strategies loaded")
        return strategies

    async def get_data(self) -> Dict[str, Any]:
        instId = self.instrument_info["instId"]
        bars = self.instrument_info["bars"]
        limit = self.instrument_info["limit"]
        results = await self.gateway.fetch_multiple(instId, bars, limit)
        return await self.gateway.process_multiple(results)

    def run_get_data(self) -> Dict[str, Any]:
        return asyncio.run(self.get_data())

    def generate_signals(self, contracts: Dict[str, Any]) -> Dict[str, Any]:
        signals = {}
        for strategy_name, signal_generators in self.strategies.items():
            strategy_signals = []
            for sg in signal_generators:
                contract_key = f"{self.instrument_info['instId']}_{sg.interval}"
                signal = sg.generate_signals(contracts[contract_key])[-1]
                print(f"Signal: {signal} from {sg}")
                strategy_signals.append(signal)

            combined_signal = utils.sum_signals(strategy_signals)
            signals[strategy_name] = {
                "individual_signals": strategy_signals,
                "combined_signal": combined_signal,
            }

            print(f"{strategy_name} total signals: {strategy_signals}")
            print(f"Final combined {strategy_name} signal: {combined_signal}")

        return signals

    def job(self):
        contracts = self.run_get_data()
        signals = self.generate_signals(contracts)
        # Here you can add logic to act on the signals, e.g., place orders

    def run(self):
        schedule.every(1).minute.do(self.job)
        self.job()  # Run once immediately

        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    trading_system = TradingSystem("bot_config.toml")
    trading_system.run()
