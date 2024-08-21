#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone

import os
import time

from dotenv import load_dotenv

import gateways
from trader import utils
from trader.logger import LoggedClass
from trader.managers import SignalManager, PositionManager, DataManager


class TradingSystem(LoggedClass):
    """
    convention:
    - bar_interval: int, minutes
    - only trade one instrument
    """

    def __init__(self, config_path: str):
        super().__init__(__name__)

        self.configs = utils.load_configs(config_path)
        self.instrument_info = self.configs["instrument"]

        self.gateway = self.initialize_gateway()

        self.sig_man = SignalManager(self.configs)
        self.pos_man = PositionManager(self.configs, self.gateway)
        self.data_man = DataManager(self.configs, self.gateway)


        self.push_url = os.environ.get("PUSH_URL")

        self.first_run = True

    def initialize_gateway(self) -> gateways.BaseGateway:
        gateway_type = self.configs.get("gateway")
        if gateway_type == "OKX":
            return gateways.OKXGateway()

    async def job(self):
        tasks = self.data_man.periodic_sync_tasks()
        await asyncio.gather(*tasks)

        final_signal, has_changed = await self.sig_man.generate_signals(
            self.data_man.contracts
        )
        self.first_run = False

        # if signal has changed, execute
        if has_changed:
            current_posdata = self.data_man.current_posdata[0]
            mark_price = current_posdata.markPx

            self.pos_man.execute(
                self.data_man.contracts,
                final_signal,
                mark_price,
                current_posdata,
            )

            notional = round(self.pos_man.target_notional, 2)
            lever = self.pos_man.target_lever
            utils.push_to_device(
                self.push_url,
                "Trading Bot",
                f"Signal: {final_signal}, Notional Pos Size: {notional}, Leverage: {lever}",
            )
            self.logger.warning(
                f"NEW Sig str: {final_signal}, notional pos size: {notional}, leverage: {lever}"
            )

        time.sleep(1)

    async def run(self):
        # run once immediately
        self.logger.info("Starting main loop")
        await self.job()
        utils.push_to_device(self.push_url, "Trading Bot", "Started")

        while True:
            now_seconds = datetime.now(timezone.utc).second
            sleep_seconds = (20 - now_seconds) % 60
            await asyncio.sleep(sleep_seconds)
            await self.job()

    def start(self):
        asyncio.run(self.run())


if __name__ == "__main__":
    load_dotenv()
    trading_system = TradingSystem("bot_config.toml")
    trading_system.start()
