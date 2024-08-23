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
        self.instrument = self.configs["instrument"]

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
            try:
                current_posdata = self.data_man.current_posdata[0]
            except IndexError:
                self.logger.warning("No current position data")
                current_posdata = None

            mark_price = self.data_man.mark_price

            self.pos_man.execute(
                self.data_man.contracts,
                final_signal,
                mark_price,
                current_posdata,
            )

            target_notional = round(self.pos_man.target_notional, 2)
            target_lever = self.pos_man.target_lever
            utils.push_to_device(
                self.push_url,
                "OKX Bot New Signal",
                f"Signal: {final_signal}, target notional: {target_notional}, target lever: {target_lever}",
            )
            self.logger.warning(
                f"NEW Sig str: {final_signal}, target notional: {target_notional}, target lever: {target_lever}"
            )

            # confirm position
            await asyncio.sleep(5)

            posdata = await self.gateway.get_positions(instId=self.instrument["symbol"])

            if posdata:
                posdata = posdata[0]
                notional_diff = round(
                    (posdata.notional - target_notional) / target_notional, 2
                )
                lever_diff = posdata.lever - target_lever

                self.logger.warning(f"Confirmation: {notional_diff=}, {lever_diff=}")

                utils.push_to_device(
                    self.push_url,
                    "OKX Bot Confirm Position",
                    f"Position: {posdata} \n {notional_diff=}, {lever_diff=}",
                )

        time.sleep(1)

    async def run(self):
        # run once immediately
        self.logger.info("Starting main loop")
        utils.push_to_device(self.push_url, f"{self.configs['gateway']} Bot", "Started")
        await self.job()

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
