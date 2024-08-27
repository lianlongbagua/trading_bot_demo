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

    def initialize_gateway(self) -> gateways.BaseGateway:
        gateway_type = self.configs.get("gateway")
        if gateway_type == "OKX":
            return gateways.OKXGateway()

    async def confirm_order(self):
        orderdata = await self.gateway.get_order(
            instId=self.instrument["symbol"], ordId=self.pos_man.last_orderid
        )
        if orderdata:
            orderdata = orderdata[0]
            self.logger.warning(f"Order: {orderdata}")
            utils.push_to_device(
                self.push_url,
                "OKX Bot Confirm Order",
                f"{orderdata.order_status=}",
            )

    async def confirm_pos(self):
        posdata = await self.gateway.get_positions(instId=self.instrument["symbol"])
        if posdata:
            posdata = posdata[0]
            self.logger.warning(f"Position: {posdata}")
            utils.push_to_device(
                self.push_url,
                "OKX Bot Confirm Position",
                f"actual notional = {posdata.notional}, target = {round(self.pos_man.target_notional, 2)}, "
                f"actual lever = {posdata.lever}, target = {self.pos_man.target_lever}",
            )

            self.pos_man.adjust_margin(posdata)

    def adjust_pos(self):
        utils.push_to_device(
            self.push_url,
            "OKX Bot New Signal",
            f"{self.instrument['symbol']}:{self.sig_man.final_signal}",
        )
        self.logger.warning(f"NEW Sig: {self.sig_man.final_signal}")

        self.pos_man.execute(
            self.data_man.contracts,
            self.sig_man.final_signal,
            self.data_man.mark_price,
            self.data_man.current_posdata,
        )

    async def job(self):
        tasks = self.data_man.periodic_sync_tasks()
        await asyncio.gather(*tasks)

        signal_changed = await self.sig_man.generate_signals(self.data_man.contracts)

        # if signal has changed, execute
        if signal_changed:
            self.adjust_pos()

            # confirm order has been filled
            await asyncio.sleep(3)
            await asyncio.gather(self.confirm_order(), self.confirm_pos())

        time.sleep(1)

    async def run(self):
        try:
            # run once immediately
            self.logger.info(f"Starting main loop trading {self.instrument['symbol']}")
            utils.push_to_device(
                self.push_url,
                f"{self.configs['gateway']} Bot",
                f"Start trading {self.instrument['symbol']}",
            )
            await self.job()

            while True:
                now_seconds = datetime.now(timezone.utc).second
                sleep_seconds = (20 - now_seconds) % 60
                await asyncio.sleep(sleep_seconds)
                await self.job()
        except Exception as e:
            self.logger.error(f"Error in main loop: {str(e)}")
            utils.push_to_device(self.push_url, "OKX Bot Error", f"{str(e)}")

    def start(self):
        asyncio.run(self.run())


if __name__ == "__main__":
    load_dotenv()
    trading_system = TradingSystem("bot_config.toml")
    trading_system.start()
