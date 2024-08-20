#!/usr/bin/env python3

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import os
import time

from dotenv import load_dotenv
import requests
from tenacity import retry, wait_random_exponential

import gateways
from Core import utils
from Core.logger import LoggedClass
from trader.managers import SignalManager, PositionManager


def push_to_device(url, title, content):
    # small clever recursion
    if isinstance(url, list):
        for u in url:
            push_to_device(u, title, content)
        return
    url = url + f"/{title}/{content}"
    requests.post(url)


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
        self.contracts = {
            bar_interval: None for bar_interval in self.instrument_info["bars"]
        }

        self.gateway = self.initialize_gateway()

        self.sig_man = SignalManager(self.configs)
        self.pos_man = PositionManager(self.configs, self.gateway)

        self.mark_price = 0
        self.current_posdata = None

        self.push_url = os.environ.get("PUSH_URL")

        self.first_run = True

    def initialize_gateway(self) -> gateways.BaseGateway:
        gateway_type = self.configs.get("gateway")
        if gateway_type == "OKX":
            return gateways.OKXGateway()

    async def update_mark_price(self):
        self.mark_price = await self.gateway.get_mark_price(
            instId=self.instrument_info["markId"],
            instType=self.instrument_info["instType"],
        )

    async def update_current_posdata(self):
        # we just get the first one assuming there's only one position
        self.current_posdata = await self.gateway.get_positions(
            instId=self.instrument_info["markId"],
            instType=self.instrument_info["instType"],
        )

    @retry(wait=wait_random_exponential(multiplier=1), reraise=True)
    async def fetch_and_process_data(self, bar_interval: int) -> Dict[str, Any]:
        try:
            instId = self.instrument_info["instId"]
            limit = self.instrument_info["limit"]
            polled_contract = await self.gateway.fetch_and_process(
                instId, bar_interval, limit
            )
            self.contracts[bar_interval] = polled_contract
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch data for {bar_interval}m - {str(e)} - retrying..."
            )
            raise

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

            elif now_minute % bar_interval == 0:
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
        tasks.append(self.update_mark_price())
        tasks.append(self.update_current_posdata())
        self.logger.info(f"Fetching data for {bar_intervals_to_fetch} minutes")
        await asyncio.gather(*tasks)
        final_signal, has_changed = await self.sig_man.generate_signals(self.contracts)
        self.first_run = False

        if has_changed:
            self.pos_man.execute(
                self.contracts, final_signal, self.mark_price, self.current_posdata[0]
            )
            notional = self.pos_man.target_notional
            lever = self.pos_man.target_lever

            push_to_device(
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
        push_to_device(self.push_url, "Trading Bot", "Started")

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
