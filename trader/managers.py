import asyncio
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta

from tenacity import retry, wait_random_exponential

from strategies.strategy_factory import create_strategy
from trader.logger import LoggedClass


class DataManager(LoggedClass):
    def __init__(self, configs, gateway):
        super().__init__(__name__)
        self.instrument = configs["instrument"]
        self.contracts = {
            bar_interval: None for bar_interval in self.instrument["bar_intervals"]
        }

        self.gateway = gateway

        self._current_posdata = None

    @property
    def current_posdata(self):
        try:
            return self._current_posdata[0]
        except IndexError:
            self.logger.warning("No current pos data")
            return None

    async def update_current_posdata(self):
        # we just get the first one assuming there's only one position
        self._current_posdata = await self.gateway.get_positions(
            instId=self.instrument["symbol"],
            instType=self.instrument["inst_type"],
        )

    async def update_mark_price(self):
        self.mark_price = await self.gateway.get_mark_price(
            instId=self.instrument["symbol"],
            instType=self.instrument["inst_type"],
        )

    @retry(wait=wait_random_exponential(multiplier=1))
    async def fetch_and_process_data(self, bar_interval: int) -> Dict[str, Any]:
        try:
            instId = self.instrument["index_symbol"]
            limit = self.instrument["bar_length"]
            polled_contract = await self.gateway.fetch_and_process(
                instId, bar_interval, limit
            )
            self.contracts[bar_interval] = polled_contract
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch data for {bar_interval}m - {str(e)} - retrying..."
            )
            raise

        if not self.is_data_complete(polled_contract):
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

    def periodic_sync_tasks(self):
        bar_intervals_to_fetch = self.should_fetch_bars()
        tasks = [
            self.fetch_and_process_data(bar_interval)
            for bar_interval in bar_intervals_to_fetch
        ]
        tasks.append(self.update_current_posdata())
        tasks.append(self.update_mark_price())
        self.logger.info(f"Fetching data for {bar_intervals_to_fetch} minutes")
        return tasks


class SignalManager(LoggedClass):
    def __init__(self, config):
        super().__init__(__name__)
        # organized by resample "bar_interval" key
        strategy_configs = config["strategies"]
        self.combine_kind = config["combine_kind"]
        self.strategy_dict = asyncio.run(self.load_strategies(strategy_configs))
        strategy_len = sum(len(v) for v in self.strategy_dict.values())
        self.logger.info(f"Loaded {strategy_len} strategies")

        self.final_signal = 0.0

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
    def combine_signals(signals, combine_kind):
        if combine_kind == "sum":
            return sum(signals) / len(signals) if signals else 0
        if combine_kind == "vote":
            return sum(signals) / abs(sum(signals)) if abs(sum(signals)) > 0 else 0

    async def generate_signals(self, contracts):
        signals = []

        tasks = []
        for bar_interval, sgs in self.strategy_dict.items():
            if bar_interval in contracts:
                contract = contracts[bar_interval]
                for sg in sgs:
                    tasks.append(self._gen_signal(sg, contract))

        signals = await asyncio.gather(*tasks)
        print(signals)
        new_signal = self.combine_signals(signals, self.combine_kind)
        signal_changed = self.signal_has_changed(new_signal)
        self.final_signal = new_signal
        self.logger.info(f"Final Signal: {self.final_signal}")
        return signal_changed

    def signal_has_changed(self, new_signal):
        return new_signal != self.final_signal


class PositionManager(LoggedClass):
    def __init__(self, config, gateway):
        """
        calculates risk based on ATR
        position size = (max_risk * signalstrength) / (ATR * tol)
        returns position in units, not USD
        notional = position * price
        """
        super().__init__(__name__)
        risk_config = config["risk"]
        self.max_risk = risk_config["max_risk"]
        self.bar_interval = risk_config["bar_interval"]
        self.atr_period = risk_config["atr_period"]
        self.symbol = config["instrument"]["symbol"]
        self.inst_type = config["instrument"]["inst_type"]
        self.contract_size = config["instrument"]["contract_size"]
        self.tol = risk_config["tol"]

        self.target_pos = 0
        self.target_notional = 0
        self.target_qty = 0
        self.target_lever = 0

        self.gateway = gateway
        self.last_orderid = None

    def _get_target_pos(self, contracts, signal_strength):
        # size in BTC
        atr = contracts[self.bar_interval].atr(self.atr_period)[-1] * self.tol
        self.target_pos = round((self.max_risk / atr) * signal_strength, 6)

    def _get_target_notional_size(self, mark_price):
        # size in USDT
        self.target_notional = float(mark_price) * self.target_pos

    def _get_target_contracts(self):
        # every contract is 0.001 BTC here
        self.target_qty = self.target_pos / self.contract_size

    def _get_target_leverage(self):
        self.target_lever = int(self.target_notional / self.max_risk)
        # cap leverage at 100
        self.target_lever = 100 if self.target_lever > 100 else self.target_lever

    def _calc_target(self, contracts, signal_strength, mark_price):
        self._get_target_pos(contracts, signal_strength)
        self._get_target_notional_size(mark_price)
        self._get_target_leverage()
        self._get_target_contracts()

    def _calc_diff(self, current_posdata):
        if not current_posdata:
            return self.target_lever, self.target_qty
        current_lever = int(current_posdata.lever) if current_posdata.lever else 0
        current_qty = float(current_posdata.qty) if current_posdata.qty else 0.0
        lever_diff = self.target_lever - int(current_lever)
        qty_diff = self.target_qty - float(current_qty)
        return lever_diff, qty_diff

    def adjust_margin(self, current_posdata):
        margin = current_posdata.margin
        if margin > self.max_risk * 1.5:
            margin_diff = margin - self.max_risk * 1.5
            amt = int(margin_diff)
            result = self.gateway.adjust_margin(
                instId=self.symbol, posSide="net", type="reduce", amt=str(amt)
            )
            if result.get("code") != "0":
                self.logger.error(f"Failed to reduce margin by {amt} - {result['msg']}")
            else:
                self.logger.warning(f"Reduced margin by {amt}")

    def execute(self, contracts, signal_strength, mark_price, current_posdata):
        self._calc_target(contracts, signal_strength, mark_price)
        lever_diff, qty_diff = self._calc_diff(current_posdata)

        if lever_diff:
            result = self.gateway.set_leverage(
                instId=self.symbol,
                lever=str(self.target_lever),
                mgnMode="isolated",
            )
            self.process_result(
                result, "Failed to set leverage", f"Set leverage to {self.target_lever}"
            )

        if abs(qty_diff) > 1:
            sz = str(abs(round(qty_diff, 1)))
            result = self.gateway.place_order(
                instId=self.symbol,
                tdMode="isolated",
                ordType="market",
                side="buy" if qty_diff > 0 else "sell",
                sz=str(abs(round(qty_diff, 1))),
            )
            self.process_result(
                result, "Failed to place order", f"Placed order for {sz}"
            )
            self.last_orderid = result["data"][0]["ordId"]

    def process_result(self, result, fail_msg, success_msg):
        if result.get("code") != "0":
            messages = [msg for msg in result["data"]]
            for msg in messages:
                self.logger.error(f"{fail_msg} - {msg['sMsg']}")
        else:
            self.logger.warning(success_msg)
