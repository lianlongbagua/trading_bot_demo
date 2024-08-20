import asyncio

from Core.strategy_factory import create_strategy
from Core.logger import LoggedClass


class SignalManager(LoggedClass):
    def __init__(self, config):
        super().__init__(__name__)
        # organized by resample "bar_interval" key
        strategy_configs = config["strategies"]
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


class PositionManager(LoggedClass):
    def __init__(self, config, gateway):
        """
        calculates risk based on ATR
        position size = max_risk / ATR
        returns position in units, not USD
        notional = position * price
        """
        super().__init__(__name__)
        risk_config = config["risk"]
        self.max_risk = risk_config["max_risk"]
        self.bar_interval = risk_config["bar_interval"]
        self.atr_period = risk_config["atr_period"]
        self.markId = config["instrument"]["markId"]
        self.instType = config["instrument"]["instType"]
        self.contract_size = config["instrument"]["contract_size"]
        self.tol = risk_config["tol"]

        self.target_pos = 0
        self.target_notional = 0
        self.target_contracts = 0
        self.target_lever = 0

        self.gateway = gateway

    def _get_current_pos(self):
        pass

    def _get_target_pos(self, contracts, signal_strength):
        # size in BTC
        atr = contracts[self.bar_interval].atr(self.atr_period)[-1]
        # some tolerance
        atr *= self.tol
        self.target_pos = round((self.max_risk / atr) * signal_strength, 6)

    def _get_target_notional_size(self, mark_price):
        # size in USDT
        self.target_notional = mark_price * self.target_pos

    def _get_target_contracts(self):
        # every contract is 0.001 BTC here
        self.target_contracts = self.target_pos / self.contract_size

    def _get_target_leverage(self):
        self.target_lever = int(self.target_notional / self.max_risk)
        # cap leverage at 100
        self.target_lever = 100 if self.target_lever > 100 else self.target_lever

    def _calc_target(self, contracts, signal_strength, mark_price):
        self._get_target_pos(contracts, signal_strength)
        self._get_target_notional_size(mark_price)
        self._get_target_leverage()
        self._get_target_contracts()

    def _calc_diff(self, current_pos):
        current_lever = int(current_pos.lever) if current_pos.lever else 0
        current_pos_size = float(current_pos.pos) if current_pos.pos else 0.0
        lever_diff = self.target_lever - int(current_lever)
        contracts_diff = self.target_contracts - float(current_pos_size)
        return lever_diff, contracts_diff

    def execute(self, contracts, signal_strength, mark_price, current_pos):
        self._calc_target(contracts, signal_strength, mark_price)
        # current_pos = self._get_current_pos()
        lever_diff, contracts_diff = self._calc_diff(current_pos)

        if lever_diff:
            self.gateway.set_leverage(
                instId=self.markId,
                lever=str(self.target_lever),
                mgnMode="isolated",
            )
            self.logger.warning(f"Set leverage to {self.target_lever}")

        if contracts_diff:
            self.gateway.place_order(
                instId=self.markId,
                tdMode="isolated",
                ordType="market",
                side="buy" if contracts_diff > 0 else "sell",
                sz=str(abs(round(contracts_diff, 1))),
            )
            self.logger.warning(
                f"Placed order for {round(contracts_diff, 1)} contracts"
            )
