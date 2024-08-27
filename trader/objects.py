from dataclasses import dataclass
from typing import List, Dict, Any
import numpy as np
import talib as ta


@dataclass
class OrderData:
    symbol: str
    side: str
    fill_price: float
    fill_time: str
    qty: float
    order_id: str
    order_type: str
    order_status: str
    lever: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderData":
        return cls(
            symbol=data["instId"],
            side=data["side"],
            fill_price=float(data["fillPx"]),
            qty=float(data["sz"]),
            order_id=data["ordId"],
            order_type=data["ordType"],
            order_status=data["state"],
            lever=int(data["lever"]),
            fill_time=data["fillTime"],
        )

    def __repr__(self):
        return (
            f"symbol={self.symbol}, side={self.side}, fill_price={self.fill_price}, qty={self.qty}, "
            f"order_id={self.order_id}, order_type={self.order_type}, order_status={self.order_status}, "
            f"lever={self.lever}, fill_time={self.fill_time}"
        )


def parse_order_data(data_packet: Dict[str, Any]) -> List[OrderData]:
    orders = data_packet.get("data", [])
    return [OrderData.from_dict(order) for order in orders]


@dataclass
class PositionData:
    symbol: str
    qty: float
    side: str
    average_price: float
    lever: int
    margin_mode: str
    margin: float
    notional: float
    unrealized_pnl: float

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PositionData":
        try:
            return cls(
                symbol=data["instId"],
                qty=float(data["pos"]),
                side=data["posSide"],
                average_price=round(float(data["avgPx"]), 2),
                lever=int(data["lever"]),
                margin_mode=data["mgnMode"],
                margin=round(float(data["margin"]), 2),
                notional=round(float(data["notionalUsd"]), 2),
                unrealized_pnl=round(float(data["upl"]), 2),
            )
        except Exception as e:
            return None

    def __repr__(self):
        return (
            f"symbol={self.symbol}, qty={self.qty}, side={self.side}, "
            f"average_price={self.average_price}, lever={self.lever}, margin_mode={self.margin_mode}, "
            f"margin={self.margin}, notional={self.notional}, unrealized_pnl={self.unrealized_pnl})"
        )


def parse_position_data(data_packet: Dict[str, Any]) -> List[PositionData]:
    positions = data_packet.get("data", [])
    return [PositionData.from_dict(pos) for pos in positions]


class Contract:
    """
    Attributes:
        data (np.ndarray): A 2D NumPy array containing OHLCV data.
        datetime (np.ndarray): An array containing the datetime for each entry.
        interval (int): The time interval between entries.
        symbol (str): The symbol for the financial instrument.
    """

    __slots__ = ["data", "datetime", "interval", "symbol", "total_trading_days"]

    def __init__(self, ohlcv_data, datetime, interval, symbol=None):
        """
        Initialize the ohlcv class with data.

        Args:
            ohlcv_data (np.ndarray or list): The OHLCV data as a 2D NumPy array or list of lists.
            datetime (np.ndarray or list): The datetime for each entry.
            interval (int): The time interval between entries.
            symbol (str, optional): The symbol for the financial instrument.
        """
        if not isinstance(ohlcv_data, np.ndarray):
            ohlcv_data = np.array(ohlcv_data, dtype=np.double)
        elif ohlcv_data.dtype != np.double:
            ohlcv_data = ohlcv_data.astype(np.double)

        if not isinstance(datetime, np.ndarray):
            datetime = np.array(datetime, dtype="datetime64[m]")

        self.data = ohlcv_data
        self.datetime = datetime
        self.interval = interval
        self.symbol = symbol
        self.total_trading_days = len(np.unique(datetime.astype("datetime64[D]")))

    @classmethod
    def from_dataframe(cls, dataframe):
        """
        Create an instance of OHLCV from a pandas DataFrame.

        Args:
            dataframe (pandas.DataFrame): The DataFrame containing OHLCV data.
            interval (int): The time interval between entries.
            symbol (str, optional): The symbol for the financial instrument.

        Returns:
            ohlcv: An instance of OHLCV containing the data from the DataFrame.
        """
        ohlcv_data = dataframe[["open", "high", "low", "close", "volume"]].values.T
        datetime = dataframe.index
        interval = (datetime[1] - datetime[0]).seconds // 60
        symbol = dataframe.attrs["symbol"]
        return cls(ohlcv_data, datetime, interval, symbol)

    @property
    def open(self):
        return self.data[0]

    @property
    def high(self):
        return self.data[1]

    @property
    def low(self):
        return self.data[2]

    @property
    def close(self):
        return self.data[3]

    @property
    def volume(self):
        return self.data[4]

    @property
    def pure_returns(self):
        return np.diff(self.close)

    @property
    def log_returns(self):
        return np.diff(np.log(self.close))

    def __len__(self):
        return len(self.data[0])

    def __getitem__(self, key):
        """
        Enable indexing and slicing of the OHLCV data.

        Args:
            key (int or slice): The index or slice of the data to retrieve.

        Returns:
            ohlcv: A new instance of ohlcv with the sliced data.
        """
        if isinstance(key, slice):
            data_slice = self.data[:, key]
            datetime_slice = self.datetime[key]
            return Contract(data_slice, datetime_slice, self.interval, self.symbol)
        else:
            # not sure if this is the best way to handle this
            return self.data[:, key]

    def __repr__(self):
        return (
            f"ohlcv(symbol={self.symbol}, length={len(self)}, "
            f"begin={self.datetime[0].astype('datetime64[D]').__str__()}, "
            f"end={self.datetime[-1].astype('datetime64[D]').__str__()}, "
            f"interval={self.interval} minutes)"
        )

    @staticmethod
    def _resample(data, interval, operation):
        """Helper function to resample prices or volume"""
        resampled = operation(
            np.lib.stride_tricks.sliding_window_view(data, interval)[::interval, :],
            axis=1,
        )
        return np.ascontiguousarray(resampled)

    def resample(self, interval):
        """Resample the OHLCV data to a new interval"""
        if interval == 1:
            return self

        num_periods = self.data.shape[1] // interval
        resampled_data = np.empty((self.data.shape[0], num_periods))
        resampled_data[0] = self.open[interval - 1 :: interval]  # Open
        resampled_data[1] = self._resample(self.high, interval, np.max)  # High
        resampled_data[2] = self._resample(self.low, interval, np.min)  # Low
        resampled_data[3] = self.close[interval - 1 :: interval]  # Close
        resampled_data[4] = self._resample(self.volume, interval, np.sum)  # Volume

        resampled_datetime = self.datetime[interval - 1 :: interval]

        return Contract(
            resampled_data, resampled_datetime, self.interval * interval, self.symbol
        )

    def ma(self, timeperiod, kind="ema"):
        func = eval(f"ta.{kind.upper()}")
        return func(self.close, timeperiod)

    def rsi(self, timeperiod):
        return ta.RSI(self.close, timeperiod)

    def macd(self):
        macd, signal, hist = ta.MACD(self.close)
        return macd, signal, hist

    def adx(self, timeperiod):
        return ta.ADX(self.high, self.low, self.close, timeperiod)

    def cci(self, timeperiod):
        return ta.CCI(self.high, self.low, self.close, timeperiod)

    def mom(self, timeperiod):
        return ta.MOM(self.close, timeperiod)

    def stoch(self, fastk_period, slowk_period, slowd_period):
        slowk, slowd = ta.STOCH(
            self.high, self.low, self.close, fastk_period, slowk_period, slowd_period
        )
        return slowk, slowd

    def atr(self, timeperiod):
        return ta.ATR(self.high, self.low, self.close, timeperiod)

    def bollinger_bands(self, timeperiod, nbdevup=2, nbdevdn=2, matype=0):
        upper, middle, lower = ta.BBANDS(
            self.close, timeperiod, nbdevup, nbdevdn, matype
        )
        return upper, middle, lower

    def ad(self):
        return ta.AD(self.high, self.low, self.close, self.volume)

    def obv(self):
        return ta.OBV(self.close, self.volume)

    def aroon(self, timeperiod):
        aroondown, aroonup = ta.AROON(self.high, self.low, timeperiod)
        return aroondown, aroonup

    def aroon_osc(self, timeperiod):
        return ta.AROONOSC(self.high, self.low, timeperiod)

    def cmo(self, timeperiod):
        return ta.CMO(self.close, timeperiod)

    def dx(self, timeperiod):
        return ta.DX(self.high, self.low, self.close, timeperiod)

    def mfi(self, timeperiod):
        return ta.MFI(self.high, self.low, self.close, self.volume, timeperiod)

    def minus_di(self, timeperiod):
        return ta.MINUS_DI(self.high, self.low, self.close, timeperiod)

    def minus_dm(self, timeperiod):
        return ta.MINUS_DM(self.high, self.low, timeperiod)

    def plus_di(self, timeperiod):
        return ta.PLUS_DI(self.high, self.low, self.close, timeperiod)

    def plus_dm(self, timeperiod):
        return ta.PLUS_DM(self.high, self.low, timeperiod)

    def roc(self, timeperiod):
        return ta.ROC(self.close, timeperiod)

    def rocp(self, timeperiod):
        return ta.ROCP(self.close, timeperiod)

    def rocr(self, timeperiod):
        return ta.ROCR(self.close, timeperiod)

    def rocr100(self, timeperiod):
        return ta.ROCR100(self.close, timeperiod)

    def trix(self, timeperiod):
        return ta.TRIX(self.close, timeperiod)

    def willr(self, timeperiod):
        return ta.WILLR(self.high, self.low, self.close, timeperiod)


# Example usage:
# ohlcv_data = [[open1, high1, low1, close1, volume1], [open2, high2, low2, close2, volume2], ...]
# datetime = [datetime1, datetime2, ...]
# ohlcv_instance = ohlcv(ohlcv_data, datetime, interval=1, symbol='AAPL')


class Portfolio:
    """
    Attributes:
        contracts (dict): A dictionary mapping symbols to Contract instances.
    """

    def __init__(self, contracts: list = None):
        self.interval = None
        self.max_length = 0
        self.max_len_contract = None
        if not contracts:
            self.contracts = {}
        else:
            if self._validate_contracts(contracts):
                self.contracts = {c.symbol: c for c in contracts}
                self.interval = contracts[0].interval
                self.max_length = self._get_max_length()
                self.max_len_contract = max(
                    self.contracts, key=lambda x: len(self.contracts[x])
                )

    def add_contract(self, contract):
        # Ensure all contracts have the same interval
        if not self.interval:
            self.interval = contract.interval
        elif not self._validate_interval(contract):
            raise ValueError("All contracts must have the same interval.")
        elif not self._validate_duplicates(contract):
            print("Duplicate symbol found in portfolio.")
            return

        if not contract.symbol:
            contract.symbol = f"contract_{len(self.contracts) + 1}"
        self.contracts[contract.symbol] = contract
        self.max_length = max(self.max_length, len(contract))
        self.max_len_contract = max(
            self.contracts, key=lambda x: len(self.contracts[x])
        )

    def remove_contract(self, symbol):
        if symbol in self.contracts:
            del self.contracts[symbol]

    def _validate_interval(self, contract):
        if contract.interval != self.interval:
            return False
        return True

    def _get_max_length(self):
        return max(len(c) for c in self.contracts.values())

    @staticmethod
    def _validate_contracts(contracts):
        if not all(isinstance(c, Contract) for c in contracts):
            raise ValueError("All contracts must be instances of the Contract class.")

        intervals = set(c.interval for c in contracts)
        if len(intervals) > 1:
            raise ValueError("All contracts must have the same interval.")
        return True

    def _validate_duplicates(self, contract):
        if contract.symbol in self.contracts.keys():
            return False
        return True

    def __getitem__(self, symbol):
        if symbol not in self.contracts.keys():
            raise KeyError(f"Symbol {symbol} not found in portfolio.")
        return self.contracts[symbol]

    def __len__(self):
        return len(self.contracts)

    def __iter__(self):
        return iter(self.contracts.values())

    def resample(self, interval):
        contracts = self.contracts.copy()
        for symbol, contract in self.contracts.items():
            contracts[symbol] = contract.resample(interval)
        return Portfolio(list(contracts.values()))

    @property
    def total_trading_days(self):
        return self.contracts[self.max_len_contract].total_trading_days
