import talib as ta
import numpy as np

from .datas import Contract
from .signal_generator import SignalGenerator


class MAOverlap(SignalGenerator):
    """
    A signal generator that generates signals based on moving average crossover strategy.

    Parameters:
        lead (int)
        lag (int)
        resample (int)
        mode (int): 1 for long only, 2 for short only, 3 for both

    Additional Properties:
        _strategy_kind (str): right skew or left skew
    """

    def __init__(
        self,
        lead: int = None,
        lag: int = None,
        resample: int = 1,
        mode: int = 3,
    ):
        self.lead = lead
        self.lag = lag
        self.resample = resample
        self.mode = mode
        self._strategy_kind = "rs"

    def __repr__(self):
        return f"MAOverlap with params: lead={self.lead}, lag={self.lag}, resample={self.resample}, mode={self.mode}"

    def validate_params(self):
        return all(
            [
                self.lead > 1,
                self.lag > 2,
                self.lead < self.lag,
                self.resample > 0,
                self.mode in [1, 2, 3],
            ]
        )

    def generate_signals(self, data: Contract):
        """
        Generates signals based on moving average crossover strategy.

        Returns:
            s (list): List of generated signals.
        """
        lead_ma_arr = data.ma(self.lead)
        lag_ma_arr = data.ma(self.lag)
        s = self.gen_overlap_signals(data.close, lead_ma_arr, lag_ma_arr, self.mode)
        s = np.nan_to_num(s)
        return s


class RSICrossover(SignalGenerator):
    def __init__(
        self,
        N: int = None,
        detrend_N: int = None,
        resample: int = 1,
        mode: int = 3,
    ):
        self.N = N
        self.detrend_N = detrend_N
        self.resample = resample
        self.mode = mode
        self._strategy_kind = "ls"

    def __repr__(self):
        return f"RSICrossover with params: N={self.N}, detrend_N={self.detrend_N}, resample={self.resample}, mode={self.mode}"

    def validate_params(self):
        return all(
            [
                self.N > 1,
                self.detrend_N > 0,
                self.mode in [1, 2, 3],
            ]
        )

    def generate_signals(self, data: Contract):
        """
        Generates signals based on RSI crossover strategy.

        Returns:
            s (list): List of generated signals.
        """
        if self.detrend_N > 1:
            detrend_arr = ta.LINEARREG(data.close, timeperiod=self.detrend_N)
            detrended_close = data.close - detrend_arr
            rsi_arr = ta.RSI(detrended_close, timeperiod=self.N)
        else:
            rsi_arr = data.rsi(self.N)
        s = self.gen_crossover_signals(rsi_arr, 50, self.mode)
        s = np.nan_to_num(s)
        return s
