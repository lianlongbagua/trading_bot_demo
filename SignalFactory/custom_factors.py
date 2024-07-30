import talib as ta
import numpy as np

from DataHandling import Contract
from .signal_generator import SignalGenerator


class MAOverlap(SignalGenerator):
    """
    A signal generator that generates signals based on moving average crossover strategy.

    Parameters:
        lead (int)
        lag (int)
        decay_factor (int): a decay factor of 1 means no decay
        resample (int)
        mode (int): 1 for long only, 2 for short only, 3 for both

    Additional Properties:
        _strategy_kind (str): right skew or left skew
        _true_range (int): The longest timeframe used by the strategy.
    """

    def __init__(
        self,
        lead: int = None,
        lag: int = None,
        decay_factor: int = 1,
        resample: int = 1,
        mode: int = 3,
    ):
        self.lead = lead
        self.lag = lag
        self.decay_factor = decay_factor
        self.resample = resample
        self.mode = mode
        self._strategy_kind = "rs"
        self._true_range = decay_factor * lag if lag else None

    def __repr__(self):
        return f"MAOverlap with params: lead={self.lead}, lag={self.lag}, decay_factor={self.decay_factor}, resample={self.resample}, mode={self.mode}"

    def validate_params(self):
        return all(
            [
                self.lead > 1,
                self.lag > 2,
                self.lead < self.lag,
                self.decay_factor > 0,
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
        s = self.apply_decay(s)
        s = np.nan_to_num(s)
        return s


class RSICrossover(SignalGenerator):
    def __init__(
        self,
        N: int = None,
        detrend_N: int = None,
        decay_factor: int = 1,
        resample: int = 1,
        mode: int = 3,
    ):
        self.N = N
        self.detrend_N = detrend_N
        self.decay_factor = decay_factor
        self.resample = resample
        self.mode = mode
        self._strategy_kind = "rs"
        self._true_range = decay_factor * N if N else None

    def __repr__(self):
        return f"RSICrossover with params: N={self.N}, detrend_N={self.detrend_N}, decay_factor={self.decay_factor}, resample={self.resample}, mode={self.mode}"

    def validate_params(self):
        return all(
            [
                self.N > 1,
                self.detrend_N > 0,
                self.decay_factor > 0,
                self.resample > 0,
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
        s = self.apply_decay(s)
        s = np.nan_to_num(s)
        return s
