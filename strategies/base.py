from abc import ABC, abstractmethod

import numpy as np
from numba import njit

from trader.objects import Contract


@njit
def interpolate(s, datalen):
    """
    interpolate signal to match data length
    normal interpolation won't work because it will
    involve future data
    """
    s_interpolated = np.zeros(datalen)
    factor = datalen // len(s)
    s_interpolated[: factor * len(s)] = np.repeat(s, factor)
    s_interpolated = np.roll(s_interpolated, factor)
    s_interpolated[:factor] = 0
    return s_interpolated


def signed_log(x):
    """
    apply log while preserving sign
    """
    return np.sign(x) * np.log(np.abs(x) + 1)


class TradeSignalGenerator(ABC):
    """
    Abstract base class for signal generators.

    This class defines the interface for all signal generators. Each signal generator
    must implement the `generate_signals` method, which takes an OHLCV dataframe and
    returns a series of trading signals.
    """

    @abstractmethod
    def generate_signals(self, **kwargs):
        pass

    @abstractmethod
    def validate_params(self):
        """
        Validate the parameters of the signal generator.

        Returns:
            bool: True if the parameters are valid, False otherwise.
        """
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass

    def get_params(self):
        """
        Get the parameters of the signal generator.

        Returns:
            list: The parameters of the signal generator.
        """
        return [v for k, v in vars(self).items() if not k.startswith("_")]

    def set_params(self, params, return_self=False):
        """
        Set the parameters of the signal generator.
        Parameters must be passed in the same order as returned by get_params.
        """
        if isinstance(params, dict):
            for param, value in params.items():
                setattr(self, param, value)
        else:
            for i, param in enumerate(vars(self)):
                if not param.startswith("_"):
                    setattr(self, param, params[i])

        if not self.validate_params():
            raise ValueError("Invalid parameters")

        if return_self:
            return self

    @property
    def strategy_kind(self):
        return self._strategy_kind

    @classmethod
    def new(cls, **kwargs):
        return cls(**kwargs)

    @staticmethod
    @njit
    def gen_overlap_signals(close, lead, lag, mode):
        """
        Generate overlap signals based on the comparison of lead and lag values.

        Parameters:
        close (numpy.ndarray): The closing prices.
        lead (numpy.ndarray): The lead values.
        lag (numpy.ndarray): The lag values.
        mode (str, optional): The mode of signal generation. Defaults to "both".

        Returns:
        numpy.ndarray: The generated signals.

        Notes:
        - Mode 1: "long_only", a signal of 1 is generated when lead > lag.
        - Mode 2: "short_only", a signal of -1 is generated when lead < lag.
        - Mode 3: "both", a signal of 1 is generated when lead > lag,
        and a signal of -1 is generated when lead < lag.
        """
        s = np.zeros(len(close))
        if mode == 1:
            s[lead > lag] = 1
        elif mode == 2:
            s[lead < lag] = -1
        elif mode == 3:
            s[lead > lag] = 1
            s[lead < lag] = -1
        return s

    @staticmethod
    @njit
    def gen_crossover_signals(ind, thresh, mode):
        """
        Generate crossover signals.

        Args:
            ind (ndarray): The indicator values.
            thresh (float): The threshold value.
            mode : see gen_overlap_signals

        Returns:
            ndarray: The generated signals.
        """
        s = np.zeros(len(ind))

        if mode == 1:
            s[ind > thresh] = 1
        elif mode == 2:
            s[ind > thresh] = -1
        elif mode == 3:
            s[ind > thresh] = 1
            s[ind < thresh] = -1
        return s

    @staticmethod
    @njit
    def gen_crossbelow_signals(ind, thresh, mode):
        """
        Generate crossbelow signals.

        Args:
            ind (ndarray): The indicator values.
            thresh (float): The threshold value.
            mode: see gen_overlap_signals

        Returns:
            ndarray: The generated signals.
        """
        s = np.zeros(len(ind))

        if mode == 1:
            s[ind < thresh] = 1
        elif mode == 2:
            s[ind < thresh] = -1
        elif mode == 3:
            s[ind < thresh] = 1
            s[ind > thresh] = -1

        return s

    @staticmethod
    @njit
    def gen_oscil_signals(ind, lower, upper, reversed=False):
        """
        For mean reversion strategies.
        reversed=True is for trend following strategies.
        Generate oscillation signals.
        If ind is above upper, signal is -1 until ind crosses below lower.
        If ind is below lower, signal is 1 until ind crosses above upper.

        Args:
            oscil_series (numpy.ndarray): The oscillation series.
            thresh_lower (float): The lower threshold value.
            thresh_upper (float): The upper threshold value.

        Returns:
            numpy.ndarray: The generated signals.
        """
        s = np.zeros(len(ind))
        idx = ind > upper
        idx = np.roll(idx, 1) & ~idx
        idx[0] = 0
        s[idx] = -1

        idx = ind < lower
        idx = np.roll(idx, 1) & ~idx
        idx[0] = 0
        s[idx] = 1
        s = TradeSignalGenerator.ffill(s)
        if reversed:
            s = -s
        return s

    # njit doesn't work with np.maximum.accumulate
    @staticmethod
    def ffill(arr):
        mask = arr == 0
        idx = np.where(~mask, np.arange(mask.shape[0]), 0)
        np.maximum.accumulate(idx, out=idx)
        out = arr[idx]
        return out

    @staticmethod
    @njit
    def discretize_signals(signals, mode):
        """
        Converts continuous signals into discrete signals.

        Args:
            signals: The continuous signals.
            mode: in accordance with gen_overlap_signals

        Returns:
            The discrete signals.
        """
        rolled_signals = np.roll(signals, 1)
        diff = signals - rolled_signals
        if mode == 1:
            diff[diff > 0] = 1
            diff[diff < 0] = 0
        elif mode == 2:
            diff[diff < 0] = -1
            diff[diff > 0] = 0
        return diff


class SignalVoter(TradeSignalGenerator):
    """
    A signal generator that generates signals based on voting of multiple signal generators.

    Parameters:
        signal_generators (list): A list of signal generators.
    """

    def __init__(
        self, signal_generators: list, mode: str = "threshold", threshold: float = 0.5
    ):
        self.signal_generators = signal_generators
        self.mode = mode
        self.threshold = int(len(signal_generators) * threshold)
        self.resample = 1
        self._strategy_kind = "rs"

    def __repr__(self):
        return f"SignalVoter with {len(self.signal_generators)} signal generators"

    def validate_params(self):
        return all(
            [isinstance(sg, TradeSignalGenerator) for sg in self.signal_generators]
        )

    def process_sg(sg, data):
        if sg.resample > 1:
            data_resampled = data.resample(sg.resample)
            signal = sg.generate_signals(data_resampled)
            signal = interpolate(signal, len(data.close))
        else:
            signal = sg.generate_signals(data)
        return signal

    def generate_signals(self, data: Contract):
        """
        Generates signals based on voting of multiple signal generators.

        Returns:
            s (list): List of generated signals.
        """
        signals = []
        for sg in self.signal_generators:
            if sg.resample > 1:
                data_resampled = data.resample(sg.resample)
                signal = sg.generate_signals(data_resampled)
                signal = interpolate(signal, len(data.close))
            else:
                signal = sg.generate_signals(data)
            signals.append(signal)

        signals = np.array(signals)
        if self.mode == "threshold":
            sum_s = np.sum(signals, axis=0)
            s = np.zeros(len(sum_s))
            s = np.where(sum_s >= self.threshold, 1, s)
            s = np.where(sum_s <= -self.threshold, -1, s)
        elif self.mode == "majority":
            s = np.sign(np.sum(signals, axis=0))
        return s
