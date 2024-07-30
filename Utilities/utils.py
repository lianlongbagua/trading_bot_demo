import numpy as np
from numba import njit


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
