import numpy as np
from numba import njit
import toml
import pandas as pd
import requests


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


def write_to_toml(file_name, entry_key, content):
    try:
        with open(file_name, "r") as f:
            data = toml.load(f)
    except FileNotFoundError:
        data = {}

    # Update the specific sub-dictionary
    if entry_key not in data:
        data[entry_key] = {}

    data[entry_key] = content

    # Save the updated content back to the TOML file
    with open(file_name, "w") as f:
        toml.dump(data, f)


def append_to_toml(file_name, entry_key, content):
    """
    this updates the entire key with the new content
    """
    try:
        with open(file_name, "r") as f:
            data = toml.load(f)
    except FileNotFoundError:
        data = {}

    # Update the specific sub-dictionary
    if entry_key not in data:
        data[entry_key] = {}

    data[entry_key].update(content)

    # Save the updated content back to the TOML file
    with open(file_name, "w") as f:
        toml.dump(data, f)


def read_from_toml(file_name, entry_key):
    try:
        with open(file_name, "r") as f:
            data = toml.load(f)
    except FileNotFoundError:
        return None

    return data[entry_key]


def list_to_df(data):
    data = pd.DataFrame(
        data,
        columns=["time", "open", "high", "low", "close", "volume", "n1", "n2", "n3"],
    )
    data = data.drop(columns=["n1", "n2", "n3"])
    data = data.astype(float)
    data["time"] = pd.to_datetime(data["time"], unit="ms")
    data.set_index("time", inplace=True)
    data = data[::-1]
    return data


def load_configs(config_file):
    with open(config_file, "r") as f:
        config = toml.load(f)
    return config


def push_to_device(url, title, content):
    url = url + f"/{title}/{content}"
    requests.post(url)


def sum_signals(signals):
    """
    sum signals from multiple signal generators
    """
    return sum(signals) / len(signals)
