import pandas as pd
from arcticdb import Arctic


def import_from_arctic(symbol, date_range, library="vnpy_futures", laptop=False):
    """
    Imports data for a given symbol and date range from an Arctic database.

    Parameters:
    - symbol (str): The symbol for which to retrieve data.
    - date_range (tuple): A tuple of (start_date, end_date) for the data retrieval.
    - laptop (bool): There are different locations for the Arctic database depending on whether the code is running on a laptop or a server. Set this to True if running on a laptop.

    Returns:
    - pandas.DataFrame: The OHLCV data for the given symbol and date range.
    """
    if laptop:
        store_path = "lmdb://D:/arcticdb_vnpy"
    else:
        store_path = "lmdb://e:/lmdb_storage"
    store = Arctic(store_path)
    library = store.get_library(library)
    data = library.read(symbol, date_range=date_range).data
    data.attrs["symbol"] = symbol
    return data


def import_from_csv(filepath):
    data = pd.read_csv(filepath, index_col="datetime", parse_dates=True)
    data.attrs["symbol"] = filepath.split("/")[-1].split(".")[0]
    return data
