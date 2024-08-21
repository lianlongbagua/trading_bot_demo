import asyncio
from typing import Any, Dict
import os

from okx import MarketData, PublicData, Account, Trade
from tenacity import retry, wait_random_exponential

from trader import utils
from trader.objects import parse_position_data, Contract
from .base import BaseGateway


okx2fgtc = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1H": 60,
}

fgtc2okx = {v: k for k, v in okx2fgtc.items()}


class OKXGateway(BaseGateway):
    def __init__(self):
        super().__init__()
        self.credentials = {
            "api_key": os.environ.get("apikey"),
            "api_secret_key": os.environ.get("secretkey"),
            "passphrase": os.environ.get("passphrase"),
        }
        self.account_api = Account.AccountAPI(**self.credentials, debug=False)
        self.trade_api = Trade.TradeAPI(**self.credentials, debug=False)
        self.market_api = MarketData.MarketAPI(debug=False)
        self.public_api = PublicData.PublicAPI(debug=False)

    def get_account_config(self) -> Dict[str, Any]:
        return self.account_api.get_account_config()

    def place_order(self, **kwargs) -> Dict[str, Any]:
        try:
            return self.trade_api.place_order(**kwargs)
        except Exception as e:
            self.logger.warning(f"Failed to place order - {str(e)} - retrying...")
            raise

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    def get_account_balance(self, **kwargs) -> Dict[str, Any]:
        try:
            return self.account_api.get_account_balance(**kwargs)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch account balance - {str(e)} - retrying..."
            )
            raise

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    def get_account_position_risk(self, instType: str = "") -> Dict[str, Any]:
        try:
            return self.account_api.get_position_risk(instType)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch account position risk - {str(e)} - retrying..."
            )
            raise

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    async def get_positions(self, **kwargs) -> Dict[str, Any]:
        try:
            result = await asyncio.to_thread(self.account_api.get_positions, **kwargs)
            return parse_position_data(result)
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch account positions - {str(e)} - retrying..."
            )
            raise

    def set_leverage(self, **kwargs) -> Dict[str, Any]:
        try:
            return self.account_api.set_leverage(**kwargs)
        except Exception as e:
            self.logger.warning(
                f"Failed to set leverage for {kwargs.get('instId')} - {str(e)} - retrying..."
            )
            raise

    def get_leverage(self, **kwargs) -> Dict[str, Any]:
        try:
            return self.account_api.get_leverage(**kwargs)
        except Exception as e:
            self.logger.warning(
                f"Failed to get leverage for {kwargs.get('instId')} - {str(e)} - retrying..."
            )
            raise

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    async def get_mark_price(self, **kwargs) -> float:
        try:
            result = await asyncio.to_thread(self.public_api.get_mark_price, **kwargs)
            return float(result["data"][0]["markPx"])
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch mark price for {kwargs.get('instId')} - {str(e)} - retrying..."
            )
            raise

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    async def fetch(self, instId, fgtc_bar, limit) -> Dict[str, Any]:
        try:
            result = await asyncio.to_thread(
                self.market_api.get_candlesticks,
                instId=instId,
                bar=fgtc2okx[fgtc_bar],
                limit=limit,
            )
            result.update({"instId": instId, "bar": fgtc_bar, "limit": limit})
            return result
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {fgtc_bar}m candles for {instId} - {str(e)} - retrying..."
            )
            raise

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    async def fetch_index(self, instId, fgtc_bar, limit) -> Dict[str, Any]:
        try:
            limit = 100 if limit > 100 else limit
            result = await asyncio.to_thread(
                self.market_api.get_index_candlesticks,
                instId=instId,
                bar=fgtc2okx[fgtc_bar],
                limit=limit,
            )
            result.update({"instId": instId, "bar": fgtc_bar, "limit": limit})
            return result
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {fgtc_bar}m candles for {instId} - {str(e)} - retrying..."
            )
            raise

    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Wrap the processing in an asynchronous function
        df = await asyncio.to_thread(utils.list_to_df, data["data"])
        df.attrs["symbol"] = data["instId"]
        contract = await asyncio.to_thread(Contract.from_dataframe, df)
        if len(contract) != data["limit"]:
            self.logger.error(
                f"Data length for {data['instId']} is not {data['limit']}"
            )
        return contract

    async def fetch_and_process(
        self, instId: str, bar_interval: str, limit: int
    ) -> Dict[str, Any]:
        data = await self.fetch(instId, bar_interval, limit)
        return await self.process(data)
