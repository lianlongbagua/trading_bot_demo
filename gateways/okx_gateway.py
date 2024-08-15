import asyncio
from typing import Any, Dict

from okx import MarketData
from tenacity import retry, wait_random_exponential

from Core import utils
from Core.datas import Contract
from .base import BaseGateway


class OKXGateway(BaseGateway):
    def __init__(self):
        super().__init__()
        self.market_api = MarketData.MarketAPI(debug=False)

    @retry(
        wait=wait_random_exponential(multiplier=1),
    )
    async def fetch(self, instId, bar, limit) -> Dict[str, Any]:
        try:
            result = await asyncio.to_thread(
                self.market_api.get_candlesticks,
                instId=instId,
                bar=bar,
                limit=limit,
            )
            result.update({"instId": instId, "bar": bar, "limit": limit})
            return result
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {bar} candles for {instId} - {str(e)} - retrying..."
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
