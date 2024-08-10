import asyncio
from typing import Any, Dict, List

from okx import MarketData
from tenacity import retry, wait_random_exponential

from Core import utils
from Core.datas import Contract
from .base import BaseGateway


class OKXGateway(BaseGateway):
    def __init__(self):
        super().__init__()
        self.market_api = MarketData.MarketAPI(debug=False)

    def get_logger(self):
        return self.logger

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
            result.update({"instId": instId, "bar": bar})
            # self.logger.info(
            #     f"Fetched {len(result['data'])} candles for {bar} timeframe"
            # )
            return result
        except Exception as e:
            self.logger.warning(
                f"Failed to fetch {bar} candles for {instId} - {str(e)} - retrying..."
            )
            raise

    async def fetch_multiple(
        self, instId: str, bars: List[str], limit: int
    ) -> Dict[str, Dict[str, Any]]:
        tasks = [self.fetch(instId, bar, limit) for bar in bars]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            instId + "_" + bar: result
            for bar, result in zip(bars, results)
            if not isinstance(result, Exception)
        }

    @retry(wait=wait_random_exponential(multiplier=1, min=1, max=10))
    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Wrap the processing in an asynchronous function
        df = await asyncio.to_thread(utils.list_to_df, data["data"])
        df.attrs["symbol"] = data["instId"]
        contract = await asyncio.to_thread(Contract.from_dataframe, df)
        return contract

    async def process_multiple(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        tasks = [self.process(data) for data in data.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            data["instId"] + "_" + data["bar"]: result
            for data, result in zip(data.values(), results)
            if not isinstance(result, Exception)
        }
