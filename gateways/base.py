from abc import ABC, abstractmethod
from typing import Any, Dict

from trader.logger import LoggedClass


class BaseGateway(LoggedClass, ABC):
    def __init__(self):
        super().__init__(__name__)
        self.logger.info(f"Initializing {self.__class__.__name__} gateway")

    @abstractmethod
    async def fetch(self) -> Dict[str, Any]:
        """
        Fetch data from the source.

        Returns:
            Dict[str, Any]: The fetched data
        """
        pass

    @abstractmethod
    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process the input data.

        Args:
            data (Dict[str, Any]): The input data to process

        Returns:
            Dict[str, Any]: The processed data
        """
        pass


class FetchingError(Exception):
    pass


class ProcessingError(Exception):
    pass
