from abc import ABC
from abc import abstractmethod
from typing import TypeVar, Generic


T = TypeVar('T')


class AbstractStorage(ABC, Generic[T]):
    def __init__(self, client: T):
        self.client = client

    @abstractmethod
    async def get_objs(self, index) -> list:
        pass

    @abstractmethod
    async def save_objs(self, objs, index) -> None:
        pass

    @abstractmethod
    async def create_index(self, index, mapping) -> None:
        pass

    @abstractmethod
    async def create_mapping(self, index: str) -> dict:
        # Должен вернуть словарь вида:
        # mapping = {
        #   "table_name": {
        #       "column1": {
        #           "data_type": "integer",
        #           "is_primary": True
        #       },
        #       ....
        #   }
        # }
        pass
