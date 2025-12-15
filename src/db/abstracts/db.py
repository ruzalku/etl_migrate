from abc import ABC
from abc import abstractmethod
from typing import TypeVar, Generic


T = TypeVar('T')


class AbstractStorage(ABC, Generic[T]):
    def __init__(self, client: T):
        self.client = client

    @abstractmethod
    async def get_objs(self, index: str, batch: int) -> list:
        pass

    @abstractmethod
    async def save_objs(self, objs: list[dict], index: str) -> None:
        pass

    @abstractmethod
    async def create_mapping(self, indexes: list) -> dict:
        pass
