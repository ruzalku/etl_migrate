from abc import ABC
from abc import abstractmethod
from typing import TypeVar, Generic


T = TypeVar('T')


class AbstractStorage(ABC, Generic[T]):
    def __init__(self, client: T):
        self.client = client

    @abstractmethod
    async def get_objs(self, index):
        pass

    @abstractmethod
    async def save_objs(self, objs, index):
        pass

    @abstractmethod
    async def create_index(self, index, mapping):
        pass

    @abstractmethod
    async def create_mapping(self, index):
        pass
