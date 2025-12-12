from abc import ABC
from abc import abstractmethod
from typing import TypeVar, Generic


T = TypeVar('T')


class DB(ABC, Generic[T]):
    def __init__(self, client: T):
        self.client = client

    @abstractmethod
    def get_objs(self, index):
        pass

    @abstractmethod
    def save_objs(self, objs, index):
        pass

    @abstractmethod
    def create_index(self, index, mapping):
        pass
