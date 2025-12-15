from elasticsearch import AsyncElasticsearch

from ..db.abstracts.db import AbstractStorage


class ElasticStorage(AbstractStorage[AsyncElasticsearch]):
    async def create_mapping(self, indexes):
        return await super().create_mapping(indexes)
    
    async def get_objs(self, index, batch):
        return await self._get_objs_from_stream(index, batch)
    
    async def save_objs(self, objs, index):
        return await super().save_objs(objs, index)
    
    async def _get_objs_from_stream(self, index: str, batch: int):
        pass