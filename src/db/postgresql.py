from ..db.abstracts.db import AbstractStorage
from psycopg import AsyncConnection
from ..service.extractor import DataExtractor
from ..core.backoff import backoff


class PostgreStorage(AbstractStorage[AsyncConnection]):
    @backoff()
    async def create_mapping(self, indexes: tuple):
        async with self.client:
            async with self.client.cursor() as cursor:
                await cursor.execute("""
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.table_name,
                        tc.constraint_type
                    FROM information_schema.columns AS c
                    LEFT JOIN information_schema.key_column_usage AS kcu
                      ON  c.table_schema = kcu.table_schema
                      AND c.table_name  = kcu.table_name
                      AND c.column_name = kcu.column_name
                    LEFT JOIN information_schema.table_constraints AS tc
                      ON  tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema    = kcu.table_schema
                    WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
                        AND c.table_name = ANY(%s)
                    ORDER BY
                        c.table_schema,
                        c.table_name,
                        c.ordinal_position;
                    """, (list(indexes), ))
                
                data = await cursor.fetchall()

        return DataExtractor.extract_mapping(data)
    
    @backoff()
    async def get_objs(self, index):
        return await super().get_objs(index)
    
    @backoff()
    async def save_objs(self, objs, index):
        return await super().save_objs(objs, index)
    
    @backoff()
    async def create_index(self, index, mapping):
        return await super().create_index(index, mapping)
