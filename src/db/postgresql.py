import logging
from psycopg.errors import UndefinedColumn

from ..db.abstracts.db import AbstractStorage
from psycopg import AsyncConnection
from ..service.extractor import DataExtractor
from ..core.backoff import backoff


logger = logging.getLogger(__name__)


class PostgreStorage(AbstractStorage[AsyncConnection]):
    @backoff()
    async def create_mapping(self, indexes: tuple):
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
    async def get_objs(self, index: str, batch: int):
        return self._get_objs_stream(index, batch)

    async def _get_objs_stream(self, index: str, batch: int):
        async with self.client.cursor() as cursor:
            await cursor.execute(f"SELECT * FROM {index};")
            logger.info(
                f'Get data from {index}'
            )
            while True:
                rows = await cursor.fetchmany(batch)
                if not rows:
                    break
                yield rows
    
    @backoff()
    async def save_objs(self, objs: list[dict], index: str):
        if not objs:
            logger.warning(f'Index: {index} is null')
            return

        columns = list(objs[0].keys())
        cols_sql = ", ".join(columns)
        copy_sql = f"COPY {index} ({cols_sql}) FROM STDIN"
        try:
            async with self.client.cursor() as cursor:
                async with cursor.copy(copy_sql) as copy:
                        for row in objs:
                            await copy.write_row([row[c] for c in columns])
        except UndefinedColumn as es:
            await self.client.rollback()
            logger.error(
                f'Error in {__name__}, Error: {es}'
            )
            return

        logger.info(f'Index: {index} is saved')
