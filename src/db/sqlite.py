from aiosqlite import Connection as AsyncConnection
import logging
import re

from ..db.abstracts.db import AbstractStorage
from ..service.extractor import DataExtractor


logger = logging.getLogger(__name__)


class SQLiteStorage(AbstractStorage[AsyncConnection]):
    async def save_objs(self, objs: list[dict], index: str):
        if not objs:
            logger.warning(f"Index {index} is null")
            return

        columns = list(objs[0].keys())
        cols_sql = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO {index} ({cols_sql}) VALUES ({placeholders})"

        values = [tuple(row[c] for c in columns) for row in objs]
        await self.client.executemany(sql, values)
        await self.client.commit()

    
    async def get_objs(self, index, batch: int):
        return await self._get_objs_from_stream(index, batch)
    
    async def _get_objs_from_stream(self, index: str, batch: int):
        async with self.client.execute(f"SELECT * FROM {index};") as cursor:
            while True:
                rows = await cursor.fetchmany(batch)
                if not rows:
                    break
                yield [dict(r) for r in rows]
    
    async def create_mapping(self, indexes: tuple[str]) -> dict:
        name_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

        raw_rows: list[dict] = []

        for table in indexes:
            if not name_re.match(table):
                raise ValueError(f"Bad table name: {table}")

            async with self.client.execute(f"PRAGMA table_info({table});") as cur:
                cols = await cur.fetchall()

            cols = [dict(r) for r in cols]

            unique_cols: set[str] = set()

            async with self.client.execute(f"PRAGMA index_list({table});") as cur:
                idx_list = [dict(r) for r in await cur.fetchall()]

            for idx in idx_list:
                if idx.get("unique") != 1:
                    continue
                idx_name = idx["name"]

                async with self.client.execute(f"PRAGMA index_info({idx_name});") as cur:
                    idx_cols = [dict(r) for r in await cur.fetchall()]
                for ic in idx_cols:
                    if ic.get("name"):
                        unique_cols.add(ic["name"])

            for c in cols:
                col_name = c["name"]
                col_type = c.get("type")
                pk = c.get("pk", 0)

                constraint = None
                if pk and pk > 0:
                    constraint = "PRIMARY KEY"
                elif col_name in unique_cols:
                    constraint = "UNIQUE"

                raw_rows.append({
                    "table_name": table,
                    "column_name": col_name,
                    "data_type": col_type,
                    "constraint_type": constraint,
                })

        return DataExtractor.extract_mapping(raw_rows)