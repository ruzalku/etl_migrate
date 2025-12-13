from ..db.abstracts.db import AbstractStorage
import json


class Mapping:
    def __init__(self, file_path: str, storage: AbstractStorage):
        self.file_path = file_path
        self.storage = storage

    async def save_json_mapping(self, indexes: tuple):
        mapping = await self.storage.create_mapping(indexes)

        with open(self.file_path, 'w', encoding='utf-8') as file:
            json.dump(mapping, file, ensure_ascii=False, indent=2)

    def get_json_mapping(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                mapping = json.load(file)

            return mapping
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


    def _extract_row(self, column: dict, column_name: str):
        column['column'] = column_name
        return column

