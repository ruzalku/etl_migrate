from ..db.abstracts.db import AbstractStorage


class Mapping:
    def __init__(self, file_path: str, storage: AbstractStorage):
        self.file_path = file_path
        self.storage = storage

    def save_json_mapping():
        # TODO: Сделать сохранение mapping в файл
        pass

    def get_json_mapping():
        # TODO: Сделать получение mapping-а из json-файла
        pass
