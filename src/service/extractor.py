class DataExtractor:
    def get_index_mapping(self, index):
        # TODO: Сделать получение данных о mapping-е
        pass

    def extract_data(self, data, index):
        # TODO: Сделать преобразование данных на основе mapping-а
        pass

    @staticmethod
    def extract_mapping(mapping: list[dict]):
        new_mapping: dict[dict] = {}

        for column in mapping:
            table_name = column.get('table_name')
            column_name = column.get('column_name')

            if table_name not in new_mapping:
                new_mapping[table_name] = {}

            new_mapping[table_name][column_name] = {
                'data_type': column.get('data_type'),
                'constraint_type': column.get('constraint_type'),
            }

        return new_mapping
