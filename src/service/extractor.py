from ..service.mapping import Mapping


class DataExtractor:
    @staticmethod
    def extract_data(data, index, file_path):
        mapping = Mapping(file_path=file_path)

        columns = mapping.get_json_mapping().get(index, {})
        new_data = []
        for i in data:
            row = {}
            for column, col_data in columns.items():
                row[col_data['new_column_name']] = i[column]

            new_data.append(row)
        
        return new_data

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
                'new_column_name': column_name
            }

        return new_mapping
