from ..service.mapping import Mapping


class DataExtractor:
    @staticmethod
    def extract_data(data: list[dict], index: str, file_path: str) -> list[dict]:
        mapping = Mapping(file_path=file_path).get_json_mapping()

        table_mapping: dict = mapping.get(index, {})
        new_table_name = table_mapping.get("new_table_name", index)

        new_data: list[dict] = []
        for row in data:
            new_row: dict = {}
            for old_col, meta in table_mapping.items():
                if old_col == "new_table_name":
                    continue
                new_col = meta.get("new_column_name", old_col)
                new_row[new_col] = row.get(old_col)
            new_row["new_table_name"] = new_table_name
            new_data.append(new_row)

        return new_data

    @staticmethod
    def extract_mapping(rows: list[dict]) -> dict:
        new_mapping: dict = {}

        for r in rows:
            table_name = r.get("table_name")
            column_name = r.get("column_name")
            if not table_name or not column_name:
                continue

            if table_name not in new_mapping:
                new_mapping[table_name] = {}

            new_mapping[table_name][column_name] = {
                "data_type": r.get("data_type"),
                "constraint_type": r.get("constraint_type"),
                "new_column_name": column_name,
            }

        for table_name, table_dict in new_mapping.items():
            table_dict["new_table_name"] = table_name

        return new_mapping
