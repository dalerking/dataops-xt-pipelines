from datetime import datetime
from elasticsearch import Elasticsearch
from enum import Enum
from src.pipelines.common.usx_exception import USXException

import boto3, json, re, urllib3
import pandas as pd


class XTDataPipelineConversion(Enum):
    Date = 1
    ESTimestamp = 2
    EventTime = 3
    Flag = 4
    GeoPoint = 5
    Integer = 6
    Other = 7
    Radius = 8
    TimeinMs = 9


class XTDataPipelineColumn:
    def __init__(self, column: dict) -> None:
        self.combine = None
        self.json_path = None
        self.name = None
        self.replace = None
        self.conversion = XTDataPipelineConversion.Other

        if "combine" in column:
            self.combine = column["combine"]

        if "conversion" in column:
            self.conversion = column["conversion"]

        if "json_path" in column:
            self.json_path = column["json_path"]

        if "name" in column:
            self.name = column["name"]

        if "replace" in column:
            self.replace = column["replace"]

    def get_combine(self) -> str:
        return self.combine

    def get_conversion(self) -> XTDataPipelineConversion:
        return self.conversion

    def get_json_path(self) -> str:
        return self.json_path

    def get_name(self) -> str:
        return self.name

    def get_replace(self) -> str:
        return self.replace

    def is_combine(self) -> bool:
        return True if self.combine is not None else False

    def is_date_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.Date else False

    def is_es_timestamp_conversion(self) -> bool:
        return (
            True if self.conversion == XTDataPipelineConversion.ESTimestamp else False
        )

    def is_event_time_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.EventTime else False

    def is_flag_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.Flag else False

    def is_geo_point_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.GeoPoint else False

    def is_integer_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.Integer else False

    def is_other(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.Other else False

    def is_json_path(self) -> bool:
        return True if self.json_path is not None else False

    def is_radius_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.Radius else False

    def is_replace(self) -> bool:
        return True if self.replace is not None else False

    def is_time_in_ms_conversion(self) -> bool:
        return True if self.conversion == XTDataPipelineConversion.TimeinMs else False


class XTDataPipeline:
    def __init__(self, attributes: dict, search_size: int) -> None:
        self.dataframe = {}
        self.search_size = search_size
        self.successful = True

        if "api_index" in attributes:
            self.api_index = attributes["api_index"]
        else:
            raise USXException("Missing api_index attribute")

        self.columns = {}
        if "columns" in attributes:
            for column in attributes["columns"]:
                self.columns[column["name"]] = XTDataPipelineColumn(column)
        else:
            raise USXException("Missing columns attribute")

        if "db_table_name" in attributes:
            self.db_table_name = attributes["db_table_name"]
        else:
            raise USXException("Missing db_table_name attribute")

        self.month_map = {
            "Jan": "01",
            "Feb": "02",
            "Mar": "03",
            "Apr": "04",
            "May": "05",
            "Jun": "06",
            "Jul": "07",
            "Aug": "08",
            "Sep": "09",
            "Oct": "10",
            "Nov": "11",
            "Dec": "12",
        }

        secret = self.get_secret()
        self.api_password = secret["api_password"]
        self.api_server = secret["api_server"]
        self.api_user = secret["api_user"]

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _add_to_dataframe(self, column_name: str, data: str) -> None:
        if column_name not in self.dataframe:
            self.dataframe[column_name] = []

        self.dataframe[column_name].append(data)

    def _get_columns(self) -> dict:
        return self.columns

    def _get_column_value(
        self, api_row: dict, column: XTDataPipelineColumn, column_name: str
    ) -> str:
        column_value = None

        if column.is_json_path():
            json_path = column.get_json_path()
            json_path_level_list = json_path.split(".")

            if len(json_path_level_list):
                element = api_row

                for json_path_level in json_path_level_list:
                    if element is not None:
                        if json_path_level in element:
                            element = element[json_path_level]
                        else:
                            element = None

                if element is not None:
                    column_value = element

            elif column_name in api_row:
                column_value = api_row[column_name]

        else:
            if column_name in api_row:
                column_value = api_row[column_name]

        return column_value

    def _get_dataframe(self):
        return pd.DataFrame(self.dataframe)

    def get_secret(self) -> dict:
        secret_retries = 5

        client = boto3.client("secretsmanager")

        for _ in range(secret_retries):
            response = client.get_secret_value(SecretId="xt-elasticsearch-creds")
            return json.loads(response["SecretString"])

        raise RuntimeError("Failed to connect to secretsmanager, retries exceeded")

    def handleException(self, exception) -> None:
        if isinstance(exception, KeyError):
            print(f"KeyError:  {str(exception)}")
        elif isinstance(exception, USXException):
            print(exception.get_message())
        else:
            print(str(exception))

        self.successful = False

    def insert_row(self, cursor, api_row: dict) -> None:
        first = True
        sql = (
            f"INSERT INTO {self.db_table_name} ("
            + ",".join(self._get_columns())
            + ") VALUES ("
        )

        for column_name, column in self._get_columns().items():
            column_value = self._get_column_value(api_row, column, column_name)

            if not first:
                sql = sql + ","

            # self._add_to_dataframe(
            #     column_name, self.translate(api_row, column, column_value)
            # )
            sql = sql + self.translate(api_row, column, column_value)

            first = False
        sql = sql + ")"

        if cursor is None:
            print(sql, flush=True)
        else:
            cursor.execute(sql)

    def search(self, sort_and_query: str, timestamp, subsearch=None) -> list:
        api_row_list = []

        elastic_search = Elasticsearch(
            self.api_server,
            http_auth=(self.api_user, self.api_password),
            verify_certs=False,
            ssl_show_warn=False,
        )

        if timestamp is None or timestamp == 0:
            response = elastic_search.search(
                index=self.api_index,
                size=self.search_size,
                query={"match_all": {}},
                sort=sort_and_query,
            )
        else:
            response = elastic_search.search(
                index=self.api_index,
                size=self.search_size,
                query={"range": {sort_and_query: {"gt": timestamp}}},
                sort=sort_and_query,
            )

        # Parse the response
        # print(str(response)[0:5000])
        # apiGetTotal = response["_shards"]["total"]
        # apiGetSuccessful = response["_shards"]["successful"]
        # apiGetSkipped = response["_shards"]["skipped"]
        # apiGetFailed = response["_shards"]["failed"]
        # print(
        #     f"  Total ({apiGetTotal:,}); successful ({apiGetSuccessful:,}); skipped ({apiGetSkipped:,}); failed ({apiGetFailed:,})"
        # )

        api_rows = response["hits"]["hits"]

        for api_row in api_rows:
            api_row_list.append(api_row["_source"])

        if subsearch is not None:
            api_row_list_save = api_row_list
            api_row_list = []

            for api_row in api_row_list_save:
                if subsearch in api_row:
                    subsearch_list = api_row["load_roll_reasons"]
                    del api_row["load_roll_reasons"]

                    for subsearch_element in subsearch_list:
                        api_row_list.append(api_row | subsearch_element)

        return api_row_list

    def translate(
        self, api_row: dict, column: XTDataPipelineColumn, column_value
    ) -> str:
        if column.is_replace():
            replace = column.get_replace()

            if replace in api_row:
                column_value = api_row[replace]

        if column.is_combine():
            combine = column.get_combine()

            if column_value is not None:
                column_value = ",".join(value[combine] for value in column_value)

        if column.is_date_conversion():
            if column_value is not None:
                elements = column_value.split()

                if len(elements) == 6:
                    elements[1] = self.month_map[elements[1]]
                    elements[2] = elements[2].zfill(2)
                    column_value = (
                        f"{elements[5]}-{elements[1]}-{elements[2]} {elements[3]}"
                    )
                else:
                    elements = column_value.split("/")

                    if len(elements) == 3:
                        elements[0] = elements[0].zfill(2)
                        elements[1] = elements[1].zfill(2)

                        if int(elements[2]) < 2000:
                            elements[2] = f"20{elements[2]}"

                        column_value = (
                            f"{elements[2]}-{elements[0]}-{elements[1]} 00:00:00"
                        )
        elif column.is_es_timestamp_conversion():
            column_value = re.sub("T", " ", column_value)

            elements = column_value.split(".")
            if len(elements) > 1:
                column_value = f"{elements[0]}.{elements[1][0:3]}"
        elif column.is_event_time_conversion():
            column_value = re.sub(
                "\+.*",
                "",
                re.sub(
                    "\..*",
                    "",
                    re.sub(
                        "T",
                        " ",
                        column_value,
                    ),
                ),
            )
        elif column.is_flag_conversion():
            if column_value is None:
                column_value = "0"
            elif isinstance(column_value, bool):
                if column_value == 0:
                    column_value = "0"
                else:
                    column_value = "1"
            else:
                column_value = str(column_value)
        elif column.is_geo_point_conversion():
            if column_value is not None:
                column_value = f"geography::Point({column_value},4326)"
        elif column.is_integer_conversion():
            if column_value is not None:
                column_value = int(column_value)
        elif column.is_radius_conversion():
            if column_value is not None:
                if isinstance(column_value, str):
                    column_value = int(re.sub(" mi", "", column_value))
        elif column.is_time_in_ms_conversion():
            if column_value is not None:
                new_data1 = str(datetime.fromtimestamp(int(column_value) / 1000.0))
                column_value = re.sub("\..*", "", new_data1)
                column_value = new_data1
        if column_value is None:
            return "null"
        elif isinstance(column_value, bool):
            return "'" + str(column_value) + "'"
        elif isinstance(column_value, float):
            return str(column_value)
        elif isinstance(column_value, int):
            return str(column_value)
        elif isinstance(column_value, str):
            new_data1 = column_value.strip().replace("'", "").replace("\n", "")
            new_data2 = re.sub("\U0001F602", "", new_data1)
            return "'" + new_data2 + "'"
        else:
            return column_value
