from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta
from logging import getLogger
from random import randint
from urllib import parse as parser

import pandas
from pymongo import MongoClient

from tests.fixtures.processing.base_processing import BaseProcessing

logger = getLogger(__name__)


class MongoDBProcessing(BaseProcessing):
    _column_types_and_names_matching = {
        "_id": "",
        "text_string": "",
        "hwm_int": "",
        "hwm_datetime": "",
        # https://groups.google.com/g/mongodb-user/c/_Jj_yM_EQqM/m/EgRSl3HSpJYJ
        # PyMongo doesn't support saving date instances. So there is no 'hwm_date' field.
        # TODO(@dypedchenk) fix datetime in mongo (After spark reading add 3 hours).
        "float_value": "",
    }

    column_names: list = ["_id", "text_string", "hwm_int", "hwm_datetime", "float_value"]

    def __enter__(self):
        self.connection = self.get_conn()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.connection.close()
        return False

    @property
    def user(self) -> str:
        return os.environ["ONETL_MONGO_USER"]

    @property
    def password(self) -> str:
        return os.environ["ONETL_MONGO_PASSWORD"]

    @property
    def host(self) -> str:
        return os.environ["ONETL_MONGO_HOST"]

    @property
    def database(self) -> str:
        return os.environ["ONETL_MONGO_DB"]

    @property
    def port(self) -> int:
        return int(os.environ["ONETL_MONGO_PORT"])

    @property
    def schema(self) -> str:
        # Mongo does not support schemas
        return ""

    @property
    def url(self) -> str:
        return f"mongodb://{self.user}:{parser.quote(self.password)}@{self.host}:{self.port}"

    def get_conn(self):
        return MongoClient(self.url)

    def create_schema_ddl(self, schema: str) -> str:
        # Mongo does not support DDL
        return ""

    def create_schema(self, schema: str) -> None:
        # Mongo does not support schemas
        pass

    def create_table_ddl(self, table: str, fields: dict[str, str], schema: str) -> str:
        # Mongo does not support DDL
        return ""

    def create_table(self, table: str, fields: dict[str, str], schema: str) -> None:
        # Mongo creates collections automatically
        pass

    def drop_database_ddl(self, schema: str) -> str:
        # Mongo does not support DDL
        return ""

    def drop_table_ddl(self, table: str, schema: str) -> str:
        # Mongo does not support DDL
        return ""

    def drop_database(
        self,
        schema: str,
    ) -> None:
        pass

    def drop_table(self, table: str, schema: str) -> None:
        # Mongo does not support DDL
        pass

    def insert_data(self, schema: str, table: str, values: list) -> None:
        list_to_insert = []
        values_as_records = values.to_numpy()

        for record in values_as_records:
            dict_as_record = {}
            for pos, column_name in enumerate(self.column_names):
                dict_as_record[column_name] = record[pos]
            list_to_insert.append(dict_as_record)

        db = self.connection[self.database]
        records = db[table]
        records.insert_many(list_to_insert)

    def get_expected_dataframe_ddl(self, schema: str, table: str, order_by: str | None = None) -> str:
        # Mongo does not support DDL
        return ""

    def get_expected_dataframe(
        self,
        schema: str,
        table: str,
        order_by: str | None = None,
    ) -> pandas.DataFrame:
        db = self.connection[self.database]
        records = db[table]
        return pandas.DataFrame(list(records.find()))

    @staticmethod
    def current_datetime() -> datetime:
        return datetime.now()

    def create_pandas_df(self, min_id: int = 1, max_id: int | None = None) -> pandas.DataFrame:
        max_id = self._df_max_length if not max_id else max_id
        time_multiplier = 100000

        values = defaultdict(list)
        for i in range(min_id, max_id + 1):
            for column_name in self.column_names:
                column_name = column_name.lower()

                if column_name == "_id" or "int" in column_name:
                    values[column_name].append(i)
                elif "float" in column_name:
                    values[column_name].append(float(f"{i}.{i}"))
                elif "text" in column_name:
                    values[column_name].append("This line is made to test the work")
                elif "datetime" in column_name:
                    rand_second = randint(0, i * time_multiplier)  # noqa: S311
                    now = self.current_datetime() + timedelta(seconds=rand_second)
                    # In the case that after rounding the result
                    # will not be in the range from 0 to 999999
                    microsecond = min(round(now.microsecond, -3), 999999)
                    now = now.replace(microsecond=microsecond)  # save milliseconds
                    values[column_name].append(now)

        return pandas.DataFrame(data=values)
