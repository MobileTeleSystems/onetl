#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from __future__ import annotations

import logging

from etl_entities import Column

from onetl.connection.db_connection.db_connection import BaseDBConnection, DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsNone,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportHWMExpressionNone,
    SupportTableWithoutDBSchema,
    SupportWhereNone,
)

log = logging.getLogger(__name__)


class KafkaDialect(  # noqa: WPS215
    SupportColumnsNone,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportWhereNone,
    SupportTableWithoutDBSchema,
    SupportHWMExpressionNone,
    DBConnection.Dialect,
):
    valid_hwm_columns = {"offset", "timestamp"}

    @classmethod
    def validate_hwm_column(
        cls,
        connection: BaseDBConnection,
        hwm_column: str | tuple[str, str] | Column | None,
    ) -> str | tuple[str, str] | Column | None:
        if isinstance(hwm_column, str):
            cls.validate_single_column(connection, hwm_column)
        elif isinstance(hwm_column, tuple):
            cls.validate_tuple_columns(connection, hwm_column)
        elif isinstance(hwm_column, Column):
            cls.validate_column_class(connection, hwm_column)

        return hwm_column

    @classmethod
    def validate_single_column(cls, connection: BaseDBConnection, column: str) -> None:
        cls.validate_column(connection, column)

    @classmethod
    def validate_tuple_columns(cls, connection: BaseDBConnection, columns: tuple[str, str]) -> None:
        for column in columns:
            cls.validate_column(connection, column)

    @classmethod
    def validate_column_class(cls, connection: BaseDBConnection, column: Column) -> None:
        cls.validate_column(connection, column.name)

    @classmethod
    def validate_column(cls, connection: BaseDBConnection, column: str) -> None:
        if column not in cls.valid_hwm_columns:
            raise ValueError(f"{column} is not a valid hwm column. Valid options are: {cls.valid_hwm_columns}")
        if column == "timestamp":
            cls.check_spark_version(connection)

    @staticmethod
    def check_spark_version(connection: BaseDBConnection) -> None:
        spark_version = connection.spark.version  # type: ignore[attr-defined]
        major_version = int(spark_version.split(".")[0])

        if major_version < 3:
            raise ValueError(f"Spark version must be 3.x for the timestamp column. Current version is: {spark_version}")
