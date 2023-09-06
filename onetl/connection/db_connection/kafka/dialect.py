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

from onetl._util.spark import get_spark_version
from onetl.base import BaseDBConnection
from onetl.connection.db_connection.db_connection.dialect import DBDialect
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsNone,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportHWMExpressionNone,
    SupportNameAny,
    SupportWhereNone,
)

log = logging.getLogger(__name__)


class KafkaDialect(  # noqa: WPS215
    SupportColumnsNone,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportWhereNone,
    SupportNameAny,
    SupportHWMExpressionNone,
    DBDialect,
):
    valid_hwm_columns = {"offset", "timestamp"}

    @classmethod
    def validate_hwm_column(
        cls,
        connection: BaseDBConnection,
        hwm_column: str | None,
    ) -> str | None:
        if not isinstance(hwm_column, str):
            raise ValueError(
                f"{connection.__class__.__name__} requires 'hwm_column' parameter type to be 'str', "
                f"got {type(hwm_column)}",
            )

        cls.validate_column(connection, hwm_column)

        return hwm_column

    @classmethod
    def validate_column(cls, connection: BaseDBConnection, column: str) -> None:
        if column not in cls.valid_hwm_columns:
            raise ValueError(f"{column} is not a valid hwm column. Valid options are: {cls.valid_hwm_columns}")

        if column == "timestamp":
            # Spark version less 3.x does not support reading from Kafka with the timestamp parameter
            spark_version = get_spark_version(connection.spark)  # type: ignore[attr-defined]
            if spark_version.major < 3:
                raise ValueError(
                    f"Spark version must be 3.x for the timestamp column. Current version is: {spark_version}",
                )
