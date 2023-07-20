from __future__ import annotations

import logging

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
            cls._check_spark_version(connection)

    @staticmethod
    def _check_spark_version(connection: BaseDBConnection) -> None:
        spark_version = connection.spark.version  # type: ignore[attr-defined]
        major_version = int(spark_version.split(".")[0])

        if major_version < 3:
            raise ValueError(f"Spark version must be 3.x for the timestamp column. Current version is: {spark_version}")
