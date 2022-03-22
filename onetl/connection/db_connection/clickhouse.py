from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar

from onetl.connection.db_connection.jdbc_connection import JDBCConnection, StatementType

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Clickhouse(JDBCConnection):
    """Class for Clickhouse jdbc connection.

    Parameters
    ----------
    host : str
        Host of clickhouse database. For example: ``clickhouse-sbl-dev.msk.bd-cloud.mts.ru``

    port : int, default: ``8123``
        Port of clickhouse database

    user : str
        User, which have access to the database and table. For example: ``TECH_ETL``

    password : str
        Password for database connection

    database : str
        Database in rdbms. To provide schema, use DBReader class

    spark : pyspark.sql.SparkSession
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"ssl": True, "sslmode": "none"}``.

    Examples
    --------

    Clickhouse jdbc connection initialization

    .. code::

        from onetl.connection import Clickhouse
        from mtspark import get_spark

        extra = {"ssl": True, "sslmode": "none"}

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Clickhouse.package],
        })

        clickhouse = Clickhouse(
            host="clickhouse-sbl-dev.msk.bd-cloud.mts.ru",
            user="TECH_ETL",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    driver: ClassVar[str] = "ru.yandex.clickhouse.ClickHouseDriver"
    package: ClassVar[str] = "ru.yandex.clickhouse:clickhouse-jdbc:0.3.0"
    port: int = 8123

    @property
    def jdbc_url(self) -> str:
        parameters = "&".join(f"{k}={v}" for k, v in self.extra.items())

        if self.database:
            return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

        return f"jdbc:clickhouse://{self.host}:{self.port}?{parameters}".rstrip("?")

    @staticmethod
    def _build_statement(
        statement: str,
        statement_type: StatementType,
        jdbc_connection,
        statement_args,
    ):
        # Clickhouse does not support prepared statements, as well as calling functions/procedures
        return jdbc_connection.createStatement(*statement_args)

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST('{result}' AS DateTime)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"CAST('{result}' AS Date)"
