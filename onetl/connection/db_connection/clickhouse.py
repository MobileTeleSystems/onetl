from __future__ import annotations

import logging
from datetime import date, datetime
from typing import ClassVar, Optional

from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_mixin import StatementType

log = logging.getLogger(__name__)


class Clickhouse(JDBCConnection):
    """Class for Clickhouse JDBC connection.

    Based on Maven package ``ru.yandex.clickhouse:clickhouse-jdbc:0.3.2``
    (`official Clickhouse JDBC driver <https://github.com/ClickHouse/clickhouse-jdbc>`_)

    .. note::

        Supported Clickhouse server versions: >= 20.7

    Parameters
    ----------
    host : str
        Host of Clickhouse database. For example: ``test.clickhouse.domain.com`` or ``193.168.1.11``

    port : int, default: ``8123``
        Port of Clickhouse database

    user : str
        User, which have access to the database and table. For example: ``some_user``

    password : str
        Password for database connection

    database : str, optional
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"continueBatchOnError": "false"}``.

        See `Clickhouse JDBC driver properties documentation
        <https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc#configuration>`_
        for more details

    Examples
    --------

    Clickhouse connection initialization

    .. code::

        from onetl.connection import Clickhouse
        from mtspark import get_spark

        extra = {"continueBatchOnError": "false"}

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Clickhouse.package],
        })

        clickhouse = Clickhouse(
            host="database.host.or.ip,
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    port: int = 8123
    database: Optional[str] = None

    driver: ClassVar[str] = "ru.yandex.clickhouse.ClickHouseDriver"
    package: ClassVar[str] = "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"

    @property
    def jdbc_url(self) -> str:
        parameters = "&".join(f"{k}={v}" for k, v in sorted(self.extra.dict(by_alias=True).items()))

        if self.database:
            return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

        return f"jdbc:clickhouse://{self.host}:{self.port}?{parameters}".rstrip("?")

    class ReadOptions(JDBCConnection.ReadOptions):
        @classmethod
        def partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"modulo(halfMD5({partition_column}), {num_partitions})"

        @classmethod
        def partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"{partition_column} % {num_partitions}"

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
