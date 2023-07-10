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
from datetime import date, datetime
from typing import ClassVar, Optional

from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_mixin import StatementType

# do not import PySpark here, as we allow user to use `Clickhouse.package` for creating Spark session


log = logging.getLogger(__name__)


class Clickhouse(JDBCConnection):
    """Clickhouse JDBC connection. |support_hooks|

    Based on Maven package ``ru.yandex.clickhouse:clickhouse-jdbc:0.3.2``
    (`official Clickhouse JDBC driver <https://github.com/ClickHouse/clickhouse-jdbc>`_).

    .. dropdown:: Version compatibility

        * Clickhouse server versions: 20.7 or higher
        * Spark versions: 2.3.x - 3.4.x
        * Java versions: 8 - 17

        See `official documentation <https://clickhouse.com/docs/en/integrations/java#jdbc-driver>`_.

    .. warning::

        To use Clickhouse connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.4.1  # pass specific PySpark version

        See :ref:`spark-install` instruction for more details.

    Parameters
    ----------
    host : str
        Host of Clickhouse database. For example: ``test.clickhouse.domain.com`` or ``193.168.1.11``

    port : int, default: ``8123``
        Port of Clickhouse database

    user : str
        User, which have proper access to the database. For example: ``some_user``

    password : str
        Password for database connection

    database : str, optional
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"continueBatchOnError": "false"}``.

        See `Clickhouse JDBC driver properties documentation
        <https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc#configuration>`_
        for more details

    Examples
    --------

    Clickhouse connection initialization

    .. code:: python

        from onetl.connection import Clickhouse
        from pyspark.sql import SparkSession

        extra = {"continueBatchOnError": "false"}

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Clickhouse.package)
            .getOrCreate()
        )

        clickhouse = Clickhouse(
            host="database.host.or.ip",
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
        extra = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))

        if self.database:
            return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

        return f"jdbc:clickhouse://{self.host}:{self.port}?{parameters}".rstrip("?")

    class Dialect(JDBCConnection.Dialect):
        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            result = value.strftime("%Y-%m-%d %H:%M:%S")
            return f"CAST('{result}' AS DateTime)"

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            result = value.strftime("%Y-%m-%d")
            return f"CAST('{result}' AS Date)"

    class ReadOptions(JDBCConnection.ReadOptions):
        @classmethod
        def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"modulo(halfMD5({partition_column}), {num_partitions})"

        @classmethod
        def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"{partition_column} % {num_partitions}"

    ReadOptions.__doc__ = JDBCConnection.ReadOptions.__doc__

    @staticmethod
    def _build_statement(
        statement: str,
        statement_type: StatementType,
        jdbc_connection,
        statement_args,
    ):
        # Clickhouse does not support prepared statements, as well as calling functions/procedures
        return jdbc_connection.createStatement(*statement_args)
