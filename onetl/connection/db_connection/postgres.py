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

from datetime import date, datetime
from typing import ClassVar

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsList,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportHWMExpressionStr,
    SupportWhereStr,
)
from onetl.connection.db_connection.dialect_mixins.support_table_with_dbschema import (
    SupportTableWithDBSchema,
)
from onetl.connection.db_connection.jdbc_connection import JDBCConnection

# do not import PySpark here, as we allow user to use `Postgres.package` for creating Spark session


class Postgres(JDBCConnection):
    """PostgreSQL JDBC connection. |support_hooks|

    Based on Maven package ``org.postgresql:postgresql:42.6.0``
    (`official Postgres JDBC driver <https://jdbc.postgresql.org/>`_).

    .. dropdown:: Version compatibility

        * PostgreSQL server versions: 8.2 or higher
        * Spark versions: 2.3.x - 3.4.x
        * Java versions: 8 - 17

        See `official documentation <https://jdbc.postgresql.org/download/>`_.

    .. warning::

        To use Postgres connector you should have PySpark installed (or injected to ``sys.path``)
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
        Host of Postgres database. For example: ``test.postgres.domain.com`` or ``193.168.1.11``

    port : int, default: ``5432``
        Port of Postgres database

    user : str
        User, which have proper access to the database. For example: ``some_user``

    password : str
        Password for database connection

    database : str
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"ssl": "false"}``

        See `Postgres JDBC driver properties documentation <https://github.com/pgjdbc/pgjdbc#connection-properties>`_
        for more details

    Examples
    --------

    Postgres connection initialization

    .. code:: python

        from onetl.connection import Postgres
        from pyspark.sql import SparkSession

        extra = {"ssl": "false"}

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
            .getOrCreate()
        )

        postgres = Postgres(
            host="database.host.or.ip",
            user="user",
            password="*****",
            database="target_database",
            extra=extra,
            spark=spark,
        )

    """

    database: str
    port: int = 5432

    driver: ClassVar[str] = "org.postgresql.Driver"
    package: ClassVar[str] = "org.postgresql:postgresql:42.6.0"

    class Dialect(  # noqa: WPS215
        SupportTableWithDBSchema,
        SupportColumnsList,
        SupportDfSchemaNone,
        SupportWhereStr,
        SupportHWMExpressionStr,
        SupportHintNone,
        DBConnection.Dialect,
    ):
        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            result = value.isoformat()
            return f"'{result}'::timestamp"

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            result = value.isoformat()
            return f"'{result}'::date"

    class ReadOptions(JDBCConnection.ReadOptions):
        # https://stackoverflow.com/a/9812029
        @classmethod
        def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"('x'||right(md5('{partition_column}'), 16))::bit(32)::bigint % {num_partitions}"

        @classmethod
        def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"{partition_column} % {num_partitions}"

    ReadOptions.__doc__ = JDBCConnection.ReadOptions.__doc__

    @property
    def jdbc_url(self) -> str:
        extra = self.extra.dict(by_alias=True)
        extra["ApplicationName"] = extra.get("ApplicationName", self.spark.sparkContext.appName)

        parameters = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

    @property
    def instance_url(self) -> str:
        return f"{super().instance_url}/{self.database}"

    def _options_to_connection_properties(self, options: JDBCConnection.JDBCOptions):  # noqa: WPS437
        # See https://github.com/pgjdbc/pgjdbc/pull/1252
        # Since 42.2.9 Postgres JDBC Driver added new option readOnlyMode=transaction
        # Which is not a desired behavior, because `.fetch()` method should always be read-only

        if not getattr(options, "readOnlyMode", None):
            options = options.copy(update={"readOnlyMode": "always"})

        return super()._options_to_connection_properties(options)
