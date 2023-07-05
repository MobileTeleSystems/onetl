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
from typing import ClassVar, Optional

from onetl.connection.db_connection.jdbc_connection import JDBCConnection

# do not import PySpark here, as we allow user to use `MySQL.package` for creating Spark session


class MySQL(JDBCConnection):
    """MySQL JDBC connection. |support_hooks|

    Based on Maven package ``com.mysql:mysql-connector-j:8.0.33``
    (`official MySQL JDBC driver <https://dev.mysql.com/downloads/connector/j/8.0.html>`_).

    .. dropdown:: Version compatibility

        * MySQL server versions: 5.7, 8.0
        * Spark versions: 2.3.x - 3.4.x
        * Java versions: 8 - 17

        See `official documentation <https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-versions.html>`_.

    .. warning::

        To use MySQL connector you should have PySpark installed (or injected to ``sys.path``)
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
        Host of MySQL database. For example: ``mysql0012.domain.com`` or ``192.168.1.11``

    port : int, default: ``3306``
        Port of MySQL database

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

        For example: ``{"useSSL": "false"}``

        See `MySQL JDBC driver properties documentation
        <https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html>`_
        for more details

    Examples
    --------

    MySQL connection initialization

    .. code:: python

        from onetl.connection import MySQL
        from pyspark.sql import SparkSession

        extra = {"useSSL": "false"}

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", MySQL.package)
            .getOrCreate()
        )

        mysql = MySQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    class Extra(JDBCConnection.Extra):
        useUnicode: str = "yes"  # noqa: N815
        characterEncoding: str = "UTF-8"  # noqa: N815

    port: int = 3306
    database: Optional[str] = None
    extra: Extra = Extra()

    driver: ClassVar[str] = "com.mysql.cj.jdbc.Driver"
    package: ClassVar[str] = "com.mysql:mysql-connector-j:8.0.33"

    @property
    def jdbc_url(self):
        prop = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(prop.items()))

        if self.database:
            return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?{parameters}"

        return f"jdbc:mysql://{self.host}:{self.port}?{parameters}"

    class Dialect(JDBCConnection.Dialect):
        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            result = value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return f"STR_TO_DATE('{result}', '%Y-%m-%d %H:%i:%s.%f')"

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            result = value.strftime("%Y-%m-%d")
            return f"STR_TO_DATE('{result}', '%Y-%m-%d')"

    class ReadOptions(JDBCConnection.ReadOptions):
        @classmethod
        def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"MOD(CONV(CONV(RIGHT(MD5({partition_column}), 16),16, 2), 2, 10), {num_partitions})"

        @classmethod
        def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"MOD({partition_column}, {num_partitions})"

    ReadOptions.__doc__ = JDBCConnection.ReadOptions.__doc__
