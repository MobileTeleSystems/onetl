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

from onetl.connection.db_connection.jdbc_connection import JDBCConnection

# do not import PySpark here, as we allow user to use `MSSQL.package` for creating Spark session


class MSSQL(JDBCConnection):
    """MSSQL JDBC connection. |support_hooks|

    Based on Maven package ``com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8``
    (`official MSSQL JDBC driver
    <https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server>`_).

    .. dropdown:: Version compatibility

        * SQL Server versions: 2014 - 2022
        * Spark versions: 2.3.x - 3.4.x
        * Java versions: 8 - 17

        See `official documentation <https://learn.microsoft.com/en-us/sql/connect/jdbc/system-requirements-for-the-jdbc-driver>`_
        and `official compatibility matrix <https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server-support-matrix>`_.

    .. warning::

        To use MSSQL connector you should have PySpark installed (or injected to ``sys.path``)
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
        Host of MSSQL database. For example: ``test.mssql.domain.com`` or ``192.168.1.14``

    port : int, default: ``1433``
        Port of MSSQL database

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

        For example: ``{"connectRetryCount": 3, "connectRetryInterval": 10}``

        See `MSSQL JDBC driver properties documentation
        <https://docs.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver16#properties>`_
        for more details

    Examples
    --------

    MSSQL connection with plain auth:

    .. code:: python

        from onetl.connection import MSSQL
        from pyspark.sql import SparkSession

        extra = {
            "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
        }

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", MSSQL.package)
            .getOrCreate()
        )

        mssql = MSSQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    MSSQL connection with domain auth:

    .. code:: python

        from onetl.connection import MSSQL
        from pyspark.sql import SparkSession

        extra = {
            "Domain": "some.domain.com",  # add here your domain
            "IntegratedSecurity": "true",
            "authenticationScheme": "NTLM",
            "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
        }

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", MSSQL.package)
            .getOrCreate()
        )

        mssql = MSSQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    MSSQL read-only connection:

    .. code:: python

        from onetl.connection import MSSQL
        from pyspark.sql import SparkSession

        extra = {
            "ApplicationIntent": "ReadOnly",  # driver will open read-only connection, to avoid writing to the database
            "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
        }

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", MSSQL.package)
            .getOrCreate()
        )

        mssql = MSSQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    class Extra(JDBCConnection.Extra):
        class Config:
            prohibited_options = frozenset(("databaseName",))

    database: str
    port: int = 1433
    extra: Extra = Extra()

    driver: ClassVar[str] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    package: ClassVar[str] = "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"
    _check_query: ClassVar[str] = "SELECT 1 AS field"

    class Dialect(JDBCConnection.Dialect):
        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            result = value.isoformat()
            return f"CAST('{result}' AS datetime2)"

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            result = value.isoformat()
            return f"CAST('{result}' AS date)"

    class ReadOptions(JDBCConnection.ReadOptions):
        # https://docs.microsoft.com/ru-ru/sql/t-sql/functions/hashbytes-transact-sql?view=sql-server-ver16
        @classmethod
        def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"CONVERT(BIGINT, HASHBYTES ( 'SHA' , {partition_column} )) % {num_partitions}"

        @classmethod
        def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"{partition_column} % {num_partitions}"

    ReadOptions.__doc__ = JDBCConnection.ReadOptions.__doc__

    @property
    def jdbc_url(self) -> str:
        prop = self.extra.dict(by_alias=True)
        prop["databaseName"] = self.database
        parameters = ";".join(f"{k}={v}" for k, v in sorted(prop.items()))

        return f"jdbc:sqlserver://{self.host}:{self.port};{parameters}"

    @property
    def instance_url(self) -> str:
        return f"{super().instance_url}/{self.database}"
