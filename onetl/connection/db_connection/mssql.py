from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


@dataclass(frozen=True)
class MSSQL(JDBCConnection):
    """Class for MSSQL jdbc connection.

    .. note::

        Supported SQL Server versions: >= 2012

    Parameters
    ----------
    host : str
        Host of MSSQL database. For example: ``0001MSSQLDB02.dmz.msk.mts.ru``

    port : int, default: ``1433``
        Port of MSSQL database

    user : str
        User, which have access to the database and table. For example: ``big_data_tech_user``

    password : str
        Password for database connection

    database : str
        Database in rdbms. To provide schema, use DBReader class

        See https://www.educba.com/postgresql-database-vs-schema/ for more details

    spark : :obj:`pyspark.sql.SparkSession`, default: ``None``
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"connectRetryCount": 3, "connectRetryInterval": 10}``

    Examples
    --------

    MSSQL jdbc connection initialization

    .. code::

        from onetl.connection import MSSQL
        from mtspark import get_spark

        extra = {
            "connectRetryCount": 3,
            "connectRetryInterval": 10,
            "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
        }

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [MSSQL.package],
        })

        mssql = MSSQL(
            host="0001MSSQLDB02.dmz.msk.mts.ru",
            user="big_data_tech_user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    driver: ClassVar[str] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    package: ClassVar[str] = "com.microsoft.sqlserver:mssql-jdbc:10.2.1.jre8"
    port: int = 1433

    _check_query: ClassVar[str] = "SELECT 1 AS field"

    def __post_init__(self):
        if not self.database:
            raise ValueError(
                f"You should provide database name for {self.__class__.__name__} connection. "
                "Use database parameter: database = 'database_name'",
            )

    @property
    def jdbc_url(self) -> str:
        parameters = "".join(f";{k}={v}" for k, v in self.extra.items())

        return f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database}{parameters}"

    @property
    def instance_url(self) -> str:
        return f"{super().instance_url}/{self.database}"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS datetime2)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS date)"
