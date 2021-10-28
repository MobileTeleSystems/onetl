from dataclasses import dataclass, field

from onetl.connection.db_connection.db_connection import DBConnection


@dataclass(frozen=True)
class MSSQL(DBConnection):
    """Class for MSSQL jdbc connection.

    Parameters
    ----------
    host : str
        Host of MSSQL database. For example: ``0001MSSQLDB02.dmz.msk.mts.ru``
    port : int, optional, default: ``1433``
        Port of MSSQL database
    user : str, default: ``None``
        User, which have access to the database and table. For example: ``big_data_tech_user``
    password : str, default: ``None``
        Password for database connection
    database : str, default: ``default``
        Database in rdbms. To provide schema, use DBReader class

        Difference like https://www.educba.com/postgresql-database-vs-schema/
    extra : Dict, optional, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"connectRetryCount": 3, "connectRetryInterval": 10}``.
    spark : pyspark.sql.SparkSession, default: ``None``
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    MSSQL jdbc connection initialization

    .. code::

        from onetl.connection.db_connection import MSSQL
        from mtspark import get_spark

        extra = {"connectRetryCount": 3, "connectRetryInterval": 10}

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": MSSQL.package,
        })

        mssql = MSSQL(
            host="0001MSSQLDB02.dmz.msk.mts.ru",
            user="big_data_tech_user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    driver: str = field(init=False, default="com.microsoft.sqlserver.jdbc.SQLServerDriver")
    package: str = field(init=False, default="com.microsoft.sqlserver:mssql-jdbc:7.2.0.jre8")
    port: int = 1433

    @property
    def url(self) -> str:
        params = "".join(f";{k}={v}" for k, v in self.extra.items())
        return f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database}{params}"

    def _get_timestamp_value_sql(self, value):
        return f"CAST({value.lit()} AS datetime2)"
