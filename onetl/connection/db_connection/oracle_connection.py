from dataclasses import dataclass, field
from datetime import datetime, date

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


@dataclass(frozen=True)
class Oracle(JDBCConnection):
    """Class for Oracle jdbc connection.

    Parameters
    ----------
    host : str
        Host of oracle database. For example: ``bill.ug.mts.ru``
    port : int, optional, default: ``1521``
        Port of oracle database
    user : str, default: ``None``
        User, which have access to the database and table. For example: ``BD_TECH_ETL``
    password : str, default: ``None``
        Password for database connection
    sid : str, optional, default: ``None``
        Sid of oracle database. For example: ``XE``

        .. warning ::

            Be careful, to correct work you must provide ``sid`` or ``service_name``
    service_name : str, optional, default: ``None``
        Specifies one or more names by which clients can connect to the instance.

        For example: ``DWHLDTS``.

        .. warning ::

            Be careful, to correct work you must provide ``sid`` or ``service_name``

    spark : pyspark.sql.SparkSession, default: ``None``
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Oracle jdbc connection initialization

    .. code::

        from onetl.connection.db_connection import Oracle
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Oracle.package,
        })

        oracle = Oracle(
            host="bill.ug.mts.ru",
            user="BD_TECH_ETL",
            password="*****",
            sid='XE',
            spark=spark,
        )

    """

    driver: str = field(init=False, default="oracle.jdbc.driver.OracleDriver")
    package: str = field(init=False, default="com.oracle:ojdbc7:12.1.0.2")
    port: int = 1521
    sid: str = ""
    service_name: str = ""

    @property
    def url(self) -> str:
        if self.sid:
            url = f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.sid}"
        elif self.service_name:
            url = f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}"
        else:
            raise ValueError("Connection to Oracle does not have sid or service_name")
        return url

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"TO_DATE('{result}', 'YYYY-MM-DD HH24:MI:SS')"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"TO_DATE('{result}', 'YYYY-MM-DD')"
