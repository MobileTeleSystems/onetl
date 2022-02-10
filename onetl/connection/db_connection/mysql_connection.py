from dataclasses import dataclass
from datetime import datetime, date
from typing import ClassVar

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


@dataclass(frozen=True)
class MySQL(JDBCConnection):
    """Class for MySQL jdbc connection.

    Parameters
    ----------
    host : str
        Host of MySQL database. For example: ``mysql0012.dmz.msk.mts.ru``

    port : int, default: ``3306``
        Port of MySQL database

    user : str
        User, which have access to the database and table. For example: ``big_data_tech_user``

    password : str
        Password for database connection

    database : str
        Database in rdbms. To provide schema, use DBReader class

    spark : pyspark.sql.SparkSession
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"some_extra": "..."}``

    Examples
    --------

    MySQL jdbc connection initialization

    .. code::

        from onetl.connection.db_connection import MySQL
        from mtspark import get_spark

        extra = {"some_extra": "..."}

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [MySQL.package],
        })

        mysql = MySQL(
            host="mysql0012.dmz.msk.mts.ru",
            user="big_data_tech_user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    driver: ClassVar[str] = "com.mysql.jdbc.Driver"
    package: ClassVar[str] = "mysql:mysql-connector-java:8.0.26"
    port: int = 3306

    @property
    def jdbc_url(self):
        prop = self.extra.copy()
        prop["useUnicode"] = "yes"
        prop["characterEncoding"] = "UTF-8"
        parameters = "&".join(f"{k}={v}" for k, v in prop.items())

        if self.database:
            return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?{parameters}"

        return f"jdbc:mysql://{self.host}:{self.port}?{parameters}"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d %H:%i:%s.%f')"  # noqa: WPS323

    def _get_date_value_sql(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"STR_TO_DATE('{result}', '%Y-%m-%d')"  # noqa: WPS323
