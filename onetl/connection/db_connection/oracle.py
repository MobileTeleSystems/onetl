from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, ClassVar

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


@dataclass(frozen=True)
class Oracle(JDBCConnection):
    """Class for Oracle jdbc connection.

    Parameters
    ----------
    host : str
        Host of oracle database. For example: ``bill.ug.mts.ru``

    port : int, default: ``1521``
        Port of oracle database

    user : str
        User, which have access to the database and table. For example: ``BD_TECH_ETL``

    password : str
        Password for database connection

    sid : str, default: ``None``
        Sid of oracle database. For example: ``XE``

        .. warning ::

            Be careful, to correct work you must provide ``sid`` or ``service_name``

    service_name : str, default: ``None``
        Specifies one or more names by which clients can connect to the instance.

        For example: ``DWHLDTS``.

        .. warning ::

            Be careful, for correct work you must provide ``sid`` or ``service_name``

    spark : pyspark.sql.SparkSession
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization

    Examples
    --------

    Oracle jdbc connection initialization

    .. code::

        from onetl.connection import Oracle
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Oracle.package],
        })

        oracle = Oracle(
            host="bill.ug.mts.ru",
            user="BD_TECH_ETL",
            password="*****",
            sid='XE',
            spark=spark,
        )

    """

    driver: ClassVar[str] = "oracle.jdbc.driver.OracleDriver"
    package: ClassVar[str] = "com.oracle:ojdbc7:12.1.0.2"
    port: int = 1521
    sid: str = ""
    service_name: str = ""

    _check_query: ClassVar[str] = "SELECT 1 FROM dual"

    def __post_init__(self):
        if self.sid and self.service_name:
            raise ValueError(
                "Parameters sid and service_name are specified at the same time, only one must be specified",
            )

        if not self.sid and not self.service_name:
            raise ValueError("Connection to Oracle does not have sid or service_name")

    @property
    def jdbc_url(self) -> str:
        params_str = "&".join(f"{k}={v}" for k, v in self.extra.items())

        if params_str:
            params_str = f"?{params_str}"

        if self.sid:
            return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.sid}{params_str}"

        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}{params_str}"

    @property
    def instance_url(self) -> str:
        if self.sid:
            return f"{super().instance_url}/{self.sid}"

        return f"{super().instance_url}/{self.service_name}"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"TO_DATE('{result}', 'YYYY-MM-DD HH24:MI:SS')"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"TO_DATE('{result}', 'YYYY-MM-DD')"

    def _get_max_value_sql(self, value: Any, alias: str) -> str:
        # `column AS alias` is not supported by Oracle
        # so using `column alias` instead
        result = self._get_value_sql(value)

        return f"MAX({result}) {alias}"

    def _get_min_value_sql(self, value: Any, alias: str) -> str:
        result = self._get_value_sql(value)

        return f"MIN({result}) {alias}"
