from dataclasses import dataclass, field
from datetime import date, datetime

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


@dataclass(frozen=True)
class Clickhouse(JDBCConnection):
    """Class for Clickhouse jdbc connection.

    Parameters
    ----------
    host : str
        Host of clickhouse database. For example: ``clickhouse-sbl-dev.msk.bd-cloud.mts.ru``
    port : int, optional, default: ``9000``
        Port of clickhouse database
    user : str, default: ``None``
        User, which have access to the database and table. For example: ``TECH_ETL``
    password : str, default: ``None``
        Password for database connection
    database : str, default: ``default``
        Database in rdbms. To provide schema, use DBReader class
    extra : Dict, optional, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"ssl": True, "sslmode": "none"}``.
    spark : pyspark.sql.SparkSession, default: ``None``
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Clickhouse jdbc connection initialization

    .. code::

        from onetl.connection.db_connection import Clickhouse
        from mtspark import get_spark

        extra = {"ssl": True, "sslmode": "none"}

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Clickhouse.package,
        })

        clickhouse = Clickhouse(
            host="clickhouse-sbl-dev.msk.bd-cloud.mts.ru",
            user="TECH_ETL",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    driver: str = field(init=False, default="ru.yandex.clickhouse.ClickHouseDriver")
    package: str = field(init=False, default="ru.yandex.clickhouse:clickhouse-jdbc:0.3.0")
    port: int = 8123

    @property
    def url(self) -> str:
        params = "&".join(f"{k}={v}" for k, v in self.extra.items())
        return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?{params}".rstrip("?")

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST('{result}', AS DateTime)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"CAST('{result}', AS Date)"
