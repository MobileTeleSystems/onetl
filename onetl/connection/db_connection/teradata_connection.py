from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


@dataclass(frozen=True)
class Teradata(JDBCConnection):
    """Class for Teradata jdbc connection.

    Parameters
    ----------
    host : str
        Host of teradata database. For example: ``0411td-rnd.pv.mts.ru``
    port : int, optional, default: ``1025``
        Port of teradata database
    user : str, default: ``None``
        User, which have access to the database and table. For example: ``TECH_ETL``
    password : str, default: ``None``
        Password for database connection
    database : str, default: ``default``
        Database in rdbms. To provide schema, use DBReader class
    extra : Dict, optional, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"LOGMECH": "TERA", "MAYBENULL": "ON", "CHARSET": "UTF8", "LOGMECH":"LDAP"}``.
    spark : pyspark.sql.SparkSession, default: ``None``
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Teradata jdbc connection initialization

    .. code::

        from onetl.connection.db_connection import Teradata
        from mtspark import get_spark

        extra = {
            "LOGMECH": "TERA",
            "MAYBENULL": "ON",
            "CHARSET": "UTF8",
            "LOGMECH":"LDAP",
        }

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Teradata.package,
        })

        teradata = Teradata(
            host="0411td-rnd.pv.mts.ru",
            user="BD_TECH_ETL",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    driver: ClassVar[str] = "com.teradata.jdbc.TeraDriver"
    # TODO:(@mivasil6) think about workaround for case with several jar packages
    package: ClassVar[str] = "com.teradata.jdbc:terajdbc4:16.20.00.10"
    port: int = 1025

    @property
    def url(self) -> str:
        prop = self.extra.copy()
        prop["DATABASE"] = self.database
        if self.port:
            prop["DBS_PORT"] = self.port

        schema = ",".join(f"{k}={v}" for k, v in prop.items())

        return f"jdbc:teradata://{self.host}/{schema}"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS TIMESTAMP)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS DATE)"
