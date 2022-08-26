from __future__ import annotations

from datetime import date, datetime
from typing import ClassVar, Optional

from onetl.connection.db_connection.jdbc_connection import JDBCConnection


class Teradata(JDBCConnection):
    """Class for Teradata JDBC connection.

    Based on package ``com.teradata.jdbc:terajdbc4:17.20.00.08``
    (`official Teradata JDBC driver <https://downloads.teradata.com/download/connectivity/jdbc-driver>`_)

    Parameters
    ----------
    host : str
        Host of Teradata database. For example: ``test.teradata.domain.com`` or ``193.168.1.12``

    port : int, default: ``1025``
        Port of Teradata database

    user : str
        User, which have access to the database and table. For example: ``some_user``

    password : str
        Password for database connection

    database : str
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters which should be appended to a connection string.

        For example: ``{"TMODE": "TERA", "MAYBENULL": "ON", "CHARSET": "UTF8", "LOGMECH":"LDAP"}``

        See `Teradata JDBC driver documentation
        <https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ>`_
        for more details

        .. note::

            By default, ``STRICT_NAMES=OFF`` and ``FLATTEN=ON`` options are added to extra.

            It is possible to pass different values for these options,
            e.g. ``extra={"FLATTEN": "OFF"}``

    Examples
    --------

    Teradata connection with LDAP auth:

    .. code::

        from onetl.connection import Teradata
        from mtspark import get_spark

        extra = {
            "TMODE": "TERA",  # "TERA" or "ANSI"
            "MAYBENULL": "ON",
            "CHARSET": "UTF8",
            "LOGMECH":"LDAP",
            "LOG": "TIMING",  # increase log level
        }

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Teradata.package],
        })

        teradata = Teradata(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    class Extra(JDBCConnection.Extra):
        STRICT_NAMES: str = "OFF"
        FLATTEN: str = "ON"

        class Config:
            prohibited_options = frozenset(("DATABASE", "DBS_PORT"))

    port: int = 1025
    database: Optional[str] = None
    extra: Extra = Extra()

    driver: ClassVar[str] = "com.teradata.jdbc.TeraDriver"
    package: ClassVar[str] = "com.teradata.jdbc:terajdbc4:17.20.00.08"

    _check_query: ClassVar[str] = "SELECT 1 AS check_result"

    @property
    def jdbc_url(self) -> str:
        prop = self.extra.dict(by_alias=True)

        if self.database:
            prop["DATABASE"] = self.database

        prop["DBS_PORT"] = self.port

        conn = ",".join(f"{k}={v}" for k, v in sorted(prop.items()))
        return f"jdbc:teradata://{self.host}/{conn}"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS TIMESTAMP)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.isoformat()
        return f"CAST('{result}' AS DATE)"
