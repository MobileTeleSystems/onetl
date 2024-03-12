# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import ClassVar, Optional

from onetl._util.classproperty import classproperty
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.mysql.dialect import MySQLDialect
from onetl.hooks import slot, support_hooks
from onetl.impl.generic_options import GenericOptions

# do not import PySpark here, as we allow user to use `MySQL.get_packages()` for creating Spark session


class MySQLExtra(GenericOptions):
    useUnicode: str = "yes"  # noqa: N815
    characterEncoding: str = "UTF-8"  # noqa: N815

    class Config:
        extra = "allow"


@support_hooks
class MySQL(JDBCConnection):
    """MySQL JDBC connection. |support_hooks|

    Based on Maven package ``com.mysql:mysql-connector-j:8.0.33``
    (`official MySQL JDBC driver <https://dev.mysql.com/downloads/connector/j/8.0.html>`_).

    .. warning::

        Before using this connector please take into account :ref:`mysql-prerequisites`

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

        For example: ``{"useSSL": "false", "allowPublicKeyRetrieval": "true"}``

        See `MySQL JDBC driver properties documentation
        <https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html>`_
        for more details

    Examples
    --------

    MySQL connection initialization

    .. code:: python

        from onetl.connection import MySQL
        from pyspark.sql import SparkSession

        # Create Spark session with MySQL driver loaded
        maven_packages = MySQL.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        mysql = MySQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra={"useSSL": "false", "allowPublicKeyRetrieval": "true"},
            spark=spark,
        )

    """

    port: int = 3306
    database: Optional[str] = None
    extra: MySQLExtra = MySQLExtra()

    Extra = MySQLExtra
    Dialect = MySQLDialect

    DRIVER: ClassVar[str] = "com.mysql.cj.jdbc.Driver"

    @slot
    @classmethod
    def get_packages(cls) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Examples
        --------

        .. code:: python

            from onetl.connection import MySQL

            MySQL.get_packages()

        """
        return ["com.mysql:mysql-connector-j:8.0.33"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`MySQL.package` will be removed in 1.0.0, use `MySQL.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.mysql:mysql-connector-j:8.0.33"

    @property
    def jdbc_url(self):
        prop = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(prop.items()))

        if self.database:
            return f"jdbc:mysql://{self.host}:{self.port}/{self.database}?{parameters}"

        return f"jdbc:mysql://{self.host}:{self.port}?{parameters}"
