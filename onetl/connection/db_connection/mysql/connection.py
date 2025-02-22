# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from contextlib import closing
from typing import ClassVar, Optional

from etl_entities.instance import Host

from onetl._util.classproperty import classproperty
from onetl._util.spark import get_client_info
from onetl._util.version import Version
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCExecuteOptions,
    JDBCFetchOptions,
)
from onetl.connection.db_connection.mysql.dialect import MySQLDialect
from onetl.connection.db_connection.mysql.options import (
    MySQLExecuteOptions,
    MySQLFetchOptions,
    MySQLReadOptions,
    MySQLSQLOptions,
    MySQLWriteOptions,
)
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

    Based on Maven package `com.mysql:mysql-connector-j:9.2.0 <https://mvnrepository.com/artifact/com.mysql/mysql-connector-j/9.2.0>`_
    (`official MySQL JDBC driver <https://dev.mysql.com/doc/connector-j/en/>`_).

    .. seealso::

        Before using this connector please take into account :ref:`mysql-prerequisites`

    .. versionadded:: 0.1.0

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

    Create and check MySQL connection:

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
        ).check()

    """

    host: Host
    port: int = 3306
    database: Optional[str] = None
    extra: MySQLExtra = MySQLExtra()

    ReadOptions = MySQLReadOptions
    WriteOptions = MySQLWriteOptions
    SQLOptions = MySQLSQLOptions
    FetchOptions = MySQLFetchOptions
    ExecuteOptions = MySQLExecuteOptions

    Extra = MySQLExtra
    Dialect = MySQLDialect

    DRIVER: ClassVar[str] = "com.mysql.cj.jdbc.Driver"

    @slot
    @classmethod
    def get_packages(cls, package_version: str | None = None) -> list[str]:
        """
        Get package names to be downloaded by Spark. Allows specifying a custom JDBC driver version for MySQL. |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        package_version : str, optional
            Specifies the version of the MySQL JDBC driver to use. Defaults to ``9.2.0``.

            .. versionadded:: 0.11.0

        Examples
        --------
        .. code:: python

            from onetl.connection import MySQL

            MySQL.get_packages()

            # specify a custom package version
            MySQL.get_packages(package_version="8.2.0")
        """
        default_version = "9.2.0"
        version = Version(package_version or default_version).min_digits(3)

        return [f"com.mysql:mysql-connector-j:{version}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`MySQL.package` will be removed in 1.0.0, use `MySQL.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.mysql:mysql-connector-j:9.2.0"

    @property
    def jdbc_url(self) -> str:
        if self.database:
            return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"

        return f"jdbc:mysql://{self.host}:{self.port}"

    @property
    def jdbc_params(self) -> dict:
        result = super().jdbc_params
        result.update(self.extra.dict(by_alias=True))
        # https://dev.mysql.com/doc/connector-j/en/connector-j-connp-props-connection.html
        # https://stackoverflow.com/questions/31722323/mysql-connection-with-advanced-attributes-such-as-program-name
        client_info = f"program_name:{get_client_info(self.spark, unsupported=':,')}"
        connection_attributes = result.get("connectionAttributes")
        if connection_attributes and "program_name:" not in connection_attributes:
            result["connectionAttributes"] = f"{connection_attributes},{client_info}"
        elif not connection_attributes:
            result["connectionAttributes"] = client_info
        return result

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"

    def _get_jdbc_connection(self, options: JDBCFetchOptions | JDBCExecuteOptions, read_only: bool):
        connection = super()._get_jdbc_connection(options, read_only)

        # connection.setReadOnly() is no-op in MySQL JDBC driver. Session type can be changed by statement:
        # https://stackoverflow.com/questions/10240890/sql-open-connection-in-read-only-mode#comment123789248_48959180
        # https://dev.mysql.com/doc/refman/8.4/en/set-transaction.html
        transaction = "READ ONLY" if read_only else "READ WRITE"
        statement = connection.prepareStatement(f"SET SESSION TRANSACTION {transaction};")
        with closing(statement):
            statement.execute()

        return connection
