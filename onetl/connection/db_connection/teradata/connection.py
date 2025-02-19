# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import ClassVar, Optional

from etl_entities.instance import Host

from onetl._util.classproperty import classproperty
from onetl._util.spark import stringify
from onetl._util.version import Version
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.teradata.dialect import TeradataDialect
from onetl.connection.db_connection.teradata.options import (
    TeradataExecuteOptions,
    TeradataFetchOptions,
    TeradataReadOptions,
    TeradataSQLOptions,
    TeradataWriteOptions,
)
from onetl.hooks import slot
from onetl.impl import GenericOptions

# do not import PySpark here, as we allow user to use `Teradata.get_packages()` for creating Spark session


class TeradataExtra(GenericOptions):
    CHARSET: str = "UTF8"
    COLUMN_NAME: str = "ON"
    FLATTEN: str = "ON"
    MAYBENULL: str = "ON"
    STRICT_NAMES: str = "OFF"

    class Config:
        extra = "allow"
        prohibited_options = frozenset(("DATABASE", "DBS_PORT"))


class Teradata(JDBCConnection):
    """Teradata JDBC connection. |support_hooks|

    Based on package `com.teradata.jdbc:terajdbc:17.20.00.15 <https://central.sonatype.com/artifact/com.teradata.jdbc/terajdbc/17.20.00.15>`_
    (`official Teradata JDBC driver <https://downloads.teradata.com/download/connectivity/jdbc-driver>`_).

    .. seealso::

        Before using this connector please take into account :ref:`teradata-prerequisites`

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of Teradata database. For example: ``test.teradata.domain.com`` or ``193.168.1.12``

    port : int, default: ``1025``
        Port of Teradata database

    user : str
        User, which have proper access to the database. For example: ``some_user``

    password : str
        Password for database connection

    database : str, optional
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    extra : dict, default: ``None``
        Specifies one or more extra parameters which should be appended to a connection string.

        For example: ``{"TMODE": "TERA", "MAYBENULL": "ON", "CHARSET": "UTF8", "LOGMECH":"LDAP"}``

        See `Teradata JDBC driver documentation
        <https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ>`_
        for more details

        .. note::

            By default, these options are added to extra:

                * ``CHARSET = "UTF8"``
                * ``COLUMN_NAME = "ON"`` - allow reading column title from a table
                * ``FLATTEN = "ON"`` - improves error messages
                * ``MAYBENULL = "ON"``
                * ``STRICT_NAMES = "OFF"`` - ignore Spark options passed to JDBC URL

            It is possible to override default values, for example set ``extra={"FLATTEN": "OFF"}``

    Examples
    --------

    Create Teradata connection with LDAP auth:

    .. code:: python

        from onetl.connection import Teradata
        from pyspark.sql import SparkSession

        # Create Spark session with Teradata driver loaded
        maven_packages = Teradata.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        teradata = Teradata(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra={
                "TMODE": "TERA",  # "TERA" or "ANSI"
                "LOGMECH": "LDAP",
                "LOG": "TIMING",  # increase log level
            },
            spark=spark,
        ).check()

    """

    host: Host
    port: int = 1025
    database: Optional[str] = None
    extra: TeradataExtra = TeradataExtra()

    ReadOptions = TeradataReadOptions
    WriteOptions = TeradataWriteOptions
    SQLOptions = TeradataSQLOptions
    FetchOptions = TeradataFetchOptions
    ExecuteOptions = TeradataExecuteOptions

    Extra = TeradataExtra
    Dialect = TeradataDialect

    DRIVER: ClassVar[str] = "com.teradata.jdbc.TeraDriver"
    _CHECK_QUERY: ClassVar[str] = "SELECT 1 AS check_result"

    @slot
    @classmethod
    def get_packages(
        cls,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. Allows specifying custom JDBC driver versions for Teradata. |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        package_version : str, optional
            Specifies the version of the Teradata JDBC driver to use. Defaults to ``17.20.00.15``.

            .. versionadded:: 0.11.0

        Examples
        --------
        .. code:: python

            from onetl.connection import Teradata

            Teradata.get_packages()

            # specify custom driver version
            Teradata.get_packages(package_version="20.00.00.18")
        """
        default_package_version = "17.20.00.15"
        version = Version(package_version or default_package_version).min_digits(4)

        return [f"com.teradata.jdbc:terajdbc:{version}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Teradata.package` will be removed in 1.0.0, use `Teradata.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.teradata.jdbc:terajdbc:17.20.00.15"

    @property
    def jdbc_url(self) -> str:
        # Teradata JDBC driver documentation specifically mentions that params from
        # java.sql.DriverManager.getConnection(url, params) are used to only retrieve 'user' and 'password' values.
        # Other params should be passed via url
        properties = self.extra.dict(by_alias=True)

        if self.database:
            properties["DATABASE"] = self.database

        properties["DBS_PORT"] = self.port

        connection_params = []
        for key, value in sorted(properties.items()):
            string_value = stringify(value)
            if "," in string_value:
                connection_params.append(f"{key}='{string_value}'")
            else:
                connection_params.append(f"{key}={string_value}")

        return f"jdbc:teradata://{self.host}/{','.join(connection_params)}"

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"
