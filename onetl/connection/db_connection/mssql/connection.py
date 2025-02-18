# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
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
from onetl.connection.db_connection.mssql.dialect import MSSQLDialect
from onetl.connection.db_connection.mssql.options import (
    MSSQLExecuteOptions,
    MSSQLFetchOptions,
    MSSQLReadOptions,
    MSSQLSQLOptions,
    MSSQLWriteOptions,
)
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

# do not import PySpark here, as we allow user to use `MSSQL.get_packages()` for creating Spark session


class MSSQLExtra(GenericOptions):
    class Config:
        extra = "allow"
        prohibited_options = frozenset(("databaseName",))


@support_hooks
class MSSQL(JDBCConnection):
    """MSSQL JDBC connection. |support_hooks|

    Based on Maven package `com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre8 <https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc/12.8.1.jre8>`_
    (`official MSSQL JDBC driver
    <https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server>`_).

    .. seealso::

        Before using this connector please take into account :ref:`mssql-prerequisites`

    Parameters
    ----------
    host : str
        Host of MSSQL database. For example: ``test.mssql.domain.com`` or ``192.168.1.14``

    port : int, default: ``None``
        Port of MSSQL database

        .. versionchanged:: 0.11.1
            Default value was changed from ``1433`` to ``None``,
            to allow automatic port discovery with ``instanceName``.

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

        For example: ``{"connectRetryCount": 3, "connectRetryInterval": 10}``

        See `MSSQL JDBC driver properties documentation
        <https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties#properties>`_
        for more details

    Examples
    --------

    .. tabs::

        .. code-tab:: py Create MSSQL connection with plain auth

            from onetl.connection import MSSQL
            from pyspark.sql import SparkSession

            # Create Spark session with MSSQL driver loaded
            maven_packages = MSSQL.get_packages()
            spark = (
                SparkSession.builder.appName("spark-app-name")
                .config("spark.jars.packages", ",".join(maven_packages))
                .getOrCreate()
            )

            # Create connection
            mssql = MSSQL(
                host="database.host.or.ip",
                port=1433,
                user="user",
                password="*****",
                extra={
                    "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
                },
                spark=spark,
            )

        .. code-tab:: py Create MSSQL connection with domain auth

            # Create Spark session with MSSQL driver loaded
            ...

            # Create connection
            mssql = MSSQL(
                host="database.host.or.ip",
                port=1433,
                user="user",
                password="*****",
                extra={
                    "domain": "some.domain.com",  # add here your domain
                    "integratedSecurity": "true",
                    "authenticationScheme": "NTLM",
                    "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
                },
                spark=spark,
            )

        .. code-tab:: py Create MSSQL connection with instance name

            # Create Spark session with MSSQL driver loaded
            ...

            # Create connection
            mssql = MSSQL(
                host="database.host.or.ip",
                # !!! no port !!!
                user="user",
                password="*****",
                extra={
                    "instanceName": "myinstance",  # add here your instance name
                    "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
                },
                spark=spark,
            )

        .. code-tab:: py Create MSSQL read-only connection

            # Create Spark session with MSSQL driver loaded
            ...

            # Create connection
            mssql = MSSQL(
                host="database.host.or.ip",
                port=1433,
                user="user",
                password="*****",
                extra={
                    "applicationIntent": "ReadOnly",  # driver will open read-only connection, to avoid writing to the database
                    "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
                },
                spark=spark,
            ).check()
    """

    database: str
    host: Host
    port: Optional[int] = None
    extra: MSSQLExtra = MSSQLExtra()

    ReadOptions = MSSQLReadOptions
    WriteOptions = MSSQLWriteOptions
    SQLOptions = MSSQLSQLOptions
    FetchOptions = MSSQLFetchOptions
    ExecuteOptions = MSSQLExecuteOptions

    Extra = MSSQLExtra
    Dialect = MSSQLDialect

    DRIVER: ClassVar[str] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    _CHECK_QUERY: ClassVar[str] = "SELECT 1 AS field"

    @slot
    @classmethod
    def get_packages(
        cls,
        java_version: str | None = None,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. Allows specifying custom JDBC driver versions for MSSQL.  |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        java_version : str, optional
            Java major version, defaults to ``8``. Must be ``8`` or ``11``.
        package_version : str, optional
            Specifies the version of the MSSQL JDBC driver to use. Defaults to ``12.8.1.``.

        Examples
        --------
        .. code:: python

            from onetl.connection import MSSQL

            MSSQL.get_packages()

            # specify Java and package versions
            MSSQL.get_packages(java_version="8", package_version="12.8.1.jre11")
        """
        default_java_version = "8"
        default_package_version = "12.8.1"

        java_ver = Version(java_version or default_java_version)
        if java_ver.major < 8:
            raise ValueError(f"Java version must be at least 8, got {java_ver}")

        jre_ver = "8" if java_ver.major < 11 else "11"
        full_package_version = Version(package_version or default_package_version).min_digits(3)

        # check if a JRE suffix is already included
        if ".jre" in str(full_package_version):
            jdbc_version = full_package_version
        else:
            jdbc_version = Version(f"{full_package_version}.jre{jre_ver}")

        return [f"com.microsoft.sqlserver:mssql-jdbc:{jdbc_version}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`MSSQL.package` will be removed in 1.0.0, use `MSSQL.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre8"

    @property
    def jdbc_url(self) -> str:
        if self.port:
            # automatic port discovery, like used with custom instanceName
            # https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16#named-and-multiple-sql-server-instances
            return f"jdbc:sqlserver://{self.host}:{self.port}"
        return f"jdbc:sqlserver://{self.host}"

    @property
    def jdbc_params(self) -> dict:
        result = super().jdbc_params
        result.update(self.extra.dict(by_alias=True))
        result["databaseName"] = self.database
        # https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver16#properties
        result["applicationName"] = result.get("applicationName", get_client_info(self.spark, limit=128))
        return result

    @property
    def instance_url(self) -> str:
        extra_dict = self.extra.dict(by_alias=True)
        instance_name = extra_dict.get("instanceName")
        if instance_name:
            return rf"{self.__class__.__name__.lower()}://{self.host}\{instance_name}/{self.database}"

        # for backward compatibility keep port number in legacy HWM instance url
        port = self.port or 1433
        return f"{self.__class__.__name__.lower()}://{self.host}:{port}/{self.database}"

    def __str__(self):
        extra_dict = self.extra.dict(by_alias=True)
        instance_name = extra_dict.get("instanceName")
        if instance_name:
            return rf"{self.__class__.__name__}[{self.host}\{instance_name}/{self.database}]"

        port = self.port or 1433
        return f"{self.__class__.__name__}[{self.host}:{port}/{self.database}]"

    def _get_jdbc_connection(self, options: JDBCFetchOptions | JDBCExecuteOptions, read_only: bool):
        if read_only:
            # connection.setReadOnly() is no-op in MSSQL:
            # https://learn.microsoft.com/en-us/sql/connect/jdbc/reference/setreadonly-method-sqlserverconnection?view=sql-server-ver16
            # Instead, we should change connection type via option:
            # https://github.com/microsoft/mssql-jdbc/issues/484
            options = options.copy(update={"ApplicationIntent": "ReadOnly"})

        return super()._get_jdbc_connection(options, read_only)
