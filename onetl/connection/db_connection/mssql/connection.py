# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import ClassVar

from onetl._util.classproperty import classproperty
from onetl._util.version import Version
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.mssql.dialect import MSSQLDialect
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

    Based on Maven package ``com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8``
    (`official MSSQL JDBC driver
    <https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server>`_).

    .. dropdown:: Version compatibility

        * SQL Server versions: 2014 - 2022
        * Spark versions: 2.3.x - 3.5.x
        * Java versions: 8 - 20

        See `official documentation <https://learn.microsoft.com/en-us/sql/connect/jdbc/system-requirements-for-the-jdbc-driver>`_
        and `official compatibility matrix <https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server-support-matrix>`_.

    .. warning::

        To use MSSQL connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.5.0  # pass specific PySpark version

        See :ref:`install-spark` installation instruction for more details.

    Parameters
    ----------
    host : str
        Host of MSSQL database. For example: ``test.mssql.domain.com`` or ``192.168.1.14``

    port : int, default: ``1433``
        Port of MSSQL database

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

    MSSQL connection with plain auth:

    .. code:: python

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
            user="user",
            password="*****",
            extra={
                "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
            },
            spark=spark,
        )

    MSSQL connection with domain auth:

    .. code:: python

        # Create Spark session with MSSQL driver loaded
        ...

        # Create connection
        mssql = MSSQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra={
                "Domain": "some.domain.com",  # add here your domain
                "IntegratedSecurity": "true",
                "authenticationScheme": "NTLM",
                "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
            },
            spark=spark,
        )

    MSSQL read-only connection:

    .. code:: python

        # Create Spark session with MSSQL driver loaded
        ...

        # Create connection
        mssql = MSSQL(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra={
                "ApplicationIntent": "ReadOnly",  # driver will open read-only connection, to avoid writing to the database
                "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
            },
            spark=spark,
        )

    """

    database: str
    port: int = 1433
    extra: MSSQLExtra = MSSQLExtra()

    Extra = MSSQLExtra
    Dialect = MSSQLDialect

    DRIVER: ClassVar[str] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    _CHECK_QUERY: ClassVar[str] = "SELECT 1 AS field"

    @slot
    @classmethod
    def get_packages(
        cls,
        java_version: str | Version | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Parameters
        ----------
        java_version : str, default ``8``
            Java major version.

        Examples
        --------

        .. code:: python

            from onetl.connection import MSSQL

            MSSQL.get_packages()
            MSSQL.get_packages(java_version="8")

        """
        if java_version is None:
            java_version = "8"

        java_ver = Version.parse(java_version)
        if java_ver.major < 8:
            raise ValueError(f"Java version must be at least 8, got {java_ver}")

        jre_ver = "8" if java_ver.major < 11 else "11"
        return [f"com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre{jre_ver}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`MSSQL.package` will be removed in 1.0.0, use `MSSQL.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"

    @property
    def jdbc_url(self) -> str:
        prop = self.extra.dict(by_alias=True)
        prop["databaseName"] = self.database
        parameters = ";".join(f"{k}={v}" for k, v in sorted(prop.items()))

        return f"jdbc:sqlserver://{self.host}:{self.port};{parameters}"

    @property
    def instance_url(self) -> str:
        return f"{super().instance_url}/{self.database}"
