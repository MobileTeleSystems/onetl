# SPDX-FileCopyrightText: 2021-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import warnings
from typing import ClassVar

from pydantic import ConfigDict

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
from onetl.impl import GenericOptions, Host

# do not import PySpark here, as we allow user to use `MSSQL.get_packages()` for creating Spark session


class MSSQLExtra(GenericOptions):
    """
    Extra options for MSSQL connection.

    You can pass here any property supported by
    [MSSQL JDBC driver]((https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties#properties),
    even if it is not mentioned in this documentation.
    """

    model_config = ConfigDict(extra="allow", prohibited_options=frozenset(("databaseName",)))  # type: ignore[typeddict-unknown-key]


@support_hooks
class MSSQL(JDBCConnection):
    """MSSQL JDBC connection. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on Maven package [com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre8](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc/13.4.0.jre8)
    ([official MSSQL JDBC driver](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server)).

    !!! info "See also"

        Before using this connector please take into account [MSSQL prerequisites][DBR-onetl-connection-db-connection-mssql-prerequisites]

    Parameters
    ----------
    host
        Host of MSSQL database. For example: `test.mssql.domain.com` or `192.168.1.14`

    port
        Port of MSSQL database

        !!! info "Changed in 0.11.1"
            Default value was changed from `1433` to `None`,
            to allow automatic port discovery with `instanceName`.

    user
        User for database connection

    password
        Password for database connection

    database
        Database in RDBMS, NOT schema

        See [this page](https://www.educba.com/postgresql-database-vs-schema/) for more details

    spark
        Spark session

    extra
        Extra parameters passed directly to JDBC driver.
        For example: `{"connectRetryCount": 3, "connectRetryInterval": 10}`.

    Examples
    --------

    === "Create MSSQL connection with plain auth"

        ```python
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
        ```

    === "Create MSSQL connection with domain auth"

        ```python
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
        ```

    === "Create MSSQL connection with instance name"

        ```python
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
        ```

    === "Create MSSQL read-only connection"

        ```python
        # Create Spark session with MSSQL driver loaded
        ...

        # Create connection
        mssql = MSSQL(
            host="database.host.or.ip",
            port=1433,
            user="user",
            password="*****",
            extra={
                # driver will open read-only connection, to avoid writing to the database
                "applicationIntent": "ReadOnly",
                # add this to avoid SSL certificate issues
                "trustServerCertificate": "true",
            },
            spark=spark,
        ).check()
        ```
    """

    database: str
    host: Host
    port: int | None = None
    extra: MSSQLExtra = MSSQLExtra()

    ReadOptions: ClassVar = MSSQLReadOptions
    WriteOptions: ClassVar = MSSQLWriteOptions
    SQLOptions: ClassVar = MSSQLSQLOptions
    FetchOptions: ClassVar = MSSQLFetchOptions  # type: ignore[misc]
    ExecuteOptions: ClassVar = MSSQLExecuteOptions  # type: ignore[misc]

    Extra: ClassVar = MSSQLExtra
    Dialect: ClassVar = MSSQLDialect

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
        Get package names to be downloaded by Spark. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        Allows specifying custom JDBC driver versions for MSSQL.

        !!! success "Added in 0.9.0"

        Parameters
        ----------
        java_version
            Java major version, defaults to `8`. Must be `8` or `11`.
        package_version
            Specifies the version of the MSSQL JDBC driver to use. Defaults to `13.4.0.`.

        Examples
        --------
        ```python
        from onetl.connection import MSSQL

        MSSQL.get_packages()

        # specify Java and package versions
        MSSQL.get_packages(java_version="8", package_version="13.4.0.jre11")
        ```
        """
        default_java_version = "8"
        default_package_version = "13.4.0"

        java_ver = Version(java_version or default_java_version)
        if java_ver.major < 8:  # noqa: PLR2004
            msg = f"Java version must be at least 8, got {java_ver}"
            raise ValueError(msg)

        jre_ver = "8" if java_ver.major < 11 else "11"  # noqa: PLR2004
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
        return "com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre8"

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
        result.update(self.extra.model_dump(by_alias=True))
        result["databaseName"] = self.database
        # https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-ver16#properties
        result["applicationName"] = result.get("applicationName", get_client_info(self.spark, limit=128))
        return result

    @property
    def instance_url(self) -> str:
        extra_dict = self.extra.model_dump(by_alias=True)
        instance_name = extra_dict.get("instanceName")
        if instance_name:
            return rf"{self.__class__.__name__.lower()}://{self.host}\{instance_name}/{self.database}"

        # for backward compatibility keep port number in legacy HWM instance url
        port = self.port or 1433
        return f"{self.__class__.__name__.lower()}://{self.host}:{port}/{self.database}"

    def __str__(self):
        extra_dict = self.extra.model_dump(by_alias=True)
        instance_name = extra_dict.get("instanceName")
        if instance_name:
            return rf"{self.__class__.__name__}[{self.host}\{instance_name}/{self.database}]"

        port = self.port or 1433
        return f"{self.__class__.__name__}[{self.host}:{port}/{self.database}]"

    def _get_jdbc_connection(
        self,
        options: JDBCFetchOptions | JDBCExecuteOptions,
        *,
        read_only: bool,
    ):
        if read_only:
            # connection.setReadOnly() is no-op in MSSQL:
            # https://learn.microsoft.com/en-us/sql/connect/jdbc/reference/setreadonly-method-sqlserverconnection?view=sql-server-ver16
            # Instead, we should change connection type via option:
            # https://github.com/microsoft/mssql-jdbc/issues/484
            options = options.model_copy(update={"ApplicationIntent": "ReadOnly"})

        return super()._get_jdbc_connection(options, read_only=read_only)
