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
from onetl.connection.db_connection.postgres.dialect import PostgresDialect
from onetl.connection.db_connection.postgres.options import (
    PostgresExecuteOptions,
    PostgresFetchOptions,
    PostgresReadOptions,
    PostgresSQLOptions,
    PostgresWriteOptions,
)
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions, Host

# do not import PySpark here, as we allow user to use `Postgres.get_packages()` for creating Spark session


class PostgresExtra(GenericOptions):
    """
    Extra options for Postgres connection.

    You can pass here any property supported by
    [Postgres JDBC driver](https://jdbc.postgresql.org/documentation/use/),
    even if it is not mentioned in this documentation.
    """

    stringtype: str = "unspecified"
    """
    Allow automatic conversion from text to target column type during write.
    """

    tcpKeepAlive: str = "true"
    """
    Avoid closing connections from server side while connector is moving data to executors before insert.
    """

    reWriteBatchedInserts: str = "true"
    """
    x2 performance on batch writes
    """

    assumeMinServerVersion: str = "9.4"
    """
    Send connection params to server as early as possible
    """

    model_config = ConfigDict(extra="allow")


@support_hooks
class Postgres(JDBCConnection):
    """PostgreSQL JDBC connection. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on Maven package [org.postgresql:postgresql:42.7.11](https://mvnrepository.com/artifact/org.postgresql/postgresql/42.7.11)
    ([official Postgres JDBC driver](https://jdbc.postgresql.org/)).

    !!! info "See also"

        Before using this connector please take into account [Postgres prerequisites]DBR-onetl-connection-db-connection-postgres-prerequisites]

    !!! success "Added in 0.1.0"

    Parameters
    ----------
    host : str
        Host of Postgres database. For example: `test.postgres.domain.com` or `193.168.1.11`

    port
        Port of Postgres database

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
        For example: `{"ssl": "false"}`.

    Examples
    --------

    Create and check Postgres connection:

    ```python
    from onetl.connection import Postgres
    from pyspark.sql import SparkSession

    # Create Spark session with Postgres driver loaded
    maven_packages = Postgres.get_packages()
    spark = (
        SparkSession.builder.appName("spark-app-name")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )

    # Create connection
    postgres = Postgres(
        host="database.host.or.ip",
        user="user",
        password="*****",
        database="target_database",
        spark=spark,
    )
    ```
    Create read-only connection:

    ```python
    ...

    # Create connection
    postgres = Postgres(
        host="database.host.or.ip",
        user="user",
        password="*****",
        database="target_database",
        extra={"readOnly": True, "readOnlyMode": "always"},  # <--
        spark=spark,
    ).check()
    ```
    """

    host: Host
    database: str
    port: int = 5432
    extra: PostgresExtra = PostgresExtra()

    ReadOptions: ClassVar = PostgresReadOptions
    WriteOptions: ClassVar = PostgresWriteOptions
    SQLOptions: ClassVar = PostgresSQLOptions
    FetchOptions: ClassVar = PostgresFetchOptions  # type: ignore[misc]
    ExecuteOptions: ClassVar = PostgresExecuteOptions  # type: ignore[misc]

    Extra: ClassVar = PostgresExtra
    Dialect: ClassVar = PostgresDialect

    DRIVER: ClassVar[str] = "org.postgresql.Driver"

    @slot
    @classmethod
    def get_packages(cls, package_version: str | None = None) -> list[str]:
        """
        Get package names to be downloaded by Spark.  Allows specifying a custom JDBC driver version. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Parameters
        ----------
        package_version
            Specifies the version of the PostgreSQL JDBC driver to use.  Defaults to `42.7.11`.

        Examples
        --------

        ```python
        from onetl.connection import Postgres

        Postgres.get_packages()

        # custom package version
        Postgres.get_packages(package_version="42.6.0")
        ```
        """
        default_version = "42.7.11"
        version = Version(package_version or default_version).min_digits(3)

        return [f"org.postgresql:postgresql:{version}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Postgres.package` will be removed in 1.0.0, use `Postgres.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.postgresql:postgresql:42.7.11"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_params(self) -> dict[str, str]:
        result = super().jdbc_params
        result.update(self.extra.model_dump(by_alias=True))
        # https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME
        result["ApplicationName"] = result.get("ApplicationName", get_client_info(self.spark, limit=64))
        return result

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.database}]"

    def _get_jdbc_connection(
        self,
        options: JDBCFetchOptions | JDBCExecuteOptions,
        *,
        read_only: bool,
    ):
        if read_only:
            # To properly support pgbouncer, we have to create connection with readOnly option set.
            # See https://github.com/pgjdbc/pgjdbc/issues/848
            options = options.model_copy(update={"readOnly": True})

        connection_properties = self._options_to_connection_properties(options)
        driver_manager = self.spark._jvm.java.sql.DriverManager  # type: ignore[attr-defined, union-attr] # noqa: SLF001
        # avoid calling .setReadOnly(True) here
        return driver_manager.getConnection(self.jdbc_url, connection_properties)
