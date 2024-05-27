# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import ClassVar

from etl_entities.instance import Host

from onetl._util.classproperty import classproperty
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
from onetl.impl import GenericOptions

# do not import PySpark here, as we allow user to use `Postgres.get_packages()` for creating Spark session


class PostgresExtra(GenericOptions):
    # allows automatic conversion from text to target column type during write
    stringtype: str = "unspecified"

    # avoid closing connections from server side
    # while connector is moving data to executors before insert
    tcpKeepAlive: str = "true"  # noqa: N815

    class Config:
        extra = "allow"


@support_hooks
class Postgres(JDBCConnection):
    """PostgreSQL JDBC connection. |support_hooks|

    Based on Maven package `org.postgresql:postgresql:42.7.3 <https://mvnrepository.com/artifact/org.postgresql/postgresql/42.7.3>`_
    (`official Postgres JDBC driver <https://jdbc.postgresql.org/>`_).

    .. seealso::

        Before using this connector please take into account :ref:`postgres-prerequisites`

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of Postgres database. For example: ``test.postgres.domain.com`` or ``193.168.1.11``

    port : int, default: ``5432``
        Port of Postgres database

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

        For example: ``{"ssl": "false"}``

        See `Postgres JDBC driver properties documentation <https://jdbc.postgresql.org/documentation/use/>`_
        for more details

    Examples
    --------

    Postgres connection initialization

    .. code:: python

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
            extra={"ssl": "false"},
            spark=spark,
        )

    """

    host: Host
    database: str
    port: int = 5432
    extra: PostgresExtra = PostgresExtra()

    ReadOptions = PostgresReadOptions
    WriteOptions = PostgresWriteOptions
    SQLOptions = PostgresSQLOptions
    FetchOptions = PostgresFetchOptions
    ExecuteOptions = PostgresExecuteOptions

    Extra = PostgresExtra
    Dialect = PostgresDialect

    DRIVER: ClassVar[str] = "org.postgresql.Driver"

    @slot
    @classmethod
    def get_packages(cls, package_version: str | None = None) -> list[str]:
        """
        Get package names to be downloaded by Spark.  Allows specifying a custom JDBC driver version. |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        package_version : str, optional
            Specifies the version of the PostgreSQL JDBC driver to use.  Defaults to ``42.7.3``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Postgres

            Postgres.get_packages()

            # custom package version
            Postgres.get_packages(package_version="42.6.0")

        """
        default_version = "42.7.3"
        version = Version(package_version or default_version).min_digits(3)

        return [f"org.postgresql:postgresql:{version}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Postgres.package` will be removed in 1.0.0, use `Postgres.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.postgresql:postgresql:42.7.3"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_params(self) -> dict[str, str]:
        result = super().jdbc_params
        result.update(self.extra.dict(by_alias=True))
        result["ApplicationName"] = result.get("ApplicationName", self.spark.sparkContext.appName)
        return result

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    def _options_to_connection_properties(
        self,
        options: JDBCFetchOptions | JDBCExecuteOptions,
    ):  # noqa: WPS437
        # See https://github.com/pgjdbc/pgjdbc/pull/1252
        # Since 42.2.9 Postgres JDBC Driver added new option readOnlyMode=transaction
        # Which is not a desired behavior, because `.fetch()` method should always be read-only

        if not getattr(options, "readOnlyMode", None):
            options = options.copy(update={"readOnlyMode": "always"})

        return super()._options_to_connection_properties(options)
