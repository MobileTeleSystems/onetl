# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import ClassVar, Optional

from etl_entities.instance import Host

from onetl._util.classproperty import classproperty
from onetl._util.spark import get_client_info
from onetl._util.version import Version
from onetl.connection.db_connection.clickhouse.dialect import ClickhouseDialect
from onetl.connection.db_connection.clickhouse.options import (
    ClickhouseExecuteOptions,
    ClickhouseFetchOptions,
    ClickhouseReadOptions,
    ClickhouseSQLOptions,
    ClickhouseWriteOptions,
)
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_mixin import JDBCStatementType
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

# do not import PySpark here, as we allow user to use `Clickhouse.get_packages()` for creating Spark session


log = logging.getLogger(__name__)


class ClickhouseExtra(GenericOptions):
    class Config:
        extra = "allow"


@support_hooks
class Clickhouse(JDBCConnection):
    """Clickhouse JDBC connection. |support_hooks|

    Based on Maven package `com.clickhouse:clickhouse-jdbc:0.7.2 <https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc/0.7.2>`_
    (`official Clickhouse JDBC driver <https://github.com/ClickHouse/clickhouse-jdbc>`_).

    .. seealso::

        Before using this connector please take into account :ref:`clickhouse-prerequisites`

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of Clickhouse database. For example: ``test.clickhouse.domain.com`` or ``193.168.1.11``

    port : int, default: ``8123``
        Port of Clickhouse database

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
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"continueBatchOnError": "false"}``.

        See:
            * `Clickhouse JDBC driver properties documentation <https://clickhouse.com/docs/en/integrations/java#configuration>`_
            * `Clickhouse core settings documentation <https://clickhouse.com/docs/en/operations/settings/settings>`_
            * `Clickhouse query complexity documentation <https://clickhouse.com/docs/en/operations/settings/query-complexity>`_
            * `Clickhouse query level settings <https://clickhouse.com/docs/en/operations/settings/query-level>`_

    Examples
    --------

    Create and check Clickhouse connection:

    .. code:: python

        from onetl.connection import Clickhouse
        from pyspark.sql import SparkSession

        # Create Spark session with Clickhouse driver loaded
        maven_packages = Clickhouse.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        clickhouse = Clickhouse(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra={"continueBatchOnError": "false"},
            spark=spark,
        ).check()

    """

    host: Host
    port: int = 8123
    database: Optional[str] = None
    extra: ClickhouseExtra = ClickhouseExtra()

    Extra = ClickhouseExtra
    Dialect = ClickhouseDialect

    ReadOptions = ClickhouseReadOptions
    WriteOptions = ClickhouseWriteOptions
    SQLOptions = ClickhouseSQLOptions
    FetchOptions = ClickhouseFetchOptions
    ExecuteOptions = ClickhouseExecuteOptions

    DRIVER: ClassVar[str] = "com.clickhouse.jdbc.ClickHouseDriver"

    @slot
    @classmethod
    def get_packages(
        cls,
        package_version: str | None = None,
        apache_http_client_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. Allows specifying custom JDBC and Apache HTTP Client versions. |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        package_version : str, optional
            ClickHouse JDBC version client packages. Defaults to ``0.7.2``.

            .. versionadded:: 0.11.0

        apache_http_client_version : str, optional
            Apache HTTP Client version package. Defaults to ``5.4.2``.

            Used only if ``package_version`` is in range ``0.5.0-0.7.0``.

            .. versionadded:: 0.11.0

        Examples
        --------

        .. code:: python

            from onetl.connection import Clickhouse

            Clickhouse.get_packages(package_version="0.6.0", apache_http_client_version="5.4.2")

        """
        default_jdbc_version = "0.7.2"
        default_http_version = "5.4.2"

        jdbc_version = Version(package_version or default_jdbc_version).min_digits(3)
        http_version = Version(apache_http_client_version or default_http_version).min_digits(3)

        result = [
            f"com.clickhouse:clickhouse-jdbc:{jdbc_version}",
            f"com.clickhouse:clickhouse-http-client:{jdbc_version}",
        ]

        if Version("0.5.0") <= jdbc_version <= Version("0.7.0"):
            result.append(f"org.apache.httpcomponents.client5:httpclient5:{http_version}")

        return result

    @classproperty
    def package(self) -> str:
        """Get a single string of package names to be downloaded by Spark for establishing a Clickhouse connection."""
        return "com.clickhouse:clickhouse-jdbc:0.7.2,com.clickhouse:clickhouse-http-client:0.7.2,org.apache.httpcomponents.client5:httpclient5:5.4.2"

    @property
    def jdbc_url(self) -> str:
        if self.database:
            return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}"

        return f"jdbc:clickhouse://{self.host}:{self.port}"

    @property
    def jdbc_params(self) -> dict:
        result = super().jdbc_params
        result.update(self.extra.dict(by_alias=True))
        # https://github.com/ClickHouse/clickhouse-java/issues/691#issuecomment-975545784
        result["client_name"] = result.get("client_name", get_client_info(self.spark))
        return result

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"

    @staticmethod
    def _build_statement(
        statement: str,
        statement_type: JDBCStatementType,
        jdbc_connection,
        statement_args,
    ):
        # Clickhouse does not support prepared statements, as well as calling functions/procedures
        return jdbc_connection.createStatement(*statement_args)
