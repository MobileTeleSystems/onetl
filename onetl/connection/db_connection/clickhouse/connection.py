# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import ClassVar, Optional

from onetl._util.classproperty import classproperty
from onetl._util.version import Version
from onetl.connection.db_connection.clickhouse.dialect import ClickhouseDialect
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

    Based on Maven package `com.clickhouse:clickhouse-jdbc:0.6.0 <https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc/0.6.0>`_
    (`official Clickhouse JDBC driver <https://github.com/ClickHouse/clickhouse-jdbc>`_).

    .. warning::

        Before using this connector please take into account :ref:`clickhouse-prerequisites`

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

    Clickhouse connection initialization

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
        )

    """

    port: int = 8123
    database: Optional[str] = None
    extra: ClickhouseExtra = ClickhouseExtra()

    Extra = ClickhouseExtra
    Dialect = ClickhouseDialect

    DRIVER: ClassVar[str] = "com.clickhouse.jdbc.ClickHouseDriver"

    @slot
    @classmethod
    def get_packages(
        cls,
        package_version: str | None = None,
        apache_http_client_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Parameters
        ----------
        package_version : str , optional
             ClickHouse JDBC version client packages. Defaults to ``0.6.0``.

        apache_http_client_version : str, optional
             Apache HTTP Client version package. Defaults to ``5.3.1``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Clickhouse

            Clickhouse.get_packages(package_version="0.6.0", apache_http_client_version="5.3.1")

        .. note::

             Spark does not support ``.jar`` classifiers, so it is not possible to pass
             ``com.clickhouse:clickhouse-jdbc:0.6.0:all`` to install all required packages.

        """
        package_version_obj = Version(package_version) if package_version else Version("0.6.0")
        apache_http_client_version_obj = (
            Version(apache_http_client_version) if apache_http_client_version else Version("5.3.1")
        )
        if len(package_version_obj) != 3 or len(apache_http_client_version_obj) != 3:
            raise ValueError("Version should consist of exactly three numeric_parts (major.minor.patch)")

        result = [
            f"com.clickhouse:clickhouse-jdbc:{package_version_obj}",
            f"com.clickhouse:clickhouse-http-client:{package_version_obj}",
        ]

        if package_version_obj >= Version("0.5.0"):
            #  before 0.5.0 builtin Java HTTP Client was used
            result.append(f"org.apache.httpcomponents.client5:httpclient5:{apache_http_client_version_obj}")

        return result

    @classproperty
    def package(self) -> str:
        """Get a single string of package names to be downloaded by Spark for establishing a Clickhouse connection."""
        return "com.clickhouse:clickhouse-jdbc:0.6.0,com.clickhouse:clickhouse-http-client:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.3.1"

    @property
    def jdbc_url(self) -> str:
        extra = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))

        if self.database:
            return f"jdbc:clickhouse://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

        return f"jdbc:clickhouse://{self.host}:{self.port}?{parameters}".rstrip("?")

    @staticmethod
    def _build_statement(
        statement: str,
        statement_type: JDBCStatementType,
        jdbc_connection,
        statement_args,
    ):
        # Clickhouse does not support prepared statements, as well as calling functions/procedures
        return jdbc_connection.createStatement(*statement_args)
