# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import warnings
from typing import ClassVar, Optional

from onetl._util.classproperty import classproperty
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

    Based on Maven package ``ru.yandex.clickhouse:clickhouse-jdbc:0.3.2``
    (`official Clickhouse JDBC driver <https://github.com/ClickHouse/clickhouse-jdbc>`_).

    .. dropdown:: Version compatibility

        * Clickhouse server versions: 20.7 or higher
        * Spark versions: 2.3.x - 3.5.x
        * Java versions: 8 - 20

        See `official documentation <https://clickhouse.com/docs/en/integrations/java#jdbc-driver>`_.

    .. warning::

        To use Clickhouse connector you should have PySpark installed (or injected to ``sys.path``)
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

        See `Clickhouse JDBC driver properties documentation
        <https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-jdbc#configuration>`_
        for more details

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

    DRIVER: ClassVar[str] = "ru.yandex.clickhouse.ClickHouseDriver"

    @slot
    @classmethod
    def get_packages(cls) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Examples
        --------

        .. code:: python

            from onetl.connection import Clickhouse

            Clickhouse.get_packages()

        """
        return ["ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Clickhouse.package` will be removed in 1.0.0, use `Clickhouse.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"

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
