# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import ClassVar

from onetl._util.classproperty import classproperty
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_mixin.options import JDBCOptions
from onetl.connection.db_connection.postgres.dialect import PostgresDialect
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

# do not import PySpark here, as we allow user to use `Postgres.get_packages()` for creating Spark session


class PostgresExtra(GenericOptions):
    class Config:
        extra = "allow"


@support_hooks
class Postgres(JDBCConnection):
    """PostgreSQL JDBC connection. |support_hooks|

    Based on Maven package ``org.postgresql:postgresql:42.6.0``
    (`official Postgres JDBC driver <https://jdbc.postgresql.org/>`_).

    .. dropdown:: Version compatibility

        * PostgreSQL server versions: 8.2 or higher
        * Spark versions: 2.3.x - 3.5.x
        * Java versions: 8 - 20

        See `official documentation <https://jdbc.postgresql.org/download/>`_.

    .. warning::

        To use Postgres connector you should have PySpark installed (or injected to ``sys.path``)
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

        See `Postgres JDBC driver properties documentation <https://github.com/pgjdbc/pgjdbc#connection-properties>`_
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

    database: str
    port: int = 5432
    extra: PostgresExtra = PostgresExtra()

    Extra = PostgresExtra
    Dialect = PostgresDialect

    DRIVER: ClassVar[str] = "org.postgresql.Driver"

    @slot
    @classmethod
    def get_packages(cls) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Examples
        --------

        .. code:: python

            from onetl.connection import Postgres

            Postgres.get_packages()

        """
        return ["org.postgresql:postgresql:42.6.0"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Postgres.package` will be removed in 1.0.0, use `Postgres.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.postgresql:postgresql:42.6.0"

    @property
    def jdbc_url(self) -> str:
        extra = self.extra.dict(by_alias=True)
        extra["ApplicationName"] = extra.get("ApplicationName", self.spark.sparkContext.appName)

        parameters = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

    @property
    def instance_url(self) -> str:
        return f"{super().instance_url}/{self.database}"

    def _options_to_connection_properties(self, options: JDBCOptions):  # noqa: WPS437
        # See https://github.com/pgjdbc/pgjdbc/pull/1252
        # Since 42.2.9 Postgres JDBC Driver added new option readOnlyMode=transaction
        # Which is not a desired behavior, because `.fetch()` method should always be read-only

        if not getattr(options, "readOnlyMode", None):
            options = options.copy(update={"readOnlyMode": "always"})

        return super()._options_to_connection_properties(options)
