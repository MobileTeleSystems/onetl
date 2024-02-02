# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import ClassVar, Optional

from onetl._util.classproperty import classproperty
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.teradata.dialect import TeradataDialect
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

    Based on package ``com.teradata.jdbc:terajdbc:17.20.00.15``
    (`official Teradata JDBC driver <https://downloads.teradata.com/download/connectivity/jdbc-driver>`_).

    .. dropdown:: Version compatibility

        * Teradata server versions: 16.10 - 20.0
        * Spark versions: 2.3.x - 3.5.x
        * Java versions: 8 - 20

        See `official documentation <https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/platformMatrix.html>`_.

    .. warning::

        To use Teradata connector you should have PySpark installed (or injected to ``sys.path``)
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
                * ``COLUMN_NAME = "ON"``
                * ``FLATTEN = "ON"``
                * ``MAYBENULL = "ON"``
                * ``STRICT_NAMES = "OFF"``

            It is possible to override default values, for example set ``extra={"FLATTEN": "OFF"}``

    Examples
    --------

    Teradata connection with LDAP auth:

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
        )

    """

    port: int = 1025
    database: Optional[str] = None
    extra: TeradataExtra = TeradataExtra()

    Extra = TeradataExtra
    Dialect = TeradataDialect

    DRIVER: ClassVar[str] = "com.teradata.jdbc.TeraDriver"
    _CHECK_QUERY: ClassVar[str] = "SELECT 1 AS check_result"

    @slot
    @classmethod
    def get_packages(cls) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Examples
        --------

        .. code:: python

            from onetl.connection import Teradata

            Teradata.get_packages()

        """
        return ["com.teradata.jdbc:terajdbc:17.20.00.15"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Teradata.package` will be removed in 1.0.0, use `Teradata.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.teradata.jdbc:terajdbc:17.20.00.15"

    @property
    def jdbc_url(self) -> str:
        prop = self.extra.dict(by_alias=True)

        if self.database:
            prop["DATABASE"] = self.database

        prop["DBS_PORT"] = self.port

        conn = ",".join(f"{k}={v}" for k, v in sorted(prop.items()))
        return f"jdbc:teradata://{self.host}/{conn}"
