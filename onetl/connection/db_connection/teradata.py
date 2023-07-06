#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from datetime import date, datetime
from typing import ClassVar, Optional

from onetl.connection.db_connection.jdbc_connection import JDBCConnection

# do not import PySpark here, as we allow user to use `Teradata.package` for creating Spark session


class Teradata(JDBCConnection):
    """Teradata JDBC connection. |support_hooks|

    Based on package ``com.teradata.jdbc:terajdbc:17.20.00.15``
    (`official Teradata JDBC driver <https://downloads.teradata.com/download/connectivity/jdbc-driver>`_).

    .. dropdown:: Version compatibility

        * Teradata server versions: 16.10 - 20.0
        * Spark versions: 2.3.x - 3.4.x
        * Java versions: 8 - 17

        See `official documentation <https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/platformMatrix.html>`_.

    .. warning::

        To use Teradata connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.4.1  # pass specific PySpark version

        See :ref:`spark-install` instruction for more details.

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

        extra = {
            "TMODE": "TERA",  # "TERA" or "ANSI"
            "LOGMECH": "LDAP",
            "LOG": "TIMING",  # increase log level
        }

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Teradata.package)
            .getOrCreate()
        )

        teradata = Teradata(
            host="database.host.or.ip",
            user="user",
            password="*****",
            extra=extra,
            spark=spark,
        )

    """

    class Extra(JDBCConnection.Extra):
        CHARSET: str = "UTF8"
        COLUMN_NAME: str = "ON"
        FLATTEN: str = "ON"
        MAYBENULL: str = "ON"
        STRICT_NAMES: str = "OFF"

        class Config:
            prohibited_options = frozenset(("DATABASE", "DBS_PORT"))

    port: int = 1025
    database: Optional[str] = None
    extra: Extra = Extra()

    driver: ClassVar[str] = "com.teradata.jdbc.TeraDriver"
    package: ClassVar[str] = "com.teradata.jdbc:terajdbc:17.20.00.15"

    _check_query: ClassVar[str] = "SELECT 1 AS check_result"

    @property
    def jdbc_url(self) -> str:
        prop = self.extra.dict(by_alias=True)

        if self.database:
            prop["DATABASE"] = self.database

        prop["DBS_PORT"] = self.port

        conn = ",".join(f"{k}={v}" for k, v in sorted(prop.items()))
        return f"jdbc:teradata://{self.host}/{conn}"

    class Dialect(JDBCConnection.Dialect):
        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            result = value.isoformat()
            return f"CAST('{result}' AS TIMESTAMP)"

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            result = value.isoformat()
            return f"CAST('{result}' AS DATE)"

    class ReadOptions(JDBCConnection.ReadOptions):
        # https://docs.teradata.com/r/w4DJnG9u9GdDlXzsTXyItA/lkaegQT4wAakj~K_ZmW1Dg
        @classmethod
        def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"HASHAMP(HASHBUCKET(HASHROW({partition_column}))) mod {num_partitions}"

        @classmethod
        def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"{partition_column} mod {num_partitions}"

    ReadOptions.__doc__ = JDBCConnection.ReadOptions.__doc__
