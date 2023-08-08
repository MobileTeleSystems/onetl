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

import logging
from abc import abstractmethod
from contextlib import closing, suppress
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional, Tuple, TypeVar

from pydantic import Field, PrivateAttr, SecretStr, validator

from onetl._internal import clear_statement, stringify
from onetl._util.java import get_java_gateway, try_import_java_class
from onetl._util.spark import get_spark_version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel, GenericOptions
from onetl.log import log_lines

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

T = TypeVar("T")

# options generated by JDBCMixin methods
PROHIBITED_OPTIONS = frozenset(
    (
        "user",
        "password",
        "driver",
        "url",
    ),
)


class StatementType(Enum):
    GENERIC = auto()
    PREPARED = auto()
    CALL = auto()


@support_hooks
class JDBCMixin(FrozenModel):
    """
    Compatibility layer between Python and Java SQL Module.

    Spark does not allow to execute raw SQL/DDL/DML/etc statements, so this is a workaround.
    However, some of Spark's magic is used here, for example to convert raw ResultSet to move convenient DataFrame
    """

    spark: SparkSession = Field(repr=False)
    user: str
    password: SecretStr
    DRIVER: ClassVar[str]
    _CHECK_QUERY: ClassVar[str] = "SELECT 1"

    class JDBCOptions(GenericOptions):
        """Generic options, related to specific JDBC driver.

        .. note ::

            You can pass any value
            supported by underlying JDBC driver class,
            even if it is not mentioned in this documentation.
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            extra = "allow"

        query_timeout: Optional[int] = Field(default=None, alias="queryTimeout")
        """The number of seconds the driver will wait for a statement to execute.
        Zero means there is no limit.

        This option depends on driver implementation,
        some drivers can check the timeout of each query instead of an entire JDBC batch.
        """

        fetchsize: Optional[int] = None
        """How many rows to fetch per round trip.

        Tuning this option can influence performance of reading.

        .. warning::

            Default value depends on driver. For example, Oracle has
            default ``fetchsize=10``.
        """

    # cached JDBC connection (Java object), plus corresponding GenericOptions (Python object)
    _last_connection_and_options: Optional[Tuple[Any, JDBCOptions]] = PrivateAttr(default=None)

    @property
    @abstractmethod
    def jdbc_url(self) -> str:
        """JDBC Connection URL"""

    @slot
    def close(self):
        """
        Close all connections, opened by ``.fetch()``, ``.execute()`` or ``.check()`` methods. |support_hooks|

        Examples
        --------

        Read data and close connection:

        .. code:: python

            df = connection.fetch("SELECT * FROM mytable")
            assert df.count()
            connection.close()

            # or

            with connection:
                connection.execute("CREATE TABLE target_table(id NUMBER, data VARCHAR)")
                connection.execute("CREATE INDEX target_table_idx ON target_table (id)")

        """

        self._close_connections()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):  # noqa: U101
        self.close()

    def __del__(self):  # noqa: WPS603
        # If current object is collected by GC, close all opened connections
        self.close()

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()  # type: ignore

        log.debug("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(self._CHECK_QUERY, level=logging.DEBUG)

        try:
            self._query_optional_on_driver(self._CHECK_QUERY, self.JDBCOptions(fetchsize=1))  # type: ignore
            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e

        return self

    @slot
    def fetch(
        self,
        query: str,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> DataFrame:
        """
        **Immediately** execute SELECT statement **on Spark driver** and return in-memory DataFrame. |support_hooks|

        Works almost the same like :obj:`~sql`, but calls JDBC driver directly.

        .. warning::

            This function should **NOT** be used to return dataframe with large number of rows/columns,
            like ``SELECT * FROM table`` (without any filtering).

            Use it **ONLY** to execute queries with **one or just a few returning rows/columns**,
            like ``SELECT COUNT(*) FROM table`` or ``SELECT * FROM table WHERE id = ...``.

            This is because all the data is fetched through master host, not in a distributed way
            (even on clustered instances).
            Also the resulting dataframe is stored directly in driver's memory, which is usually quite limited.

            Use :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` for reading
            data from table via Spark executors, or for handling different :ref:`strategy`.

        .. note::

            Statement is executed in read-only connection, so it cannot change any data in the database.

        .. note::

            First call of the method opens the connection to a database.
            Call ``.close()`` method to close it, or use context manager to do it automatically.

        Parameters
        ----------
        query : str

            SQL query to be executed.

            Can be in any form of SELECT supported by RDBMS, like:

            * ``SELECT ... FROM ...``
            * ``WITH ... AS (...) SELECT ... FROM ...``
            * *Some* RDBMS instances also support ``SHOW ...`` queries, like ``SHOW TABLES``

            .. warning::

                The exact syntax **depends on a RDBMS** is being used.

        options : dict, :obj:`~JDBCOptions`, default: ``None``

            Options to be passed directly to JDBC driver, like ``fetchsize`` or ``queryTimeout``

            .. note::

                You cannot use :obj:`~ReadOptions`, they are handled by Spark, not by JDBC driver itself

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame

            Spark dataframe

        Examples
        --------

        Read data from a table:

        .. code:: python

            df = connection.fetch("SELECT * FROM mytable")
            assert df.count()

        Read data from a table with options:

        .. code:: python

            # reads data from table in batches, 10000 rows per batch
            df = connection.fetch("SELECT * FROM mytable", {"fetchsize": 10000})
            assert df.count()
        """

        query = clear_statement(query)

        log.info("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(query)

        df = self._query_on_driver(query, self.JDBCOptions.parse(options))

        log.info(
            "|%s| Query succeeded, resulting in-memory dataframe contains %d rows",
            self.__class__.__name__,
            df.count(),
        )
        return df

    @slot
    def execute(
        self,
        statement: str,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> DataFrame | None:
        """
        **Immediately** execute DDL, DML or procedure/function **on Spark driver**. |support_hooks|

        Returns DataFrame only if input is DML statement with ``RETURNING ...`` clause, or a procedure/function call.
        In other cases returns ``None``.

        There is no method like this in :obj:`pyspark.sql.SparkSession` object,
        but Spark internal methods works almost the same (but on executor side).

        .. warning::

            Use this method **ONLY** to execute queries which return **small number of rows/column,
            or return nothing**.

            This is because all the data is fetched through master host, not in a distributed way
            (even on clustered instances).
            Also the resulting dataframe is stored directly in driver's memory, which is usually quite limited.

            Please **DO NOT** use this method to execute queries
            like ``INSERT INTO ... VALUES(a, lot, of, values)``,
            use :obj:`onetl.db.db_writer.db_writer.DBWriter` instead

        .. note::

            First call of the method opens the connection to a database.
            Call ``.close()`` method to close it, or use context manager to do it automatically.

        Parameters
        ----------
        statement : str

            Statement to be executed, like:

            DML statements:

            * ``INSERT INTO target_table SELECT * FROM source_table``
            * ``UPDATE mytable SET value = 1 WHERE id BETWEEN 100 AND 999``
            * ``DELETE FROM mytable WHERE id BETWEEN 100 AND 999``
            * ``TRUNCATE TABLE mytable``

            DDL statements:

            * ``CREATE TABLE mytable (...)``
            * ``ALTER SCHEMA myschema ...``
            * ``DROP PROCEDURE myprocedure``

            Call statements:

            * ``BEGIN ... END``
            * ``EXEC myprocedure``
            * ``EXECUTE myprocedure(arg1)``
            * ``CALL myfunction(arg1, arg2)``
            * ``{call myprocedure(arg1, ?)}`` (``?`` is output parameter)
            * ``{?= call myfunction(arg1, arg2)}``

            .. warning::

                The exact syntax **depends on a RDBMS** is being used.

        options : dict, :obj:`~JDBCOptions`, default: ``None``

            Options to be passed directly to JDBC driver, like ``queryTimeout``

            .. note::

                You cannot use :obj:`~WriteOptions`, they are handled by Spark, not by JDBC driver itself

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame, optional

            Spark dataframe

        Examples
        --------

        Create table:

        .. code:: python

            assert connection.execute("CREATE TABLE target_table(id NUMBER, data VARCHAR)") is None

        Insert data to one table from another, with a specific transaction isolation level,
        and return DataFrame with new rows:

        .. code:: python

            df = connection.execute(
                "INSERT INTO target_table SELECT * FROM source_table RETURNING id, data",
                {"isolationLevel": "READ_COMMITTED"},
            )
            assert df.count()
        """

        statement = clear_statement(statement)

        log.info("|%s| Executing statement (on driver):", self.__class__.__name__)
        log_lines(statement)

        call_options = self.JDBCOptions.parse(options)
        df = self._call_on_driver(statement, call_options)

        if df is not None:
            rows_count = df.count()
            log.info(
                "|%s| Execution succeeded, resulting in-memory dataframe contains %d rows",
                self.__class__.__name__,
                rows_count,
            )
        else:
            log.info("|%s| Execution succeeded, nothing returned", self.__class__.__name__)
        return df

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        try:
            try_import_java_class(spark, cls.DRIVER)
        except Exception as e:
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=cls.DRIVER,
                package_source=cls.__name__,
                args="",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    def _query_on_driver(
        self,
        query: str,
        options: JDBCMixin.JDBCOptions,
    ) -> DataFrame:
        return self._execute_on_driver(
            statement=query,
            statement_type=StatementType.PREPARED,
            callback=self._statement_to_dataframe,
            options=options,
            read_only=True,
        )

    def _query_optional_on_driver(
        self,
        query: str,
        options: JDBCMixin.JDBCOptions,
    ) -> DataFrame | None:
        return self._execute_on_driver(
            statement=query,
            statement_type=StatementType.PREPARED,
            callback=self._statement_to_optional_dataframe,
            options=options,
            read_only=True,
        )

    def _call_on_driver(
        self,
        query: str,
        options: JDBCMixin.JDBCOptions,
    ) -> DataFrame | None:
        return self._execute_on_driver(
            statement=query,
            statement_type=StatementType.CALL,
            callback=self._statement_to_optional_dataframe,
            options=options,
            read_only=False,
        )

    def _get_jdbc_properties(
        self,
        options: JDBCOptions,
        **kwargs,
    ) -> dict:
        """
        Fills up human-readable Options class to a format required by Spark internal methods
        """

        result = options.copy(
            update={
                "user": self.user,
                "password": self.password.get_secret_value() if self.password is not None else "",
                "driver": self.DRIVER,
                "url": self.jdbc_url,
            },
        ).dict(by_alias=True, **kwargs)

        return stringify(result)

    def _options_to_connection_properties(self, options: JDBCOptions):
        """
        Converts human-readable Options class to ``java.util.Properties``.

        Spark's internal class ``JDBCOptions`` already contains all the magic we need.

        See:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCOptions.scala#L148
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L248-L255
        """

        jdbc_properties = self._get_jdbc_properties(options, exclude_unset=True)

        jdbc_utils_package = self.spark._jvm.org.apache.spark.sql.execution.datasources.jdbc  # type: ignore
        jdbc_options = jdbc_utils_package.JDBCOptions(
            self.jdbc_url,
            # JDBCOptions class requires `table` argument to be passed, but it is not used in asConnectionProperties
            "table",
            self.spark._jvm.PythonUtils.toScalaMap(jdbc_properties),  # type: ignore
        )
        return jdbc_options.asConnectionProperties()

    def _get_jdbc_connection(self, options: JDBCOptions):
        with suppress(Exception):  # nothing cached, or JVM failed
            last_connection, last_options = self._last_connection_and_options
            if options == last_options and not last_connection.isClosed():
                return last_connection

            # only one connection can be opened in one moment of time
            last_connection.close()

        connection_properties = self._options_to_connection_properties(options)
        driver_manager = self.spark._jvm.java.sql.DriverManager  # type: ignore
        new_connection = driver_manager.getConnection(self.jdbc_url, connection_properties)

        self._last_connection_and_options = (new_connection, options)
        return new_connection

    def _close_connections(self):
        with suppress(Exception):
            last_connection, _ = self._last_connection_and_options
            last_connection.close()

        self._last_connection_and_options = None

    def _get_statement_args(self) -> tuple[int, ...]:
        resultset = self.spark._jvm.java.sql.ResultSet  # type: ignore

        return resultset.TYPE_FORWARD_ONLY, resultset.CONCUR_READ_ONLY

    def _execute_on_driver(
        self,
        statement: str,
        statement_type: StatementType,
        callback: Callable[..., T],
        options: JDBCOptions,
        read_only: bool,
    ) -> T:
        """
        Actually execute statement on driver.

        Almost like ``org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD`` is fetching data:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L297-L306
        """

        jdbc_connection = self._get_jdbc_connection(options)
        jdbc_connection.setReadOnly(read_only)  # type: ignore

        statement_args = self._get_statement_args()
        jdbc_statement = self._build_statement(statement, statement_type, jdbc_connection, statement_args)

        return self._execute_statement(jdbc_statement, statement, options, callback, read_only)

    def _execute_statement(
        self,
        jdbc_statement,
        statement: str,
        options: JDBCOptions,
        callback: Callable[..., T],
        read_only: bool,
    ) -> T:
        """
        Executes ``java.sql.Statement`` or child class and passes it into the callback function.

        See:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L255-L257
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L298-L301
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L103-L105
        """
        from py4j.java_gateway import is_instance_of

        gateway = get_java_gateway(self.spark)
        prepared_statement = self.spark._jvm.java.sql.PreparedStatement  # type: ignore
        callable_statement = self.spark._jvm.java.sql.CallableStatement  # type: ignore

        with closing(jdbc_statement):
            if options.fetchsize is not None:
                jdbc_statement.setFetchSize(options.fetchsize)

            if options.query_timeout is not None:
                jdbc_statement.setQueryTimeout(options.query_timeout)

            # Java SQL classes are not consistent..
            if is_instance_of(gateway, jdbc_statement, prepared_statement):
                jdbc_statement.execute()
            elif is_instance_of(gateway, jdbc_statement, callable_statement):
                jdbc_statement.execute()
            elif read_only:
                jdbc_statement.executeQuery(statement)
            else:
                jdbc_statement.executeUpdate(statement)

            return callback(jdbc_statement)

    @staticmethod
    def _build_statement(
        statement: str,
        statement_type: StatementType,
        jdbc_connection,
        statement_args,
    ):
        """
        Builds ``java.sql.Statement``, ``java.sql.PreparedStatement`` or ``java.sql.CallableStatement``,
        depending on ``statement_type`` argument value.

        Raw ``java.sql.Statement`` does not support some features provided by db driver, like ``{call ...}`` syntax.
        This is handled by ``java.sql.PreparedStatement`` or ``java.sql.CallableStatement``.

        See:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L298-L299
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L633
        """

        if statement_type == StatementType.PREPARED:
            return jdbc_connection.prepareStatement(statement, *statement_args)

        if statement_type == StatementType.CALL:
            return jdbc_connection.prepareCall(statement, *statement_args)

        return jdbc_connection.createStatement(*statement_args)

    def _statement_to_dataframe(self, jdbc_statement) -> DataFrame:
        result_set = jdbc_statement.getResultSet()
        return self._resultset_to_dataframe(result_set)

    def _statement_to_optional_dataframe(self, jdbc_statement) -> DataFrame | None:
        """
        Returns ``org.apache.spark.sql.DataFrame`` or ``None``, if ResultSet is does not contain any columns.

        DDL or DML statement without ``RETURNING`` clause usually do not return anything.
        """

        result_set = jdbc_statement.getResultSet()

        if not result_set:
            return None

        result_metadata = result_set.getMetaData()
        result_column_count = result_metadata.getColumnCount()
        if not result_column_count:
            return None

        return self._resultset_to_dataframe(result_set)

    def _resultset_to_dataframe(self, result_set) -> DataFrame:
        """
        Converts ``java.sql.ResultSet`` to ``org.apache.spark.sql.DataFrame`` using Spark's internal methods.

        That's almost exactly like Spark is fetching the data, but on driver.

        See:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L297-L306
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L317-L323
        """

        from pyspark.sql import DataFrame  # noqa: WPS442

        jdbc_dialects_package = self.spark._jvm.org.apache.spark.sql.jdbc  # type: ignore
        jdbc_dialect = jdbc_dialects_package.JdbcDialects.get(self.jdbc_url)

        jdbc_utils_package = self.spark._jvm.org.apache.spark.sql.execution.datasources.jdbc  # type: ignore
        jdbc_utils = jdbc_utils_package.JdbcUtils

        java_converters = self.spark._jvm.scala.collection.JavaConverters  # type: ignore

        if get_spark_version(self.spark) >= (3, 4):
            # https://github.com/apache/spark/commit/2349175e1b81b0a61e1ed90c2d051c01cf78de9b
            result_schema = jdbc_utils.getSchema(result_set, jdbc_dialect, False, False)  # noqa: WPS425
        else:
            result_schema = jdbc_utils.getSchema(result_set, jdbc_dialect, False)  # noqa: WPS425

        result_iterator = jdbc_utils.resultSetToRows(result_set, result_schema)
        result_list = java_converters.seqAsJavaListConverter(result_iterator.toSeq()).asJava()
        jdf = self.spark._jsparkSession.createDataFrame(result_list, result_schema)  # type: ignore

        # DataFrame constructor in Spark 2.3 and 2.4 required second argument to be a SQLContext class
        # E.g. spark._wrapped = SQLContext(spark).
        # But since 3.2 it is replaced with SparkSession itself, and in 3.3 "_wrapped"
        # attribute was removed from SparkSession
        spark_context = getattr(self.spark, "_wrapped", self.spark)

        return DataFrame(jdf, spark_context)  # type: ignore
