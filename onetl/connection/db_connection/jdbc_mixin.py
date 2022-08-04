from __future__ import annotations

from abc import abstractmethod
from contextlib import closing, suppress
from dataclasses import dataclass, field
from enum import Enum, auto
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar

from pydantic import BaseModel

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from onetl._internal import stringify, to_camel  # noqa: WPS436

log = getLogger(__name__)

T = TypeVar("T")


class StatementType(Enum):
    GENERIC = auto()
    PREPARED = auto()
    CALL = auto()


@dataclass(frozen=True)
class JDBCMixin:
    """
    Compatibility layer between Python and Java SQL Module.

    Spark does not allow to execute raw SQL/DDL/DML/etc statements, so this is a workaround.
    However, some of Spark's magic is used here, for example to convert raw ResultSet to move convenient DataFrame
    """

    spark: SparkSession
    user: str
    password: str
    driver: str

    class Options(BaseModel):  # noqa: WPS431
        fetchsize: int = 100000
        """How many rows to fetch per round trip.

        Tuning this option can influence performance of writing.

        .. warning::

            Default value is different from Spark.

            Spark uses driver's own value, and it may be different in different drivers,
            and even versions of the same driver. For example, Oracle has
            default ``fetchsize=10``, which is absolutely not usable.

            Thus we've overridden default value with ``100_000``, which should increase reading performance.

        .. warning::

            Used **only** while **reading** data from a table.
        """

        isolation_level: str = "READ_UNCOMMITTED"
        """The transaction isolation level, which applies to current connection.

        Possible values:
            * ``NONE`` (as string, not Python's ``None``)
            * ``READ_COMMITTED``
            * ``READ_UNCOMMITTED``
            * ``REPEATABLE_READ``
            * ``SERIALIZABLE``

        Values correspond to transaction isolation levels defined by JDBC standard.
        Please refer the documentation for
        `java.sql.Connection <https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html>`_.

        .. warning::

            Used **only** while **reading** data from a table.
        """

        query_timeout: int = 0
        """The number of seconds the driver will wait for a statement to execute.
        Zero means there is no limit.

        This option depends on driver implementation,
        some drivers can check the timeout of each query instead of an entire JDBC batch.

        .. note::

            Used while both reading data from and writing data to a table.
        """

        class Config:  # noqa: WPS431
            alias_generator = to_camel
            allow_population_by_field_name = True
            frozen = True
            extra = "allow"

    # cached JDBC connection (Java object), plus corresponding Options (Python object)
    _last_connection_and_options: Optional[tuple[Any, Options]] = field(default=None, init=False)

    @property
    @abstractmethod
    def jdbc_url(self) -> str:
        """JDBC Connection URL"""

    def __del__(self):  # noqa: WPS603
        # If current object is collected by GC, close all opened connections
        self._close_connections()

    def _get_jdbc_properties(
        self,
        options: Options,
        **kwargs,
    ) -> dict:
        """
        Fills up human-readable Options class to a format required by Spark internal methods
        """

        result = options.copy(
            update={
                "user": self.user,
                "password": self.password,
                "driver": self.driver,
                "url": self.jdbc_url,
            },
        ).dict(by_alias=True, **kwargs)

        return stringify(result)

    def _options_to_connection_properties(self, options: Options):
        """
        Converts human-readable Options class to ``java.util.Properties``.

        Spark's internal class ``JDBCOptions`` already contains all the magic we need.

        See:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCOptions.scala#L148
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L248-L255
        """

        jdbc_properties = self._get_jdbc_properties(options, exclude_unset=True)

        jdbc_utils_package = self.spark._jvm.org.apache.spark.sql.execution.datasources.jdbc
        jdbc_options = jdbc_utils_package.JDBCOptions(
            self.jdbc_url,
            # JDBCOptions class requires `table` argument to be passed, but it is not used in asConnectionProperties
            "table",
            self.spark._jvm.PythonUtils.toScalaMap(jdbc_properties),
        )
        return jdbc_options.asConnectionProperties()

    def _get_jdbc_connection(self, options: Options):
        with suppress(Exception):  # nothing cached, or JVM failed
            last_connection, last_options = self._last_connection_and_options
            if options == last_options and not last_connection.isClosed():
                return last_connection

            # only one connection can be opened in one moment of time
            last_connection.close()

        connection_properties = self._options_to_connection_properties(options)
        driver_manager = self.spark._sc._gateway.jvm.java.sql.DriverManager
        new_connection = driver_manager.getConnection(self.jdbc_url, connection_properties)

        # hack to bypass dataclass(frozen=True)
        object.__setattr__(self, "_last_connection_and_options", (new_connection, options))  # noqa: WPS609
        return new_connection

    def _close_connections(self):
        with suppress(Exception):
            last_connection, _ = self._last_connection_and_options
            last_connection.close()

        object.__setattr__(self, "_last_connection_and_options", None)  # noqa: WPS609

    def _get_statement_args(self) -> tuple[int, ...]:
        ResultSet = self.spark._jvm.java.sql.ResultSet

        return ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY

    def _execute_on_driver(
        self,
        statement: str,
        statement_type: StatementType,
        callback: Callable[..., T],
        options: Options,
        read_only: bool,
    ) -> T:
        """
        Actually execute statement on driver.

        Almost like ``org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD`` is fetching data:
        * https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala#L297-L306
        """

        jdbc_connection = self._get_jdbc_connection(options)
        jdbc_connection.setReadOnly(read_only)

        statement_args = self._get_statement_args()
        jdbc_statement = self._build_statement(statement, statement_type, jdbc_connection, statement_args)

        return self._execute_statement(jdbc_statement, statement, options, callback, read_only)

    def _execute_statement(
        self,
        jdbc_statement,
        statement: str,
        options: Options,
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

        def jvm_is_instance(obj, klass):  # noqa: WPS430
            import py4j

            return py4j.java_gateway.is_instance_of(self.spark._sc._gateway, obj, klass)

        PreparedStatement = self.spark._jvm.java.sql.PreparedStatement
        CallableStatement = self.spark._jvm.java.sql.CallableStatement

        with closing(jdbc_statement):
            if options.fetchsize:
                jdbc_statement.setFetchSize(options.fetchsize)

            if options.query_timeout:
                jdbc_statement.setQueryTimeout(options.query_timeout)

            # Java SQL classes are not consistent..
            if jvm_is_instance(jdbc_statement, PreparedStatement) or jvm_is_instance(jdbc_statement, CallableStatement):
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

        jdbc_dialects_package = self.spark._jvm.org.apache.spark.sql.jdbc
        jdbc_dialect = jdbc_dialects_package.JdbcDialects.get(self.jdbc_url)

        jdbc_utils_package = self.spark._jvm.org.apache.spark.sql.execution.datasources.jdbc
        jdbc_utils = jdbc_utils_package.JdbcUtils

        java_converters = self.spark._jvm.scala.collection.JavaConverters

        result_schema = jdbc_utils.getSchema(result_set, jdbc_dialect, False)  # noqa: WPS425
        result_iterator = jdbc_utils.resultSetToRows(result_set, result_schema)
        result_list = java_converters.seqAsJavaListConverter(result_iterator.toSeq()).asJava()
        jdf = self.spark._jsparkSession.createDataFrame(result_list, result_schema)

        return DataFrame(jdf, self.spark._wrapped)
