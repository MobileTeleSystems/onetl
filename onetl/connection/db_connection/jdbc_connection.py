from __future__ import annotations

from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING, Any, Optional

from pydantic import root_validator

from onetl._internal import clear_statement, to_camel  # noqa: WPS436
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin, StatementType
from onetl.impl.generic_options import GenericOptions
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = getLogger(__name__)

# options from spark.read.jdbc which are populated by JDBCConnection methods
GENERIC_PROHIBITED_OPTIONS = frozenset(
    (
        "table",
        "dbtable",
        "query",
        "properties",
    ),
)

READ_WRITE_OPTIONS = frozenset(
    (
        "keytab",
        "principal",
        "refreshKrb5Config",
        "connectionProvider",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "column",  # in some part of Spark source code option 'partitionColumn' is called just 'column'
        "mode",
        "batchsize",
        "isolationLevel",
        "isolation_level",
        "truncate",
        "cascadeTruncate",
        "createTableOptions",
        "createTableColumnTypes",
        "createTableColumnTypes",
    ),
)

READ_OPTIONS = frozenset(
    (
        "column",  # in some part of Spark source code option 'partitionColumn' is called just 'column'
        "partitionColumn",
        "partition_column",
        "lowerBound",
        "lower_bound",
        "upperBound",
        "upper_bound",
        "numPartitions",
        "num_partitions",
        "fetchsize",
        "sessionInitStatement",
        "session_init_statement",
        "customSchema",
        "pushDownPredicate",
        "pushDownAggregate",
        "pushDownLimit",
        "pushDownTableSample",
        "predicates",
    ),
)


# parameters accepted by spark.read.jdbc method:
#  spark.read.jdbc(
#    url, table, column, lowerBound, upperBound, numPartitions, predicates
#    properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })
READ_TOP_LEVEL_OPTIONS = frozenset(("url", "column", "lower_bound", "upper_bound", "num_partitions", "predicates"))

# parameters accepted by spark.write.jdbc method:
#   spark.write.jdbc(
#     url, table, mode,
#     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })
WRITE_TOP_LEVEL_OPTIONS = frozenset(("url", "mode"))


class JDBCWriteMode(str, Enum):  # noqa: WPS600
    APPEND = "append"
    OVERWRITE = "overwrite"

    def __str__(self) -> str:
        return str(self.value)


class JDBCConnection(DBConnection, JDBCMixin):  # noqa: WPS338
    class Extra(GenericOptions):
        class Config:
            extra = "allow"

    class ReadOptions(JDBCMixin.JDBCOptions):  # noqa: WPS437
        """Class for Spark reading options, related to a specific JDBC source.

        .. note ::

            You can pass any value
            `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_,
            even if it is not mentioned in this documentation. **Its name should be in** ``camelCase``!

            The set of supported options depends on Spark version.

        Examples
        --------

        Read options initialization

        .. code:: python

            options = JDBC.ReadOptions(
                partitionColumn="reg_id",
                numPartitions=10,
                lowerBound=0,
                upperBound=1000,
                someNewOption="value",
            )
        """

        class Config:
            known_options = READ_OPTIONS | READ_WRITE_OPTIONS
            prohibited_options = (
                JDBCMixin.JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS | WRITE_OPTIONS
            )
            alias_generator = to_camel

        # Options in DataFrameWriter.jdbc() method
        partition_column: Optional[str] = None
        """Options ``partitionColumn``, ``numPartitions``, ``lowerBound``, ``upperBound``
        describe how to partition the table when reading in parallel from multiple workers.

        ``partitionColumn`` must be a numeric, date, or timestamp column in the table.

        Spark generates for each executor an SQL query like:

        Executor 1:

        .. code:: sql

            SELECT ... FROM table
            WHERE (partitionColumn >= lowerBound
                    OR partitionColumn IS NULL)
            AND partitionColumn < lowerBound + stride

        Executor 2:

        .. code:: sql

            SELECT ... FROM table
            WHERE partitionColumn >= lowerBound + stride
            AND partitionColumn < lowerBound + 2*stride

        Executor N:

        .. code:: sql

            SELECT ... FROM table
            WHERE partitionColumn >= lowerBound + (N-1) * stride
            AND partitionColumn <= upperBound

        Where ``stride=MAX(partitionColumn) - MIN(partitionColumn)) / numPartitions``.

        .. note::

            ``lowerBound`` and ``upperBound`` are used just to calculate the partition stride,
            NOT for filtering the rows in table. So all rows in the table will be returned.

        .. warning::

            You can pass ``numPartitions``, ``lowerBound`` and ``upperBound`` only with
            ``partitionColumn``.
        """

        num_partitions: Optional[int] = None
        """See documentation for :obj:`~partition_column`"""  # noqa: WPS322

        lower_bound: Optional[int] = None
        """See documentation for :obj:`~partition_column`"""  # noqa: WPS322

        upper_bound: Optional[int] = None
        """See documentation for :obj:`~partition_column`"""  # noqa: WPS322

        session_init_statement: Optional[str] = None
        '''After each database session is opened to the remote DB and before starting to read data,
        this option executes a custom SQL statement (or a PL/SQL block).

        Use this to implement session initialization code.

        Example:

        .. code:: python

            sessionInitStatement = """
                BEGIN
                    execute immediate
                    'alter session set "_serial_direct_read"=true';
                END;
            """
        '''

        @root_validator
        def num_partitions_only_set_with_partition_column(cls, values):  # noqa: N805
            num_partitions = values.get("num_partitions")
            partition_column = values.get("partition_column")
            lower_bound = values.get("lower_bound")
            upper_bound = values.get("upper_bound")

            if num_partitions and partition_column:
                return values

            if num_partitions is None and not partition_column:
                if lower_bound is None and upper_bound is None:
                    return values

                raise ValueError(
                    "Options `lowerBound` and `upperBound` can be used only "
                    "with `numPartitions` and `partitionColumn` options set",
                )

            raise ValueError(
                "Options `numPartitions` and `partitionColumn` should be either both set, or both unset",
            )

        fetchsize: int = 100_000
        """How many rows to fetch per round trip.

        Tuning this option can influence performance of writing.

        .. warning::

            Default value is different from Spark.

            Spark uses driver's own value, and it may be different in different drivers,
            and even versions of the same driver. For example, Oracle has
            default ``fetchsize=10``, which is absolutely not usable.

            Thus we've overridden default value with ``100_000``, which should increase reading performance.
        """

    class WriteOptions(JDBCMixin.JDBCOptions):  # noqa: WPS437
        """Class for Spark writing options, related to a specific JDBC source.

        .. note ::

            You can pass any value
            `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_,
            even if it is not mentioned in this documentation. **Its name should be in** ``camelCase``!

            The set of supported options depends on Spark version.

        Examples
        --------

        Write options initialization

        .. code:: python

            options = JDBC.WriteOptions(mode="append", batchsize=20_000, someNewOption="value")
        """

        class Config:
            known_options = WRITE_OPTIONS | READ_WRITE_OPTIONS
            prohibited_options = (
                JDBCMixin.JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS | READ_OPTIONS
            )
            alias_generator = to_camel

        mode: JDBCWriteMode = JDBCWriteMode.APPEND
        """Mode of writing data into target table.

        Possible values:
            * ``append`` (default)
                Appends data into existing table.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class
                    (``createTableOptions``, ``createTableColumnTypes``, etc).

                * Table exists
                    Data is appended to a table. Table has the same DDL as before writing data

            * ``overwrite``
                Overwrites data in the entire table (**table is dropped and then created, or truncated**).

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class
                    (``createTableOptions``, ``createTableColumnTypes``, etc).

                * Table exists
                    Table content is replaced with dataframe content.

                    After writing completed, target table could either have the same DDL as
                    before writing data (``truncate=True``), or can be recreated (``truncate=False``
                    or source does not support truncation).

        .. note::

            ``error`` and ``ignore`` modes are not supported.
        """

        batchsize: int = 20_000
        """How many rows can be inserted per round trip.

        Tuning this option can influence performance of writing.

        .. warning::

            Default value is different from Spark.

            Spark uses quite small value ``1000``, which is absolutely not usable
            in BigData world.

            Thus we've overridden default value with ``20_000``,
            which should increase writing performance.

            You can increase it even more, up to ``50_000``,
            but it depends on your database load and number of columns in the row.
            Higher values does not increase performance.
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
        """

    class Options(ReadOptions, WriteOptions):
        def __init__(self, *args, **kwargs):
            log.warning(
                "`SomeDB.Options` class is deprecated since v0.5.0 and will be removed in v1.0.0. "
                "Please use `SomeDB.ReadOptions` or `SomeDB.WriteOptions` classes instead",
            )
            super().__init__(*args, **kwargs)

        class Config:
            prohibited_options = JDBCMixin.JDBCOptions.Config.prohibited_options

    host: str
    port: int
    extra: Extra = Extra()

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def _query_on_executor(
        self,
        query: str,
        options: ReadOptions,
    ) -> DataFrame:
        jdbc_params = self.options_to_jdbc_params(options)
        jdbc_params.pop("mode", None)

        return self.spark.read.jdbc(table=f"({query}) T", **jdbc_params)

    def sql(
        self,
        query: str,
        options: ReadOptions | dict | None = None,
    ) -> DataFrame:
        """
        **Lazily** execute SELECT statement **on Spark executor** and return DataFrame.

        Same as ``spark.read.jdbc(query)``.

        .. note::

            Statement is executed in read-write connection,
            so if you're calling some functions/procedures with DDL/DML statements inside,
            they can change data in your database.

            Unfortunately, Spark does no provide any option to change this behavior.

        Parameters
        ----------
        query : str

            SQL query to be executed.

            Only ``SELECT ... FROM ...`` form is supported.

            Some databases also supports ``WITH ... AS (...) SELECT ... FROM ...`` form,

            Queries like ``SHOW ...`` are not supported.

        options : dict, :obj:`~ReadOptions`, default: ``None``

            JDBC options to be used while fetching data, like ``fetchsize`` or ``queryTimeout``

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame

            Spark dataframe

        Examples
        --------

        Read data from a table:

        .. code:: python

            df = connection.sql("SELECT * FROM mytable")

        Read data from a table with options:

        .. code:: python

            # reads data from table in batches, 10000 rows per batch
            df = connection.sql("SELECT * FROM mytable", {"fetchsize": 10000})
            assert df.count()

        """

        query = clear_statement(query)

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on executor):")
        log_with_indent(query)

        df = self._query_on_executor(query, self.ReadOptions.parse(options))

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

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

    def _query_or_none_on_driver(
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

    def close(self):
        """
        Close all connections, opened by ``.fetch()`` or ``.execute()`` methods.

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

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def fetch(
        self,
        query: str,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> DataFrame:
        """
        **Immediately** execute SELECT statement **on Spark driver** and return in-memory DataFrame.

        Works almost the same like ``connection.sql(query)``, but directly calls JDBC driver.

        .. note::

            Unlike ``connection.sql(query)``  method, statement is executed in read-only connection,
            so it cannot change any data in the database.

        .. note::

            First call of the method opens the connection to a database.
            Call ``.close()`` method to close it, or use context manager to do it automatically.

        .. warning::

            Resulting DataFrame is stored in a driver memory, **DO NOT** use this method to return large datasets.

        Parameters
        ----------
        query : str

            SQL query to be executed.

            Can be in any form of SELECT supported by a database, like:

            * ``SELECT ... FROM ...``
            * ``WITH ... AS (...) SELECT ... FROM ...``
            * *Some* databases also support ``SHOW ...`` queries, like ``SHOW TABLES``

        options : dict, :obj:`~JDBCOptions`, default: ``None``

            JDBC options to be used while fetching data, like ``fetchsize`` or ``queryTimeout``

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

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(query)

        df = self._query_on_driver(query, self.JDBCOptions.parse(options))

        log.info(f"|{self.__class__.__name__}| Query succeeded, resulting dataframe contains {df.count()} rows")
        return df

    def execute(
        self,
        statement: str,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> DataFrame | None:
        """
        **Immediately** execute DDL, DML or procedure/function **on Spark driver**.

        Returns DataFrame only if input is DML statement with ``RETURNING ...`` clause, or a procedure/function call.
        In other cases returns ``None``.

        There is no method like this in :obj:`pyspark.sql.SparkSession` object,
        but Spark internal methods works almost the same (but on executor side).

        .. note::

            First call of the method opens the connection to a database.
            Call ``.close()`` method to close it, or use context manager to do it automatically.

        .. warning::

            Resulting DataFrame is stored in a driver memory, **DO NOT** use this method to return large datasets.

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

            * ``BEGIN ... END```
            * ``EXEC myprocedure``
            * ``EXECUTE myprocedure(arg1)``
            * ``CALL myfunction(arg1, arg2)``
            * ``{call myprocedure(arg1, ?)}`` (``?`` is output parameter)
            * ``{?= call myfunction(arg1, arg2)}``

            The exact syntax depends on the database is being used.

            .. warning::

                This method is not designed to call statements like ``INSERT INTO ... VALUES ...``,
                which accepts some input data.

                Use ``run(dataframe)`` method of :obj:`onetl.core.db_writer.db_writer.DBWriter`,
                or ``connection.save_df(dataframe, table, options)`` instead.

        options : dict, :obj:`~JDBCOptions`, default: ``None``

            JDBC options to be used while executing the statement,
            like ``queryTimeout`` or ``isolationLevel``

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

        log.info(f"|{self.__class__.__name__}| Executing statement (on driver):")
        log_with_indent(statement)

        call_options = self.JDBCOptions.parse(options)
        df = self._call_on_driver(statement, call_options)

        message = f"|{self.__class__.__name__}| Execution succeeded"
        if df is not None:
            rows_count = df.count()
            message += f", resulting dataframe contains {rows_count} rows"

        log.info(message)
        return df

    def check(self):
        log.info(f"|{self.__class__.__name__}| Checking connection availability...")
        self._log_parameters()

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(self._check_query)

        try:
            self._query_or_none_on_driver(self._check_query, self.ReadOptions(fetchsize=1))
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            log.exception(f"|{self.__class__.__name__}| Connection is unavailable")
            raise RuntimeError("Connection is unavailable") from e

        return self

    def read_table(  # type: ignore[override]
        self,
        table: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: ReadOptions | dict | None = None,
    ) -> DataFrame:
        query = self.get_sql_query(
            table=table,
            columns=columns,
            where=where,
            hint=hint,
        )

        read_options = self._set_lower_upper_bound(
            table=table,
            where=where,
            hint=hint,
            options=self.ReadOptions.parse(options).copy(exclude={"mode"}),
        )

        return self.sql(query, read_options)

    def save_df(  # type: ignore[override]
        self,
        df: DataFrame,
        table: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.options_to_jdbc_params(self.WriteOptions.parse(options))
        df.write.jdbc(table=table, **write_options)
        log.info(f"|{self.__class__.__name__}| Table {table!r} successfully written")

    def get_schema(  # type: ignore[override]
        self,
        table: str,
        columns: list[str] | None = None,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> StructType:

        log.info(f"|{self.__class__.__name__}| Fetching schema of table {table!r}")

        query = self.get_sql_query(table, columns=columns, where="1=0")
        read_options = self._exclude_partition_options(options, fetchsize=0)

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(query)

        df = self._query_on_driver(query, read_options)
        log.info(f"|{self.__class__.__name__}| Schema fetched")

        return df.schema

    def options_to_jdbc_params(
        self,
        options: ReadOptions | WriteOptions,
    ) -> dict:
        # Have to replace the <partitionColumn> parameter with <column>
        # since the method takes the named <column> parameter
        # link to source below
        # https://github.com/apache/spark/blob/2ef8ced27a6b0170a691722a855d3886e079f037/python/pyspark/sql/readwriter.py#L465

        partition_column = getattr(options, "partition_column", None)
        if partition_column:
            options = options.copy(
                update={"column": partition_column},
                exclude={"partition_column"},
            )

        result = self._get_jdbc_properties(
            options,
            include=READ_TOP_LEVEL_OPTIONS | WRITE_TOP_LEVEL_OPTIONS,
            exclude_none=True,
        )

        result["properties"] = self._get_jdbc_properties(
            options,
            exclude=READ_TOP_LEVEL_OPTIONS | WRITE_TOP_LEVEL_OPTIONS,
            exclude_none=True,
        )

        return result

    def get_min_max_bounds(  # type: ignore[override]
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> tuple[Any, Any]:

        log.info(f"|Spark| Getting min and max values for column {column!r}")

        read_options = self._exclude_partition_options(options, fetchsize=1)

        query = self.get_sql_query(
            table=table,
            columns=[
                self.expression_with_alias(self._get_min_value_sql(expression or column), f"min_{column}"),
                self.expression_with_alias(self._get_max_value_sql(expression or column), f"max_{column}"),
            ],
            where=where,
            hint=hint,
        )

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(query)

        df = self._query_on_driver(query, read_options)

        min_value, max_value = df.collect()[0]
        log.info("|Spark| Received values:")
        log_with_indent(f"MIN({column}) = {min_value!r}")
        log_with_indent(f"MAX({column}) = {max_value!r}")

        return min_value, max_value

    def _exclude_partition_options(
        self,
        options: JDBCMixin.JDBCOptions | dict | None,
        fetchsize: int,
    ) -> JDBCMixin.JDBCOptions:
        return self.JDBCOptions.parse(options).copy(
            update={"fetchsize": fetchsize},
            exclude={"partition_column", "lower_bound", "upper_bound", "num_partitions"},
        )

    def _set_lower_upper_bound(  # type: ignore[override]
        self,
        table: str,
        hint: str | None = None,
        where: str | None = None,
        options: ReadOptions | dict | None = None,
    ) -> ReadOptions:
        """
        Determine values of upperBound and lowerBound options
        """

        result_options = self.ReadOptions.parse(options)

        if not result_options.partition_column:
            return result_options

        missing_values: list[str] = []

        if not result_options.lower_bound:
            missing_values.append("lowerBound")

        if not result_options.upper_bound:
            missing_values.append("upperBound")

        if not missing_values:
            return result_options

        log.warning(
            f"|Spark| Passed numPartitions = {result_options.num_partitions!r}, "
            f"but {' and '.join(missing_values)} value is not set. "
            "It will be detected automatically based on values "
            f"in partitionColumn {result_options.partition_column!r}",
        )

        min_partition_value, max_partition_value = self.get_min_max_bounds(
            table=table,
            column=result_options.partition_column,
            where=where,
            hint=hint,
            options=result_options,
        )

        # The sessionInitStatement parameter is removed because it only needs to be applied once.
        return result_options.copy(
            exclude={"session_init_statement"},
            update={
                "lower_bound": result_options.lower_bound or min_partition_value,
                "upper_bound": result_options.upper_bound or max_partition_value,
            },
        )

    def _log_parameters(self):
        super()._log_parameters()
        log_with_indent(f"jdbc_url = {self.jdbc_url!r}")
