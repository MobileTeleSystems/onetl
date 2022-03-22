from __future__ import annotations

from dataclasses import dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import Field, validator

from onetl._internal import clear_statement  # noqa: WPS436
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin, StatementType
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = getLogger(__name__)


@dataclass(frozen=True)  # noqa: WPS338
class JDBCConnection(DBConnection, JDBCMixin):
    host: str
    user: str
    password: str = field(repr=False)
    # Database in rdbms, schema in DBReader.
    # See https://www.educba.com/postgresql-database-vs-schema/ for more details
    database: str | None = None
    port: int | None = None
    extra: dict = field(default_factory=dict)

    driver: ClassVar[str] = ""
    package: ClassVar[str] = ""

    class Options(DBConnection.Options, JDBCMixin.Options):  # noqa: WPS431
        batchsize: Optional[int] = None
        session_init_statement: Optional[str] = Field(alias="sessionInitStatement", default=None)
        truncate: Optional[bool] = None
        create_table_options: Optional[str] = Field(alias="createTableOptions", default=None)
        create_table_column_types: Optional[str] = Field(alias="createTableColumnTypes", default=None)
        custom_schema: Optional[str] = Field(alias="customSchema", default=None)
        cascade_truncate: Optional[bool] = Field(alias="cascadeTruncate", default=None)
        push_down_predicate: Optional[str] = Field(alias="pushDownPredicate", default=None)

        # Options in DataFrameWriter.jdbc() method
        partition_column: Optional[str] = Field(alias="partitionColumn", default=None)
        lower_bound: Optional[int] = Field(alias="lowerBound", default=None)
        upper_bound: Optional[int] = Field(alias="upperBound", default=None)
        num_partitions: Optional[int] = Field(alias="numPartitions", default=None)

        @validator("num_partitions", pre=True)
        def num_partitions_only_set_with_partition_column(cls, value, values):  # noqa: N805
            partition_column = values.get("partition_column")

            if value and not partition_column:
                raise ValueError("Option `num_partitions` could be set only with `partitionColumn`")

            return value

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def _query_on_executor(
        self,
        query: str,
        options: Options | dict | None = None,
    ) -> DataFrame:
        jdbc_params = self.jdbc_params_creator(options)
        jdbc_params.pop("mode", None)

        return self.spark.read.jdbc(table=f"({query}) T", **jdbc_params)

    def sql(
        self,
        query: str,
        options: Options | dict | None = None,
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

        options : dict, :obj:`onetl.connection.DBConnection.Options`, default: ``None``

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

        df = self._query_on_executor(query, options)

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

    def _query_on_driver(
        self,
        query: str,
        options: Options | dict | None = None,
        read_only: bool = True,
    ) -> DataFrame:
        return self._execute_on_driver(
            statement=query,
            statement_type=StatementType.PREPARED,
            callback=self._statement_to_dataframe,
            options=self.to_options(options),
            read_only=read_only,
        )

    def _query_or_none_on_driver(
        self,
        query: str,
        options: Options | dict | None = None,
        read_only: bool = True,
    ) -> DataFrame | None:
        return self._execute_on_driver(
            statement=query,
            statement_type=StatementType.PREPARED,
            callback=self._statement_to_optional_dataframe,
            options=self.to_options(options),
            read_only=read_only,
        )

    def _call_on_driver(
        self,
        query: str,
        options: Options | dict | None = None,
        read_only: bool = False,
    ) -> DataFrame | None:
        return self._execute_on_driver(
            statement=query,
            statement_type=StatementType.CALL,
            callback=self._statement_to_optional_dataframe,
            options=self.to_options(options),
            read_only=read_only,
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
        options: Options | dict | None = None,
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

            Resulting DataFrame is stored in a driver memory, do not use this method to return large datasets.

        Parameters
        ----------
        query : str

            SQL query to be executed.

            Can be in any form of SELECT supported by a database, like:

            * ``SELECT ... FROM ...``
            * ``WITH ... AS (...) SELECT ... FROM ...``
            * *Some* databases also support ``SHOW ...`` queries, like ``SHOW TABLES``

        options : dict, :obj:`onetl.connection.DBConnection.Options`, default: ``None``

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

        df = self._query_on_driver(query, options)

        log.info(f"|{self.__class__.__name__}| Query succeeded, resulting dataframe contains {df.count()} rows")
        return df

    def execute(
        self,
        statement: str,
        options: Options | dict | None = None,
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

            Resulting DataFrame is stored in a driver memory, do not use this method to return large datasets.

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

        options : dict, :obj:`onetl.connection.DBConnection.Options`, default: ``None``

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

        df = self._call_on_driver(statement, options)

        message = f"|{self.__class__.__name__}| Execution succeeded"
        if df is not None:
            rows_count = df.count()
            message += f", resulting dataframe contains {rows_count} rows"

        log.info(message)
        return df

    def check(self) -> None:
        self.log_parameters()

        log.info(f"|{self.__class__.__name__}| Checking connection availability...")
        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(self._check_query)

        try:
            self._query_or_none_on_driver(self._check_query)
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg) from e

    def read_table(  # type: ignore[override]
        self,
        table: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: Options | dict | None = None,
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
            options=self.to_options(options).copy(exclude={"mode"}),
        )

        return self.sql(query, read_options)

    def save_df(  # type: ignore[override]
        self,
        df: DataFrame,
        table: str,
        options: Options | dict | None = None,
    ) -> None:
        # for convenience. parameters accepted by spark.write.jdbc method
        #   spark.write.jdbc(
        #     url, table, mode,
        #     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        write_options = self.jdbc_params_creator(options)
        df.write.jdbc(table=table, **write_options)
        log.info(f"|{self.__class__.__name__}| Table {table} successfully written")

    def get_schema(  # type: ignore[override]
        self,
        table: str,
        columns: list[str] | None = None,
        options: Options | dict | None = None,
    ) -> StructType:

        log.info(f"|{self.__class__.__name__}| Fetching schema of {table}")

        query = self.get_sql_query(table, columns=columns, where="1=0")
        read_options = self._exclude_partition_options(options, fetchsize=0)

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(query)

        df = self._query_on_driver(query, read_options)
        log.info(f"|{self.__class__.__name__}| Schema fetched")

        return df.schema

    def jdbc_params_creator(
        self,
        options: Options | dict | None = None,
    ) -> dict:
        result_options = self.to_options(options)

        # Have to replace the <partitionColumn> parameter with <column>
        # since the method takes the named <column> parameter
        # link to source below
        # https://git.io/JKOku
        if result_options.partition_column:  # noqa: WPS609
            result_options = result_options.copy(
                update={"column": result_options.partition_column},
                exclude={"partition_column"},
            )

        # for convenience. parameters accepted by spark.read.jdbc method
        #  spark.read.jdbc(
        #    url, table, column, lowerBound, upperBound, numPartitions, predicates
        #    properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        top_level_options = {"url", "column", "lower_bound", "upper_bound", "num_partitions", "mode"}

        result = self._get_jdbc_properties(
            result_options,
            include=top_level_options,
            exclude_none=True,
        )

        result["properties"] = self._get_jdbc_properties(
            result_options,
            exclude=top_level_options,
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
        options: Options | dict | None = None,
    ) -> tuple[Any, Any]:

        log.info(f"|Spark| Getting min and max values for column '{column}'")

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
        log_with_indent(f"MIN({column}) = {min_value}")
        log_with_indent(f"MAX({column}) = {max_value}")

        return min_value, max_value

    def _exclude_partition_options(self, options: Options | dict | None, fetchsize: int) -> Options:
        return self.to_options(options).copy(
            update={"fetchsize": fetchsize},
            exclude={"partition_column", "lower_bound", "upper_bound", "num_partitions"},
        )

    def _set_lower_upper_bound(  # type: ignore[override]
        self,
        table: str,
        hint: str | None = None,
        where: str | None = None,
        options: Options | dict | None = None,
    ) -> Options:
        """
        Determine values of upperBound and lowerBound options
        """

        result_options = self.to_options(options)

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
            f"|Spark| numPartitions value is set to {result_options.num_partitions}, "
            f"but {' and '.join(missing_values)} value is not set. "
            f"It will be detected automatically based on values in partitionColumn {result_options.partition_column}",
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

    @classmethod
    def _log_fields(cls) -> set[str]:
        fields = super()._log_fields()
        fields.add("jdbc_url")
        return fields

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        fields = super()._log_exclude_fields()
        return fields.union({"password", "package"})
