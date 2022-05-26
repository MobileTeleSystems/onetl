from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Tuple, Union

from pydantic import Field, validator

from onetl._internal import clear_statement  # noqa: WPS436
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.impl import DBWriteMode
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = getLogger(__name__)


@dataclass(frozen=True)
class Hive(DBConnection):
    """Class for Hive spark connection.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Spark session that required for connection to hive.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Hive connection initialization

    .. code::

        from onetl.connection import Hive
        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        hive = Hive(spark=spark)
    """

    # TODO(@dypedchenk): add documentation for values
    class Options(DBConnection.Options):  # noqa: WPS431
        partition_by: Optional[Union[List[str], str]] = Field(alias="partitionBy")
        bucket_by: Optional[Tuple[int, Union[List[str], str]]] = Field(alias="bucketBy")  # noqa: WPS234
        sort_by: Optional[Union[List[str], str]] = Field(alias="sortBy")
        compression: Optional[str] = None
        insert_into: bool = Field(alias="insertInto", default=False)
        format: str = "orc"

        @validator("sort_by")
        def sort_by_cannot_be_used_without_bucket_by(cls, sort_by, values):  # noqa: N805
            options = values.copy()
            bucket_by = options.pop("bucket_by", None)
            if sort_by and not bucket_by:
                raise ValueError("`sort_by` option can only be used with non-empty `bucket_by`")

            return sort_by

        @validator("insert_into")
        def insert_into_cannot_be_used_with_other_options(cls, insert_into, values):  # noqa: N805
            options = {key: value for key, value in values.items() if value is not None}
            mode = options.pop("mode", None)

            if insert_into:
                if mode:
                    mode = DBWriteMode(mode)
                    if mode not in {DBWriteMode.APPEND, DBWriteMode.OVERWRITE}:
                        raise ValueError(  # noqa: WPS220
                            f"Write mode {mode} cannot be used with `insert_into=True` option",
                        )

                if options:
                    raise ValueError(f"Options like {options.keys()} cannot be used with `insert_into=True`")

            return insert_into

    # TODO (@msmarty5): Replace with active_namenode function from mtspark
    @property
    def instance_url(self) -> str:
        return "rnd-dwh"

    def sql(  # type: ignore[override]
        self,
        query: str,
        options: Options | dict | None = None,
    ) -> DataFrame:
        """
        Lazily execute SELECT statement and return DataFrame.

        Same as ``spark.sql(query)``.

        Parameters
        ----------
        query : str

            SQL query to be executed, like:

            * ``SELECT ... FROM ...``
            * ``WITH ... AS (...) SELECT ... FROM ...``
            * ``SHOW ...`` queries are also supported, like ``SHOW TABLES``

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame

            Spark dataframe

        Examples
        --------

        Read data from Hive table:

        .. code:: python

            connection = Hive(spark=spark)

            df = connection.sql("SELECT * FROM mytable")
        """

        query = clear_statement(query)
        self._handle_read_options(options)

        log.info(f"|{self.__class__.__name__}| Executing SQL query:")
        log_with_indent(query)
        df = self._execute_sql(query)
        log.info("|Spark| DataFrame successfully created from SQL statement")
        return df

    def execute(  # type: ignore[override]
        self,
        statement: str,
        options: Options | dict | None = None,
    ) -> None:
        """
        Execute DDL or DML statement.

        Parameters
        ----------
        statement : str

            Statement to be executed, like:

            DML statements:

            * ``INSERT INTO target_table SELECT * FROM source_table``
            * ``TRUNCATE TABLE mytable``

            DDL statements:

            * ``CREATE TABLE mytable (...)``
            * ``ALTER TABLE mytable ...``
            * ``DROP TABLE mytable``
            * ``MSCK REPAIR TABLE mytable``

            The exact list of supported statements depends on Hive version,
            for example some new versions support ``CREATE FUNCTION`` syntax.

        Examples
        --------

        Create table:

        .. code:: python

            connection = Hive(spark=spark)

            connection.execute(
                "CREATE TABLE mytable (id NUMBER, data VARCHAR) PARTITIONED BY (date DATE)"
            )

        Drop table partition:

        .. code:: python

            connection = Hive(spark=spark)

            connection.execute("ALTER TABLE mytable DROP PARTITION(date='2022-02-01')")
        """

        statement = clear_statement(statement)
        self._handle_read_options(options)

        log.info(f"|{self.__class__.__name__}| Executing statement:")
        log_with_indent(statement)
        self._execute_sql(statement).collect()
        log.info(f"|{self.__class__.__name__}| Call succeeded")

    def check(self) -> None:
        self.log_parameters()

        log.info(f"|{self.__class__.__name__}| Checking connection availability...")

        try:
            self.sql(self._check_query)
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg) from e

    def save_df(  # type: ignore[override]
        self,
        df: DataFrame,
        table: str,
        options: Options | dict | None = None,
    ) -> None:
        write_options = self.to_options(options)

        if write_options.insert_into:
            columns = self._sort_df_columns_like_table(table, df.columns)
            writer = df.select(*columns).write

            mode = DBWriteMode(write_options.mode)
            writer.insertInto(table, overwrite=mode == DBWriteMode.OVERWRITE)  # overwrite is boolean
        else:
            writer = df.write
            for method, value in write_options.dict(by_alias=True, exclude_none=True, exclude={"insert_into"}).items():
                # <value> is the arguments that will be passed to the <method>
                # format orc, parquet methods and format simultaneously
                if hasattr(writer, method):
                    if isinstance(value, Iterable) and not isinstance(value, str):
                        writer = getattr(writer, method)(*value)  # noqa: WPS220
                    else:
                        writer = getattr(writer, method)(value)  # noqa: WPS220
                else:
                    writer = writer.option(method, value)

            writer.saveAsTable(table)

        log.info(f"|{self.__class__.__name__}| Table {table} successfully written")

    def read_table(  # type: ignore[override]
        self,
        table: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: Options | dict | None = None,
    ) -> DataFrame:
        sql_text = self.get_sql_query(
            table=table,
            columns=columns,
            where=where,
            hint=hint,
        )

        return self.sql(sql_text, options)

    def get_schema(  # type: ignore[override]
        self,
        table: str,
        columns: list[str] | None = None,
        options: Options | dict | None = None,
    ) -> StructType:
        self._handle_read_options(options)

        query_schema = self.get_sql_query(table, columns=columns, where="1=0")

        log.info(f"|{self.__class__.__name__}| Fetching schema of {table}")
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log_with_indent(query_schema)

        df = self._execute_sql(query_schema)
        return df.schema

    def get_min_max_bounds(  # type: ignore[override]
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: Options | dict | None = None,
    ) -> Tuple[Any, Any]:

        self._handle_read_options(options)
        log.info(f"|Spark| Getting min and max values for column '{column}'")

        sql_text = self.get_sql_query(
            table=table,
            columns=[
                self.expression_with_alias(self._get_min_value_sql(expression or column), f"min_{column}"),
                self.expression_with_alias(self._get_max_value_sql(expression or column), f"max_{column}"),
            ],
            where=where,
            hint=hint,
        )

        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log_with_indent(sql_text)

        df = self._execute_sql(sql_text)
        row = df.collect()[0]
        min_value, max_value = row[f"min_{column}"], row[f"max_{column}"]

        log.info("|Spark| Received values:")
        log_with_indent(f"MIN({column}) = {min_value}")
        log_with_indent(f"MAX({column}) = {max_value}")

        return min_value, max_value

    def _execute_sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)

    def _handle_read_options(self, options: Options | dict | None):
        if self.to_options(options).dict(exclude_unset=True):
            raise ValueError(
                f"{options.__class__.__name__} cannot be passed to {self.__class__.__name__}. "
                "Hive reader does not support options.",
            )

    def _sort_df_columns_like_table(self, table: str, df_columns: list[str]) -> list[str]:
        # Hive is inserting columns by the order, not by their name
        # so if you're inserting dataframe with columns B, A, C to table with columns A, B, C, data will be damaged
        # so it is important to sort columns in dataframe to match columns in the table.

        table_columns = self.spark.table(table).columns

        # But names could have different cases, this should not cause errors
        table_columns_lower = [column.lower() for column in table_columns]

        table_columns_set = set(table_columns_lower)
        df_columns_lower = {column.lower() for column in df_columns}

        missing_columns_df = df_columns_lower - table_columns_set
        missing_columns_table = table_columns_set - df_columns_lower

        if missing_columns_df or missing_columns_table:
            missing_columns_df_message = ""
            if missing_columns_df:
                missing_columns_df_message = f"""
                    These columns present only in dataframe:
                    {', '.join(missing_columns_df)}
                    """

            missing_columns_table_message = ""
            if missing_columns_table:
                missing_columns_table_message = f"""
                    These columns present only in table:
                    {', '.join(missing_columns_table)}
                    """

            raise ValueError(
                dedent(
                    f"""
                    Table "{table}" has columns:
                    {', '.join(table_columns)}

                    Dataframe has columns:
                    {', '.join(df_columns)}
                    {missing_columns_df_message}{missing_columns_table_message}
                    """,
                ),
            )

        return sorted(df_columns, key=lambda column: table_columns_lower.index(column.lower()))
