from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Tuple, Union

from pydantic import root_validator, validator

from onetl._internal import clear_statement  # noqa: WPS436
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

PARTITION_OVERWRITE_MODE_PARAM = "spark.sql.sources.partitionOverwriteMode"
log = getLogger(__name__)


class HiveWriteMode(str, Enum):  # noqa: WPS600
    APPEND = "append"
    OVERWRITE_TABLE = "overwrite_table"
    OVERWRITE_PARTITIONS = "overwrite_partitions"

    def __str__(self):
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):
        if str(value) == "overwrite":
            log.warning(
                "Mode `overwrite` is deprecated since 0.4.0 and will be removed in 0.5.0, "
                "use `overwrite_partitions` instead",
            )
            return cls.OVERWRITE_PARTITIONS


@dataclass(frozen=True)
class Hive(DBConnection):
    """Class for Hive spark connection.

    Parameters
    ----------
    spark : :obj:`pyspark.sql.SparkSession`
        Spark session that required for connection to hive.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Hive connection initialization

    .. code:: python

        from onetl.connection import Hive
        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        hive = Hive(spark=spark)
    """

    class Options(DBConnection.Options):  # noqa: WPS431
        """Class for writing options, related to Hive source.

        You can pass here key-value items which then will be converted to calls
        of :obj:`pyspark.sql.readwriter.DataFrameWriter` methods.

        For example, ``Hive.Options(mode="append", partitionBy="reg_id")`` will
        be converted to ``df.write.mode("append").partitionBy("reg_id")`` call, and so on.

        .. note::

            You can pass any method and its value
            `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html>`_,
            even if it is not mentioned in this documentation. **Its name should be in** ``camelCase``!

            The set of supported options depends on Spark version used.

        Examples
        --------

        Writing options initialization

        .. code:: python

            options = Hive.Options(mode="append", partitionBy="reg_id", someNewOption="value")
        """

        mode: HiveWriteMode = HiveWriteMode.APPEND
        """Mode of writing data into target table.

        Possible values:
            * ``append`` (default)
                Appends data into existing partition/table, or create partition/table if it does not exist.

                Almost like Spark's ``insertInto(overwrite=False)``.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class (``format``, ``compression``, etc).

                * Table exists, but not partitioned
                    Data is appended to a table. Table is still not partitioned (DDL is unchanged)

                * Table exists and partitioned, but partition is present only in dataframe
                    Partition is created based on table's ``PARTITIONED BY (...)`` options

                * Table exists and partitioned, partition is present in both dataframe and table
                    Data is appended to existing partition

                * Table exists and partitioned, but partition is present only in table
                    Existing partition is left intact

            * ``overwrite_partitions``
                Overwrites data in the existing partition, or create partition/table if it does not exist.

                Almost like Spark's ``insertInto(overwrite=True)`` +
                ``spark.sql.sources.partitionOverwriteMode=dynamic``.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class (``format``, ``compression``, etc).

                * Table exists, but not partitioned
                    Data is **overwritten in all the table**. Table is still not partitioned (DDL is unchanged)

                * Table exists and partitioned, but partition is present only in dataframe
                    Partition is created based on table's ``PARTITIONED BY (...)`` options

                * Table exists and partitioned, partition is present in both dataframe and table
                    Data is **overwritten in existing partition**

                * Table exists and partitioned, but partition is present only in table
                    Existing partition is left intact

            * ``overwrite_table``
                **Recreates table** (via ``DROP + CREATE``), **overwriting all existing data**.
                **All existing partitions are dropped.**

                Same as Spark's ``saveAsTable(mode=overwrite)`` +
                ``spark.sql.sources.partitionOverwriteMode=static`` (NOT ``insertInto``)!

                .. warning::

                    Table is recreated using other options from current class (``format``, ``compression``, etc)
                    **instead of using original table options**. Be careful

        .. note::

            ``error`` and ``ignore`` modes are not supported.

        .. note::

            Unlike Spark, config option ``spark.sql.sources.partitionOverwriteMode``
            does not affect behavior of any ``mode``

        .. warning::

            Used **only** while **writing** data to a table
        """

        format: str = "orc"
        """Format of files which should be used for storing table data.

        Examples: ``orc`` (default), ``parquet``, ``csv`` (NOT recommended)

        .. note::

            It's better to use column-based formats like ``orc`` or ``parquet``,
            not row-based (``csv``, ``json``)

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        partition_by: Optional[Union[List[str], str]] = None
        """
        List of columns should be used for data partitioning. ``None`` means partitioning is disabled.

        Each partition is a folder in HDFS which contains only files with the specific column value,
        like ``myschema.db/mytable/col1=value1``, ``myschema.db/mytable/col1=value2``, and so on.

        Multiple partitions columns means nested folder structure, like ``col1=val1/col2/val``.

        If ``WHERE`` clause in the query contains expression like ``partition = value``,
        Hive automatically filters up only specific partition.

        Examples: ``reg_id`` or ``["reg_id", "business_dt"]``

        .. note::

            Values should be scalars (integers, strings),
            and either static (``countryId``) or incrementing (dates, years), with low
            number of distinct values.

            Columns like ``userId`` or ``datetime``/``timestamp`` cannot be used for partitioning.

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        bucket_by: Optional[Tuple[int, Union[List[str], str]]] = None  # noqa: WPS234
        """Number of buckets plus bucketing columns. ``None`` means bucketing is disabled.

        Each bucket is created as a set of files with name containing ``hash(columns) mod num_buckets``,
        and used to remove shuffle from queries containing ``GROUP BY`` or ``JOIN`` over bucketing column,
        and to reduce number of files read by query containing ``=`` and ``IN`` predicates
        on bucketing column.

        Examples: ``(10, "user_id")``, ``(10, ["user_id", "user_phone"])``

        .. note::

            Bucketing should be used on columns containing values which have a lot of unique values,
            like ``userId``.

            Columns like ``countryId`` or ``date`` cannot be used for bucketing
            because of too low number of unique values.

        .. warning::

            It is recommended to use this option **ONLY** if you have a large table
            (hundreds of Gb or more), which is used mostly for JOINs with other tables,
            and you're inserting data using ``mode=overwrite_partitions``.

            Otherwise Spark will create a lot of small files
            (one file for each bucket and each executor), drastically decreasing HDFS performance.

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        sort_by: Optional[Union[List[str], str]] = None
        """Each file in a bucket will be sorted by these columns value. ``None`` means sorting is disabled.

        Examples: ``user_id`` or ``["user_id", "user_phone"]``

        .. note::

            Sorting columns should contain values which are used in ``ORDER BY`` clauses.

        .. warning::

            Could be used only with :obj:`bucket_by` option

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        compression: Optional[str] = None
        """Compressing algorithm which should be used for compressing created files in HDFS.
        ``None`` means compression is disabled.

        Examples: ``snappy``, ``zlib``

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        @validator("sort_by")
        def sort_by_cannot_be_used_without_bucket_by(cls, sort_by, values):  # noqa: N805
            options = values.copy()
            bucket_by = options.pop("bucket_by", None)
            if sort_by and not bucket_by:
                raise ValueError("`sort_by` option can only be used with non-empty `bucket_by`")

            return sort_by

        @root_validator
        def partition_overwrite_mode_is_not_allowed(cls, values):  # noqa: N805
            if values.get("partitionOverwriteMode") or values.get("partition_overwrite_mode"):
                raise ValueError(
                    "`partitionOverwriteMode` option should be replaced "
                    "with mode='overwrite_partitions' or 'overwrite_table'",
                )

            if values.get("insert_into") is not None or values.get("insertInto") is not None:
                raise ValueError(
                    "`insertInto` option was removed in onETL 0.4.0, "
                    "now df.write.insertInto or df.write.saveAsTable is selected based on table existence",
                )

            return values

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

    def check(self):
        log.info(f"|{self.__class__.__name__}| Checking connection availability...")
        self._log_parameters()

        try:
            self.sql(self._check_query)
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg) from e

        return self

    def save_df(  # type: ignore[override]
        self,
        df: DataFrame,
        table: str,
        options: Options | dict | None = None,
    ) -> None:
        write_options = self.to_options(options)

        try:
            self.get_schema(table)
            table_exists = True

            log.info(f"|{self.__class__.__name__}| Table {table!r} already exists")
        except Exception:
            table_exists = False

        if table_exists and write_options.mode != HiveWriteMode.OVERWRITE_TABLE:
            # using saveAsTable on existing table does not handle
            # spark.sql.sources.partitionOverwriteMode=dynamic, so using insertInto instead.
            self._insert_into(df, table, options)
        else:
            # if someone needs to recreate the entire table using new set of options, like partitionBy or bucketBy,
            # mode="overwrite_table" should be used
            self._save_as_table(df, table, options)

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

        log.info(f"|{self.__class__.__name__}| Fetching schema of table {table!r}")
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
        log.info(f"|Spark| Getting min and max values for column {column!r}")

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
        log_with_indent(f"MIN({column}) = {min_value!r}")
        log_with_indent(f"MAX({column}) = {max_value!r}")

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
        df_columns_lower = [column.lower() for column in df_columns]

        missing_columns_df = [column for column in df_columns_lower if column not in table_columns_lower]
        missing_columns_table = [column for column in table_columns_lower if column not in df_columns_lower]

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
                    Inconsistent columns between a table and the dataframe!

                    Table {table!r} has columns:
                        {', '.join(table_columns)}

                    Dataframe has columns:
                        {', '.join(df_columns)}
                    {missing_columns_df_message}{missing_columns_table_message}
                    """,
                ).strip(),
            )

        return sorted(df_columns, key=lambda column: table_columns_lower.index(column.lower()))

    def _insert_into(
        self,
        df: DataFrame,
        table: str,
        options: Options | dict | None = None,
    ) -> None:
        write_options = self.to_options(options)

        log.info(f"|{self.__class__.__name__}| Inserting data into existing table {table!r}")

        for key, value in write_options.dict(by_alias=True, exclude_unset=True, exclude={"mode"}).items():
            log.warning(
                f"|{self.__class__.__name__}| Option {key}={value!r} is not supported "
                "while inserting into existing table, ignoring...",
            )

        # Hive is inserting data to table by column position, not by name
        # So we should sort columns according their order in the existing table
        # instead of using order from the dataframe
        columns = self._sort_df_columns_like_table(table, df.columns)
        writer = df.select(*columns).write

        overwrite_mode: str | None
        if write_options.mode == HiveWriteMode.OVERWRITE_PARTITIONS:
            overwrite_mode = "dynamic"
        elif write_options.mode == HiveWriteMode.OVERWRITE_TABLE:
            overwrite_mode = "static"
        else:
            overwrite_mode = None

        # Writer option "partitionOverwriteMode" was added to Spark only in 2.4.0
        # so using a workaround with patching Spark config and then setting up the previous value
        original_partition_overwrite_mode = self.spark.conf.get(PARTITION_OVERWRITE_MODE_PARAM, None)

        try:  # noqa: WPS501
            if overwrite_mode:
                self.spark.conf.set(PARTITION_OVERWRITE_MODE_PARAM, overwrite_mode)

            writer.insertInto(table, overwrite=bool(overwrite_mode))
        finally:
            self.spark.conf.unset(PARTITION_OVERWRITE_MODE_PARAM)
            if original_partition_overwrite_mode:
                self.spark.conf.set(PARTITION_OVERWRITE_MODE_PARAM, original_partition_overwrite_mode)

        log.info(f"|{self.__class__.__name__}| Data is successfully inserted into table {table!r}")

    def _save_as_table(
        self,
        df: DataFrame,
        table: str,
        options: Options | dict | None = None,
    ) -> None:
        write_options = self.to_options(options)

        log.info(f"|{self.__class__.__name__}| Saving data to a table {table!r}")

        writer = df.write
        for method, value in write_options.dict(by_alias=True, exclude_none=True, exclude={"mode"}).items():
            # <value> is the arguments that will be passed to the <method>
            # format orc, parquet methods and format simultaneously
            if hasattr(writer, method):
                if isinstance(value, Iterable) and not isinstance(value, str):
                    writer = getattr(writer, method)(*value)  # noqa: WPS220
                else:
                    writer = getattr(writer, method)(value)  # noqa: WPS220
            else:
                writer = writer.option(method, value)

        overwrite = write_options.mode != HiveWriteMode.APPEND
        writer.mode("overwrite" if overwrite else "append").saveAsTable(table)

        log.info(f"|{self.__class__.__name__}| Table {table!r} successfully created")
