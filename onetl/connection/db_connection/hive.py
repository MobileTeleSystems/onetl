from dataclasses import dataclass
from logging import getLogger
from textwrap import dedent
from typing import Any, Iterable, List, Optional, Tuple, Union

from pydantic import Field, validator

from onetl.connection.db_connection.db_connection import DBConnection, WriteMode
from onetl.log import LOG_INDENT

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
                    mode = WriteMode(mode)
                    if mode not in {WriteMode.APPEND, WriteMode.OVERWRITE}:
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

    def save_df(  # type: ignore[override]
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        options: Options,
    ) -> None:
        if options.insert_into:
            columns = self._sort_df_columns_like_table(table, df.columns)
            writer = df.select(*columns).write

            mode = WriteMode(options.mode)
            writer.insertInto(table, overwrite=mode == WriteMode.OVERWRITE)  # overwrite is boolean
        else:
            writer = df.write
            for method, value in options.dict(by_alias=True, exclude_none=True, exclude={"insert_into"}).items():
                # <value> is the arguments that will be passed to the <method>
                # format orc, parquet methods and format simultaneously
                if hasattr(writer, method):
                    if isinstance(value, Iterable) and not isinstance(value, str):
                        writer = getattr(writer, method)(*value)  # noqa: WPS220
                    else:
                        writer = getattr(writer, method)(value)  # noqa: WPS220
                else:
                    writer = writer.option(method, value)

            writer.saveAsTable(table)  # type: ignore

        log.info(f"|{self.__class__.__name__}| Table {table} successfully written")

    def read_table(  # type: ignore
        self,
        table: str,
        columns: Optional[List[str]],
        hint: Optional[str],
        where: Optional[str],
        options: Options,
    ) -> "pyspark.sql.DataFrame":
        if options.dict(exclude_unset=True):
            raise ValueError(
                f"{options.__class__.__name__} cannot be passed to {self.__class__.__name__}. "
                "Hive reader does not support options.",
            )

        sql_text = self.get_sql_query_cte(
            table=table,
            cte_columns=columns,
            where=where,
            cte_hint=hint,
        )

        df = self._execute_sql(sql_text)
        log.info("|Spark| DataFrame successfully created from SQL statement")

        return df

    def get_schema(  # type: ignore[override]
        self,
        table: str,
        columns: Optional[List[str]],
        options: Options,
    ) -> "pyspark.sql.types.StructType":

        query_schema = self.get_sql_query(table, columns=columns, where="1=0")

        log.info(f"|{self.__class__.__name__}| Fetching schema of {table}")

        df = self._execute_sql(query_schema)

        return df.schema

    def get_min_max_bounds(  # type: ignore[override]
        self,
        table: str,
        for_column: str,
        columns: Optional[List[str]],
        hint: Optional[str],
        where: Optional[str],
        options: Options,
    ) -> Tuple[Any, Any]:

        log.info(f"|Spark| Getting min and max values for column '{for_column}'")

        sql_text = self.get_sql_query_cte(
            table=table,
            columns=[
                self._get_min_value_sql(for_column, "min_value"),
                self._get_max_value_sql(for_column, "max_value"),
            ],
            where=where,
            cte_columns=columns,
            cte_hint=hint,
        )

        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + sql_text)

        df = self._execute_sql(sql_text)
        row = df.collect()[0]
        min_value, max_value = row["min_value"], row["max_value"]

        log.info("|Spark| Received values:")
        log.info(" " * LOG_INDENT + f"MIN({for_column}) = {min_value}")
        log.info(" " * LOG_INDENT + f"MAX({for_column}) = {max_value}")

        return min_value, max_value

    def check(self):
        self.log_parameters()

        log.info(f"|{self.__class__.__name__}| Checking connection availability...")
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + self._check_query)

        try:
            self._execute_sql(self._check_query).collect()
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg)

    def _execute_sql(self, query: str) -> "pyspark.sql.DataFrame":
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + query)
        return self.spark.sql(query)

    def _sort_df_columns_like_table(self, table: str, df_columns: List[str]) -> List[str]:
        # Hive is inserting columns by the order, not by their name
        # so if you're inserting dataframe with columns B, A, C to table with columns A, B, C, data will be damaged
        # so it is important to sort columns in dataframe to match columns in the table.

        table_columns = self.spark.table(table).columns

        # But names could have different cases, this should not cause errors
        table_columns_lower = list(map(lambda column: column.lower(), table_columns))

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
