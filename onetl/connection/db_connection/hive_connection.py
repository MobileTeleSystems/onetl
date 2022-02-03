from dataclasses import dataclass
from logging import getLogger
from typing import Optional, List, Tuple, Iterable, Union

from pydantic import Field, validator

from onetl.connection.db_connection.db_connection import DBConnection, WriteMode
from onetl.connection.connection_helpers import get_sql_query, LOG_INDENT


log = getLogger(__name__)


@dataclass(frozen=True)
class Hive(DBConnection):
    """Class for Hive spark connection.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession, default: ``None``
        Spark session that required for connection to hive.

        You can use ``mtspark`` for spark session initialization.

    Examples
    --------

    Hive connection initialization

    .. code::

        from onetl.connection.db_connection import Hive
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
        self._log_parameters()

        if options.insert_into:
            table_columns = self.spark.table(table).columns
            columns_to_save = [column for column in table_columns if column in df.columns]

            writer = df.select(*columns_to_save).write

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
        options: Options,
        columns: Optional[str] = "*",
        hint: Optional[str] = None,
        where: Optional[str] = None,
    ) -> "pyspark.sql.DataFrame":
        if options.dict(exclude_unset=True):
            raise ValueError(
                f"{options.__class__.__name__} cannot be passed to {self.__class__.__name__}. "
                "Hive reader does not support options.",
            )

        self._log_parameters()

        sql_text = get_sql_query(
            table=table,
            hint=hint,
            columns=columns,
            where=where,
        )

        df = self._execute_sql(sql_text)
        log.info("|Spark| DataFrame successfully created from SQL statement")

        return df

    def get_schema(  # type: ignore[override]
        self,
        table: str,
        columns: str,
        options: Options,
    ) -> "pyspark.sql.types.StructType":

        query_schema = f"SELECT {columns} FROM {table} WHERE 1 = 0"

        log.info(f"|{self.__class__.__name__}| Fetching schema of {table}")

        df = self._execute_sql(query_schema)

        return df.schema

    def check(self):
        try:
            log.info(
                f"|{self.__class__.__name__}| Check connection availability...",
            )
            self._execute_sql(self.check_statement).collect()
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg)

    def _execute_sql(self, query):
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + query)
        return self.spark.sql(query)
