from dataclasses import dataclass
from logging import getLogger
from typing import Optional, List, Tuple, Iterable, Union

from pydantic import Field

from onetl.connection.db_connection import DBConnection
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
        bucket_by: Optional[Tuple[int, str]] = Field(alias="bucketBy")
        sort_by: Optional[Union[List[str], str]] = Field(alias="sortBy")
        format: str = Field(default="orc")
        compression: Optional[str] = None
        insert_into: bool = Field(alias="insertInto", default=False)

    # TODO (@msmarty5): Replace with active_namenode function from mtspark
    @property
    def instance_url(self) -> str:
        return "cluster"

    def save_df(  # type: ignore[override]
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        options: Options,
    ) -> None:
        log.info("|Spark| With writer options:")
        for option, value in options.dict(exclude_none=True).items():
            log.info(" " * LOG_INDENT + f"{option} = {value}")

        writer = df.write

        if options.insert_into:  # type: ignore
            writer.insertInto(table, overwrite=options.mode == "overwrite")  # overwrite = True or False
        else:
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

        if options:
            if options.dict(exclude_unset=True):
                raise ValueError(
                    f"{options.__class__.__name__} cannot be passed to {self.__class__.__name__}. "
                    "Hive reader does not support options.",
                )

        sql_text = get_sql_query(
            table=table,
            hint=hint,
            columns=columns,
            where=where,
        )

        log.info("|Spark| Using connection:")
        log.info(" " * LOG_INDENT + f"type = {self.__class__.__name__}")

        df = self._execute_sql(sql_text)
        log.info("|Spark| DataFrame successfully created from SQL statement")

        return df

    def get_schema(
        self,
        table: str,
        columns: str,
        options: DBConnection.Options,
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
        log.info(" " * LOG_INDENT + f"{query}")
        return self.spark.sql(query)
