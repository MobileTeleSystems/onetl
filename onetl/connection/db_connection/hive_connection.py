from dataclasses import dataclass
from logging import getLogger
from typing import Optional, Any

from onetl.connection.db_connection import DBConnection
from onetl.connection.connection_helpers import get_sql_query, attribute_checker, get_indent, SPARK_INDENT


log = getLogger(__name__)


@attribute_checker(forbidden_parameter="jdbc_options")
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

    def save_df(
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        mode: Optional[str] = "append",
        **kwargs: Any,
    ) -> None:
        log.info(f"|Spark| -> |{self.__class__.__name__}| Writing DataFrame to {table}")
        log.info("|Spark| Using <<WRITER_OPTIONS>>:")
        log.info(" " * SPARK_INDENT + "<OPTION1>=<VALUE1>")
        # TODO:(@dypedchenk) rewrite, take into account all data recording possibilities. Use "mode" param.
        df.write.insertInto(table, overwrite=False)
        log.info(f"|{self.__class__.__name__}| {table} successfully written")

    def read_table(  # type: ignore
        self,
        table: str,
        columns: Optional[str] = "*",
        hint: Optional[str] = None,
        where: Optional[str] = None,
        **kwargs: Any,
    ) -> "pyspark.sql.DataFrame":

        table = get_sql_query(
            table=table,
            hint=hint,
            columns=columns,
            where=where,
        )
        log.info(f"|{self.__class__.__name__}| -> |Spark| Reading {table} to DataFrame")
        log.info("|Spark| Using connection:")
        log.info(" " * SPARK_INDENT + f"type={self.__class__.__name__}")

        df = self._execute_sql(table)
        log.info("|Spark| DataFrame successfully created from SQL statement")

        return df

    def get_schema(
        self,
        table: str,
        columns: str,
        **kwargs: Any,
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
        class_indent = get_indent(f"|{self.__class__.__name__}|")
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * class_indent + f"{query}")
        return self.spark.sql(query)
