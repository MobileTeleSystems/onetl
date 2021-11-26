from dataclasses import dataclass
from logging import getLogger
from typing import Optional, Any

from onetl.connection.db_connection import DBConnection
from onetl.connection.connection_helpers import get_sql_query, attribute_checker


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

        # TODO:(@dypedchenk) rewrite, take into account all data recording possibilities. Use "mode" param.
        df.write.insertInto(table, overwrite=False)

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

        return self.spark.sql(table)

    def get_schema(
        self,
        table: str,
        columns: str,
        **kwargs: Any,
    ) -> "pyspark.sql.types.StructType":

        query_schema = f"SELECT {columns} FROM {table} WHERE 1 = 0"

        df = self.spark.sql(query_schema)

        return df.schema

    def check(self):
        try:
            self.spark.sql(self.check_statement).collect()
            log.info("Connection is available")
        except Exception as e:
            raise RuntimeError(f"Connection is unavailable:\n{e}")
