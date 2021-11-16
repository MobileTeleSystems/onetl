from dataclasses import dataclass
from typing import Optional, Dict

from onetl.connection.db_connection.db_connection import DBConnection


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

    port: int = 10000

    @property
    def url(self) -> str:
        params = "&".join(f"{k}={v}" for k, v in self.extra.items())

        return f"hiveserver2://{self.user}:{self.password}@{self.host}:{self.port}?{params}"

    def save_df(
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        jdbc_options: Dict,
    ) -> None:
        # TODO:(@dypedchenk) rewrite, take into account all data recording possibilities. Use "mode" param.
        df.write.insertInto(table, overwrite=False)

    # TODO:(@dypedchenk) think about unused params in this method
    def read_table(
        self,
        jdbc_options: Dict,
        table: str,
        columns: Optional[str] = "*",
        hint: Optional[str] = None,
        where: Optional[str] = None,
    ) -> "pyspark.sql.DataFrame":

        table = self.get_sql_query(
            table=table,
            hint=hint,
            columns=columns,
            where=where,
        )
        return self.spark.sql(table)  # type: ignore[union-attr]
