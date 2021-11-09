from __future__ import annotations

from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, TYPE_CHECKING

from onetl.connection.db_connection import DBConnection


log = getLogger(__name__)
# TODO:(@mivasil6) implement logging

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class DBReader:
    """Class allows you to read data from a table with specified connection
    and parameters and save it as Spark dataframe

    Parameters
    ----------
    connection: onetl.connection.db_connection.DBConnection
        Class which contain DB connection properties. See in DBConnection section.
    table : str
        Table name from which to read. You need to specify the full path to the table, including the schema.
        Name like ``schema.name``
    columns : list of str, optional, default: ``*``
        The list of columns to be read
    where : str, optional, default: ``None``
        Custom ``where`` for SQL query
    hint : str, optional, default: ``None``
        Add hint to SQL query
    jdbc_options : dict, optional, default: ``None``
        Spark jdbc read options.
        For example: ``{"partitionColumn": "some_column", "numPartitions": 20, "fetchsize": 1000}``

        You can find all list of options in link below:

        https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

        .. warning ::

            Doesn't work on ``Hive`` DBConnection


    Examples
    --------
    Simple Reader creation

    .. code::

        from onetl.reader import DBReader
        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        reader = DBReader(postgres, table="fiddle.dummy")

    RDBMS table with jdbc_options

    .. code::

        from onetl.reader import DBReader
        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        jdbc_options = {"sessionInitStatement": "select 300", "fetchsize": "100"}

        reader = DBReader(postgres, table="fiddle.dummy", jdbc_options=jdbc_options)

    Reader creation with all params:

    .. code::

        from onetl.reader import DBReader
        from onetl.connection.db_connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": Postgres.package,
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        jdbc_options = {"sessionInitStatement": "select 300", "fetchsize": "100"}

        reader = DBReader(
            connection=postgres,
            table="default.test",
            where="d_id > 100",
            hint="NOWAIT",
            limit=10,
            columns=["d_id", "d_name", "d_age"],
            jdbc_options=jdbc_options,
        )

    Reader for Hive with all available params:

    .. code::

        from onetl.connection.db_connection import Hive
        from onetl.reader import DBReader
        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        hive = Hive(spark=spark)

        reader = DBReader(
            connection=hive,
            table="default.test",
            where="d_id > 100",
            hint="NOWAIT",
            limit=10,
            columns=["d_id", "d_name", "d_age"],
        )
    """

    connection: DBConnection
    # table is 'schema.table'
    table: str
    columns: str | list[str] | None = None
    where: str | None = None
    hint: str | None = None
    hwm_column: str | None = None
    jdbc_options: dict[str, Any] = field(default_factory=dict)

    def get_columns(self) -> str:
        return ", ".join(self.columns) if isinstance(self.columns, list) else self.columns or "*"  # noqa: WPS601

    def get_schema(self) -> StructType:
        return self.connection.get_schema(table=self.table, columns=self.get_columns(), jdbc_options=self.jdbc_options)

    def run(self) -> DataFrame:
        """
        Reads data from source table and saves as Spark dataframe

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Read df

        .. code::

            df = reader.run()

        """

        # avoid circular imports
        from onetl.reader.strategy_helper import StrategyHelper, NonHWMStrategyHelper, HWMStrategyHelper

        helper: StrategyHelper
        if self.hwm_column:
            helper = HWMStrategyHelper(self, self.hwm_column)
        else:
            helper = NonHWMStrategyHelper(self)

        df = self.connection.read_table(
            table=self.table,
            columns=self.get_columns(),
            hint=self.hint,
            where=helper.where,
            jdbc_options=self.jdbc_options,
        )

        return helper.save(df)
