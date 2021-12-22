from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING

from etl_entities import Column, Table
from onetl.connection.db_connection import DBConnection
from onetl.connection.connection_helpers import decorated_log


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

    RDBMS table with JDBC Options

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

        reader = DBReader(postgres, table="fiddle.dummy", options=jdbc_options)

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
        options = Postgres.Options(sessionInitStatement="select 300", fetchsize="100"}

        reader = DBReader(
            connection=postgres,
            table="default.test",
            where="d_id > 100",
            hint="NOWAIT",
            limit=10,
            columns=["d_id", "d_name", "d_age"],
            options=jdbc_options,
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
    table: Table
    where: str | None
    hint: str | None
    columns: str
    hwm_column: Column | None
    options: DBConnection.Options

    def __init__(
        self,
        connection: DBConnection,
        table: str,
        columns: str | list[str] = "*",
        where: str | None = None,
        hint: str | None = None,
        hwm_column: str | None = None,
        options: DBConnection.Options | dict | None = None,
    ):
        self.connection = connection
        self.table = self._handle_table(table)
        self.where = where
        self.hint = hint
        self.hwm_column = self._handle_hwm_column(hwm_column)
        self.columns = self._handle_columns(columns)
        self.options = self._handle_options(options)

    def get_schema(self) -> StructType:

        return self.connection.get_schema(  # type: ignore
            table=str(self.table),
            columns=self.columns,
            options=self.options,
        )

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

        decorated_log(msg="DBReader starts")

        helper: StrategyHelper
        if self.hwm_column:
            helper = HWMStrategyHelper(self, self.hwm_column)
        else:
            helper = NonHWMStrategyHelper(self)

        df = self.connection.read_table(
            table=str(self.table),
            columns=self.columns,
            hint=self.hint,
            where=helper.where,
            options=self.options,
        )

        df = helper.save(df)

        decorated_log(msg="DBReader ends", char="-")

        return df

    def _handle_table(self, table: str) -> Table:
        if table.count(".") != 1:
            raise ValueError("`table` should be set in format `schema.table`")

        db, table = table.split(".")
        return Table(name=table, db=db, instance=self.connection.instance_url)

    @staticmethod
    def _handle_hwm_column(hwm_column: str | None) -> Column | None:
        return Column(name=hwm_column) if hwm_column else None

    @staticmethod
    def _handle_columns(columns: str | list[str]) -> str:
        items: list[str]
        if isinstance(columns, str):
            items = columns.split(",")
        else:
            items = list(columns)

        items = [item.strip() for item in items]

        if not items or "*" in items:
            return "*"

        return ", ".join(items)

    def _handle_options(self, options: DBConnection.Options | dict | None) -> DBConnection.Options:
        if options:
            return self.connection.to_options(options)

        return self.connection.Options()
