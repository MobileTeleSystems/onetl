from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable

from etl_entities import Column, Table

from onetl.connection.db_connection import DBConnection
from onetl.log import LOG_INDENT, entity_boundary_log

log = getLogger(__name__)
# TODO:(@mivasil6) implement logging

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class DBReader:
    """The DBReader class allows you to read data from a table with specified connection
    and parameters and save it as Spark dataframe

    Parameters
    ----------
    connection : :obj:`onetl.connection.DBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section

    table : str
        Table name from which to read. You need to specify the full path to the table, including the schema.
        Name like ``schema.name``

    columns : list of str, default: ``["*"]``
        The list of columns to be read

    where : str, default: ``None``
        Custom ``where`` for SQL query

    hwm_column : str, default: ``None``
        Column to be used as :ref:`hwm` value

        .. warning ::

            For :ref:`oracle` you must specify ``hwm_column`` name in UPPERCASE.

    hint : str, default: ``None``
        Add hint to SQL query

    options : dict, :obj:`onetl.connection.DBConnection.Options`, default: ``None``
        Spark JDBC read options.
        For example:

        .. code::

            Options(partitionColumn="some_column", numPartitions=20, fetchsize=1000)

        List of options:

            * ``partitionColumn``
            * ``lowerBound``
            * ``upperBound``
            * ``numPartitions``
            * ``queryTimeout``
            * ``fetchsize``
            * ``sessionInitStatement``
            * ``customSchema``
            * ``pushDownPredicate``

        You can find a description of the options at the link below:

        https://spark.apache.org/docs/2.4.0/sql-data-sources-jdbc.html

        .. warning ::

            :ref:`hive` does not accept read options


    Examples
    --------
    Simple Reader creation:

    .. code::

        from onetl.core import DBReader
        from onetl.connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Postgres.package],
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        reader = DBReader(postgres, table="fiddle.dummy")

    Reader creation with JDBC options:

    .. code::

        from onetl.core import DBReader
        from onetl.connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Postgres.package],
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        options = {"sessionInitStatement": "select 300", "fetchsize": "100"}
        # or (it is the same):
        options = Postgres.Options(sessionInitStatement="select 300", fetchsize="100"}

        reader = DBReader(postgres, table="fiddle.dummy", options=options)

    Reader creation with all parameters:

    .. code::

        from onetl.core import DBReader
        from onetl.connection import Postgres
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Postgres.package],
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
            columns=["d_id", "d_name", "d_age"],
            hwm_column="d_age",
            options=options,
        )
    """

    connection: DBConnection
    table: Table
    where: str | None
    hint: str | None
    columns: list[str]
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
        return self.connection.get_schema(
            table=str(self.table),
            columns=self._resolve_columns(),
            options=self.options,
        )

    def get_min_max_bounds(self, column: str) -> tuple[Any, Any]:
        return self.connection.get_min_max_bounds(
            table=str(self.table),
            for_column=column,
            columns=self._resolve_columns(),
            hint=self.hint,
            where=self.where,
            options=self.options,
        )

    def get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> str:
        return self.connection.get_compare_statement(comparator, arg1, arg2)

    def run(self) -> DataFrame:
        """
        Reads data from source table and saves as Spark dataframe

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Read data to Spark dataframe:

        .. code::

            df = reader.run()

        """

        # avoid circular imports
        from onetl.core.db_reader.strategy_helper import (
            HWMStrategyHelper,
            NonHWMStrategyHelper,
            StrategyHelper,
        )

        entity_boundary_log(msg="DBReader starts")

        log.info(f"|{self.connection.__class__.__name__}| -> |Spark| Reading table to DataFrame")

        log.info(f"|{self.__class__.__name__}| Parameters:")
        for attr in self.__class__.__dataclass_fields__:  # type: ignore[attr-defined]  # noqa: WPS609
            if attr in {
                "connection",
                "options",
            }:
                continue

            value_attr = getattr(self, attr)

            if value_attr:
                log.info(" " * LOG_INDENT + f"{attr} = {value_attr}")

        log.info("")
        log.info(" " * LOG_INDENT + "Options")
        for option, value in self.options.dict(exclude_none=True).items():
            log.info(" " * LOG_INDENT + f"    {option} = {value}")

        self.connection.log_parameters()

        helper: StrategyHelper
        if self.hwm_column:
            helper = HWMStrategyHelper(self, self.hwm_column)
        else:
            helper = NonHWMStrategyHelper(self)

        df = self.connection.read_table(
            table=str(self.table),
            columns=self._resolve_columns(),
            hint=self.hint,
            where=helper.where,
            options=self.options,
        )

        df = helper.save(df)

        entity_boundary_log(msg="DBReader ends", char="-")

        return df

    def _resolve_columns(self) -> list[str]:
        columns: list[str] = []
        for column in self.columns:
            if column == "*":
                schema = self.connection.get_schema(
                    table=str(self.table),
                    columns=["*"],
                    options=self.options,
                )
                real_columns = schema.fieldNames()
                columns.extend(real_columns)
            else:
                columns.append(column)

        return columns

    def _handle_table(self, table: str) -> Table:
        return Table(name=table, instance=self.connection.instance_url)

    @staticmethod
    def _handle_hwm_column(hwm_column: str | None) -> Column | None:
        return Column(name=hwm_column) if hwm_column else None

    @staticmethod
    def _handle_columns(columns: str | list[str]) -> list[str]:
        items: list[str]
        if isinstance(columns, str):
            items = columns.split(",")
        else:
            items = list(columns)

        if not items:
            raise ValueError("Columns list cannot be empty")

        result: list[str] = []

        for item in items:
            column = item.strip()

            if not column:
                raise ValueError(f"Column name cannot be empty string, got '{item}'")

            if column in result:
                raise ValueError(f"Duplicated column name: '{item}'")

            result.append(column)

        return result

    def _handle_options(self, options: DBConnection.Options | dict | None) -> DBConnection.Options:
        if options:
            return self.connection.to_options(options)

        return self.connection.Options()