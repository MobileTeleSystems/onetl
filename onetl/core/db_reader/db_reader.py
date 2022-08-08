from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable

from etl_entities import Column, Table

from onetl._internal import uniq_ignore_case  # noqa: WPS436
from onetl.connection.db_connection import DBConnection
from onetl.log import LOG_INDENT, entity_boundary_log

log = getLogger(__name__)

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

    hwm_column : str or tuple[str, str], default: ``None``
        Column to be used as :ref:`hwm` value.

        If you want to use some SQL expression as HWM value, you can pass it as tuple
        ``("column_name", "expression")``, like:

        .. code:: python

            hwm_column = ("hwm_column", "cast(hwm_column_orig as date)")

        HWM value will be fetched using ``max(cast(hwm_column_orig as date)) as hwm_column`` SQL query.

    hint : str, default: ``None``
        Add hint to SQL query

    options : dict, :obj:`onetl.connection.DBConnection.Options`, default: ``None``
        Spark read options.
        For example:

        .. code:: python

            Postgres.Options(partitionColumn="some_column", numPartitions=20, fetchsize=1000)

        .. warning ::

            :ref:`hive` does not accept read options

    Examples
    --------
    Simple Reader creation:

    .. code:: python

        from onetl.core import DBReader
        from onetl.connection import Postgres
        from mtspark import get_spark

        spark = get_spark(
            {
                "appName": "spark-app-name",
                "spark.jars.packages": [Postgres.package],
            }
        )

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        reader = DBReader(postgres, table="fiddle.dummy")

    Reader creation with JDBC options:

    .. code:: python

        from onetl.core import DBReader
        from onetl.connection import Postgres
        from mtspark import get_spark

        spark = get_spark(
            {
                "appName": "spark-app-name",
                "spark.jars.packages": [Postgres.package],
            }
        )

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        options = {"sessionInitStatement": "select 300", "fetchsize": "100"}
        # or (it is the same):
        options = Postgres.Options(sessionInitStatement="select 300", fetchsize="100")

        reader = DBReader(postgres, table="fiddle.dummy", options=options)

    Reader creation with all parameters:

    .. code:: python

        from onetl.core import DBReader
        from onetl.connection import Postgres
        from mtspark import get_spark

        spark = get_spark(
            {
                "appName": "spark-app-name",
                "spark.jars.packages": [Postgres.package],
            },
        )

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )
        options = Postgres.Options(sessionInitStatement="select 300", fetchsize="100")

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
    hwm_expression: str | None
    options: DBConnection.Options

    def __init__(
        self,
        connection: DBConnection,
        table: str,
        columns: str | list[str] = "*",
        where: str | None = None,
        hint: str | None = None,
        hwm_column: str | tuple[str, str] | None = None,
        options: DBConnection.Options | dict | None = None,
    ):
        self.connection = connection
        self.table = self._handle_table(table)
        self.where = where
        self.hint = hint
        self.hwm_column, self.hwm_expression = self._handle_hwm_column(hwm_column)  # noqa: WPS414
        self.columns = self._handle_columns(columns)
        self.options = self._handle_options(options)

    def get_schema(self) -> StructType:
        return self.connection.get_schema(
            table=str(self.table),
            columns=self._resolve_all_columns(),
            options=self.options,
        )

    def get_min_max_bounds(self, column: str, expression: str | None = None) -> tuple[Any, Any]:
        return self.connection.get_min_max_bounds(
            table=str(self.table),
            column=column,
            expression=expression,
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

        self._log_parameters()
        self._log_options()
        self.connection.log_parameters()

        helper: StrategyHelper
        if self.hwm_column:
            helper = HWMStrategyHelper(self, self.hwm_column, self.hwm_expression)
        else:
            helper = NonHWMStrategyHelper(self)

        df = self.connection.read_table(
            table=str(self.table),
            columns=self._resolve_all_columns(),
            hint=self.hint,
            where=helper.where,
            options=self.options,
        )

        df = helper.save(df)

        entity_boundary_log(msg="DBReader ends", char="-")

        return df

    def _log_parameters(self) -> None:
        log.info(f"|{self.connection.__class__.__name__}| -> |Spark| Reading table to DataFrame using parameters:")
        log.info(LOG_INDENT + f"table = '{self.table}'")
        for attr in self.__class__.__dataclass_fields__:  # type: ignore[attr-defined]  # noqa: WPS609
            if attr in {
                "connection",
                "options",
                "table",
                "hwm_column",
            }:
                continue

            value_attr = getattr(self, attr)

            if value_attr:
                log.info(LOG_INDENT + f"{attr} = {value_attr!r}")

        if self.hwm_column:
            log.info(LOG_INDENT + f"hwm_column = '{self.hwm_column}'")

        log.info("")

    def _log_options(self) -> None:
        log.info(LOG_INDENT + "options:")
        for option, value in self.options.dict(exclude_none=True).items():
            value_wrapped = f"'{value}'" if isinstance(value, Enum) else repr(value)
            log.info(LOG_INDENT + f"    {option} = {value_wrapped}")
        log.info("")

    def _resolve_columns(self) -> list[str]:
        """
        Unwraps "*" in columns list to real column names from existing table
        """

        columns: list[str] = []

        for column in self.columns:
            if column == "*":
                schema = self.connection.get_schema(
                    table=str(self.table),
                    columns=["*"],
                    options=self.options,
                )
                field_names = schema.fieldNames()
                columns.extend(field_names)
            else:
                columns.append(column)

        return uniq_ignore_case(columns)

    def _resolve_all_columns(self) -> list[str]:
        """
        Like self._resolve_columns(), but adds hwm_column to result if it is not present
        """

        columns = self._resolve_columns()

        if not self.hwm_column:
            return columns

        hwm_statement = self.hwm_column.name
        if self.hwm_expression:
            hwm_statement = self.connection.expression_with_alias(self.hwm_expression, self.hwm_column.name)

        columns_lower = [column_name.lower() for column_name in columns]

        if self.hwm_column.name.lower() in columns_lower:
            column_index = columns_lower.index(self.hwm_column.name.lower())
            columns[column_index] = hwm_statement
        else:
            columns.append(hwm_statement)

        return columns

    def _handle_table(self, table: str) -> Table:
        return Table(name=table, instance=self.connection.instance_url)

    @staticmethod
    def _handle_hwm_column(hwm_column: str | tuple[str, str] | None) -> tuple[None, None] | tuple[Column, str | None]:
        if hwm_column is None:
            return None, None

        hwm_expression: str | None = None
        if not isinstance(hwm_column, str):
            # ("new_hwm_column", "cast(hwm_column as date)")  noqa: E800
            hwm_column, hwm_expression = hwm_column  # noqa: WPS434

            if not hwm_expression:
                raise ValueError("hwm_column should be a tuple('column_name', 'expression'), but expression is not set")

        return Column(name=hwm_column), hwm_expression

    def _handle_columns(self, columns: str | list[str]) -> list[str]:  # noqa: WPS238
        items: list[str]
        if isinstance(columns, str):
            items = columns.split(",")
        else:
            items = list(columns)

        if not items:
            raise ValueError("Columns list cannot be empty")

        result: list[str] = []
        result_lower: list[str] = []

        for item in items:
            column = item.strip()

            if not column:
                raise ValueError(f"Column name cannot be empty string, got {item!r}")

            if column.lower() in result_lower:
                raise ValueError(f"Duplicated column name: {item!r}")

            if self.hwm_expression and self.hwm_column and self.hwm_column.name.lower() == column.lower():
                raise ValueError(f"{item!r} is an alias for HWM, it cannot be used as column name")

            result.append(column)
            result_lower.append(column.lower())

        return result

    def _handle_options(self, options: DBConnection.Options | dict | None) -> DBConnection.Options:
        return self.connection.to_options(options)
