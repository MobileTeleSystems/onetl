from __future__ import annotations

from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, TYPE_CHECKING, NoReturn

from etl_entities import Column, ColumnHWM, HWM
from onetl.reader.db_reader import DBReader
from onetl.strategy import StrategyManager
from onetl.strategy.hwm_store import HWMClassRegistry
from onetl.strategy.hwm_strategy import HWMStrategy
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy

log = getLogger(__name__)
# TODO:(@mivasil6) implement logging

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame


class StrategyHelper:
    @property
    def where(self) -> str | None:  # noqa: WPS463
        pass  # noqa: WPS420

    def save(self, df: DataFrame) -> DataFrame:
        pass  # noqa: WPS420


@dataclass
class NonHWMStrategyHelper(StrategyHelper):
    reader: DBReader

    def __post_init__(self):
        strategy = StrategyManager.get_current()

        if isinstance(strategy, HWMStrategy):
            raise ValueError(
                f"{strategy.__class__.__name__} cannot be used "
                f"without `hwm_column` passed into {self.reader.__class__.__name__}",
            )

    @property
    def where(self) -> str | None:
        return self.reader.where

    def save(self, df: DataFrame) -> DataFrame:
        return df


@dataclass
class HWMStrategyHelper(StrategyHelper):
    reader: DBReader
    hwm_column: Column
    strategy: HWMStrategy = field(init=False)

    def __post_init__(self):
        self.strategy = StrategyManager.get_current()  # noqa: WPS601

        self.check_hwm_strategy()
        self.check_hwm_types_match()

        self.init_hwm()
        self.fetch_hwm()

    def check_hwm_strategy(self) -> None:
        if not isinstance(self.strategy, HWMStrategy):
            raise ValueError(
                f"{self.strategy.__class__.__name__} cannot be used "
                f"with `hwm_column` passed into {self.reader.__class__.__name__}",
            )

    def raise_hwm_type(self, hwm_type: type[HWM]) -> NoReturn:
        raise ValueError(
            f"{hwm_type.__name__} cannot be used with {self.reader.__class__.__name__}",
        )

    def check_hwm_types_match(self) -> None:
        if self.strategy.hwm is None:
            return

        if not isinstance(self.strategy.hwm, ColumnHWM):
            self.raise_hwm_type(type(self.strategy.hwm))

        if self.strategy.hwm.source != self.reader.table or self.strategy.hwm.column != self.hwm_column:
            raise ValueError(
                f"{self.reader.__class__.__name__} was created "
                f"with `hwm_column={self.reader.hwm_column}` and `table={self.reader.table}` "
                f"but current HWM is created for ",
                f"`column={self.strategy.hwm.column}` and `source={self.strategy.hwm.source}` ",
            )

    def get_hwm_type(self) -> type[ColumnHWM]:
        schema = self.reader.get_schema()
        hwm_column_type = schema[self.hwm_column.name].dataType.typeName()
        result_type = HWMClassRegistry.get(hwm_column_type)

        if not issubclass(result_type, ColumnHWM):
            self.raise_hwm_type(result_type)

        return result_type

    def init_hwm(self) -> None:
        if self.strategy.hwm is not None:
            return

        hwm_type = self.get_hwm_type()
        self.strategy.hwm = hwm_type(source=self.reader.table, column=self.hwm_column)

    def fetch_hwm(self):
        if self.strategy.hwm:
            return

        if isinstance(self.strategy, BatchHWMStrategy) and not (
            self.strategy.has_lower_limit and self.strategy.has_upper_limit
        ):
            df = self.reader.connection.read_table(
                table=str(self.reader.table),
                columns=self.reader.columns,
                hint=self.reader.hint,
                where=self.where,
                options=self.reader.options,
            )

            min_hwm_value, max_hwm_value = self.get_hwm_boundaries(df)
            if min_hwm_value is None or max_hwm_value is None:
                raise ValueError(
                    "Unable to determine max and min values. ",
                    f"Table {self.reader.table} column {self.hwm_column} cannot be used as `hwm_column`",
                )

            if not self.strategy.has_lower_limit:
                self.strategy.start = min_hwm_value

            if not self.strategy.has_upper_limit:
                self.strategy.stop = max_hwm_value

        self.strategy.fetch_hwm()

    def save(self, df: DataFrame) -> DataFrame:
        _, max_hwm_value = self.get_hwm_boundaries(df)
        self.strategy.update_hwm(max_hwm_value)
        return df

    def get_hwm_boundaries(self, df: DataFrame) -> tuple[Any, Any]:
        from pyspark.sql import functions as F  # noqa: N812

        result = df.select(
            F.max(self.hwm_column.name).alias("max_value"),
            F.min(self.hwm_column.name).alias("min_value"),
        ).collect()[0]

        return result["min_value"], result["max_value"]

    @property
    def where(self) -> str:
        result = [self.reader.where]

        if self.strategy.current_value is not None and self.strategy.hwm is not None:
            comparator = self.strategy.current_value_comparator
            compare = self.reader.connection.get_compare_statement(
                comparator,
                self.strategy.hwm.name,
                self.strategy.current_value,
            )
            result.append(compare)

        if self.strategy.next_value is not None and self.strategy.hwm is not None:
            comparator = self.strategy.next_value_comparator
            compare = self.reader.connection.get_compare_statement(
                comparator,
                self.strategy.hwm.name,
                self.strategy.next_value,
            )
            result.append(compare)

        return " AND ".join(f"({where})" for where in result if where)
