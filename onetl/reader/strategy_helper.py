from __future__ import annotations

from dataclasses import dataclass, field
from logging import getLogger
from typing import TYPE_CHECKING, NoReturn

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
        self.check_hwm_match_reader()

        self.init_hwm()
        self.detect_hwm_column_boundaries()

    def check_hwm_strategy(self) -> None:
        if not isinstance(self.strategy, HWMStrategy):
            raise ValueError(
                f"{self.strategy.__class__.__name__} cannot be used "
                f"with `hwm_column` passed into {self.reader.__class__.__name__}",
            )

    def raise_wrong_hwm_type(self, hwm_type: type[HWM]) -> NoReturn:
        raise ValueError(
            f"{hwm_type.__name__} cannot be used with {self.reader.__class__.__name__}",
        )

    def check_hwm_match_reader(self) -> None:
        if self.strategy.hwm is None:
            return

        if not isinstance(self.strategy.hwm, ColumnHWM):
            self.raise_wrong_hwm_type(type(self.strategy.hwm))

        if self.strategy.hwm.source != self.reader.table or self.strategy.hwm.column != self.hwm_column:
            raise ValueError(
                f"{self.reader.__class__.__name__} was created "
                f"with `hwm_column={self.reader.hwm_column}` and `table={self.reader.table}` "
                f"but current HWM is created for ",
                f"`column={self.strategy.hwm.column}` and `source={self.strategy.hwm.source}` ",
            )

    def detect_hwm_column_type(self) -> type[HWM]:
        schema = self.reader.get_schema()
        hwm_column_type = schema[self.hwm_column.name].dataType.typeName()
        return HWMClassRegistry.get(hwm_column_type)

    def init_hwm(self) -> None:
        if self.strategy.hwm is None:
            # Small hack used only to generate qualified_name
            self.strategy.hwm = ColumnHWM(source=self.reader.table, column=self.hwm_column)

        if not self.strategy.hwm:
            self.strategy.fetch_hwm()

        hwm_type: type[HWM] | None = type(self.strategy.hwm)
        if hwm_type == ColumnHWM:
            # Remove HWM type set by hack above
            hwm_type = None

        detected_hwm_type = self.detect_hwm_column_type()

        if not hwm_type:
            hwm_type = detected_hwm_type

        if hwm_type != detected_hwm_type:
            raise TypeError(
                f'Type of "{self.hwm_column}" column is matching '
                f'"{detected_hwm_type.__name__}" which is different from "{hwm_type.__name__}"',
            )

        if hwm_type == ColumnHWM or not issubclass(hwm_type, ColumnHWM):
            self.raise_wrong_hwm_type(hwm_type)

        self.strategy.hwm = hwm_type(source=self.reader.table, column=self.hwm_column, value=self.strategy.hwm.value)

    def detect_hwm_column_boundaries(self) -> None:
        if not isinstance(self.strategy, BatchHWMStrategy):
            return

        if self.strategy.has_upper_limit and (self.strategy.has_lower_limit or self.strategy.hwm):
            # values already set by previous reader runs within the strategy
            return

        min_hwm_value, max_hwm_value = self.reader.get_min_max_bounds(self.hwm_column.name)
        if min_hwm_value is None or max_hwm_value is None:
            raise ValueError(
                "Unable to determine max and min values. ",
                f"Table {self.reader.table} column {self.hwm_column} cannot be used as `hwm_column`",
            )

        if not self.strategy.has_lower_limit and not self.strategy.hwm:
            self.strategy.start = min_hwm_value

        if not self.strategy.has_upper_limit:
            self.strategy.stop = max_hwm_value

    def save(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F  # noqa: N812

        max_df = df.select(F.max(self.hwm_column.name).alias("max_value"))

        row = max_df.collect()[0]
        max_hwm_value = row["max_value"]

        self.strategy.update_hwm(max_hwm_value)
        return df

    @property
    def where(self) -> str:
        result = [self.reader.where]

        # `self.strategy.hwm is not None` is need only to handle mypy warnings
        if self.strategy.current_value is not None and self.strategy.hwm is not None:
            compare = self.reader.get_compare_statement(
                self.strategy.current_value_comparator,
                self.strategy.hwm.name,
                self.strategy.current_value,
            )
            result.append(compare)

        if self.strategy.next_value is not None and self.strategy.hwm is not None:
            compare = self.reader.get_compare_statement(
                self.strategy.next_value_comparator,
                self.strategy.hwm.name,
                self.strategy.next_value,
            )
            result.append(compare)

        return " AND ".join(f"({where})" for where in result if where)
