#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, NoReturn, Optional, Tuple

from etl_entities import HWM, Column, ColumnHWM
from pydantic import Field, root_validator, validator
from typing_extensions import Protocol

from onetl.db.db_reader.db_reader import DBReader
from onetl.hwm import Statement
from onetl.hwm.store import HWMClassRegistry
from onetl.impl import FrozenModel
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_strategy import HWMStrategy
from onetl.strategy.strategy_manager import StrategyManager

log = getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame


# ColumnHWM has abstract method serialize_value, so it's not possible to create a class instance
# small hack to bypass this exception
class MockColumnHWM(ColumnHWM):
    def serialize_value(self):
        """Fake implementation of ColumnHWM.serialize_value"""


class StrategyHelper(Protocol):
    def save(self, df: DataFrame) -> DataFrame:
        """Saves HWM value to HWMStore"""

    def get_boundaries(self) -> tuple[Statement | None, Statement | None]:
        """Returns ``(min_boundary, max_boundary)`` for applying HWM to source"""


class NonHWMStrategyHelper(FrozenModel):
    reader: DBReader

    def get_boundaries(self) -> Tuple[Optional[Statement], Optional[Statement]]:
        return None, None

    @root_validator(pre=True)
    def validate_current_strategy(cls, values):
        reader = values.get("reader")
        strategy = StrategyManager.get_current()

        if isinstance(strategy, HWMStrategy):
            raise ValueError(
                f"{strategy.__class__.__name__} cannot be used "
                f"without `hwm_column` passed into {reader.__class__.__name__}",
            )

        return values

    def save(self, df: DataFrame) -> DataFrame:
        return df


class HWMStrategyHelper(FrozenModel):
    reader: DBReader
    hwm_column: Column
    hwm_expression: Optional[str] = None
    strategy: HWMStrategy = Field(default_factory=StrategyManager.get_current)

    class Config:
        validate_all = True

    @validator("strategy", always=True, pre=True)
    def validate_strategy_is_hwm(cls, strategy, values):
        reader = values.get("reader")

        if not isinstance(strategy, HWMStrategy):
            raise ValueError(
                f"{strategy.__class__.__name__} cannot be used "
                f"with `hwm_column` passed into {reader.__class__.__name__}",
            )

        return strategy

    @validator("strategy", always=True)
    def validate_strategy_matching_reader(cls, strategy, values):
        if strategy.hwm is None:
            return strategy

        reader = values.get("reader")
        hwm_column = values.get("hwm_column")

        if not isinstance(strategy.hwm, ColumnHWM):
            cls.raise_wrong_hwm_type(reader, type(strategy.hwm))

        if strategy.hwm.source != reader.source or strategy.hwm.column != hwm_column:
            raise ValueError(
                f"{reader.__class__.__name__} was created "
                f"with `hwm_column={reader.hwm_column}` and `source={reader.source}` "
                f"but current HWM is created for ",
                f"`column={strategy.hwm.column}` and `source={strategy.hwm.source}` ",
            )

        return strategy

    @validator("strategy", always=True)
    def init_hwm(cls, strategy, values):
        reader = values.get("reader")
        hwm_column = values.get("hwm_column")

        if strategy.hwm is None:
            # Small hack used only to generate qualified_name
            strategy.hwm = MockColumnHWM(source=reader.source, column=hwm_column)

        if not strategy.hwm:
            strategy.fetch_hwm()

        hwm_type: type[HWM] | None = type(strategy.hwm)
        if hwm_type == MockColumnHWM:
            # Remove HWM type set by hack above
            hwm_type = None

        detected_hwm_type = cls.detect_hwm_column_type(reader, hwm_column)

        if not hwm_type:
            hwm_type = detected_hwm_type

        if hwm_type != detected_hwm_type:
            raise TypeError(
                f'Type of "{hwm_column}" column is matching '
                f'"{detected_hwm_type.__name__}" which is different from "{hwm_type.__name__}"',
            )

        if hwm_type == MockColumnHWM or not issubclass(hwm_type, ColumnHWM):
            cls.raise_wrong_hwm_type(reader, hwm_type)

        strategy.hwm = hwm_type(source=reader.source, column=hwm_column, value=strategy.hwm.value)
        return strategy

    @validator("strategy", always=True)
    def detect_hwm_column_boundaries(cls, strategy, values):
        if not isinstance(strategy, BatchHWMStrategy):
            return strategy

        if strategy.has_upper_limit and (strategy.has_lower_limit or strategy.hwm):
            # values already set by previous reader runs within the strategy
            return strategy

        reader = values.get("reader")
        hwm_column = values.get("hwm_column")
        hwm_expression = values.get("hwm_expression")

        min_hwm_value, max_hwm_value = reader.get_min_max_bounds(hwm_column.name, hwm_expression)
        if min_hwm_value is None or max_hwm_value is None:
            raise ValueError(
                "Unable to determine max and min values. ",
                f"Table '{reader.source}' column '{hwm_column}' cannot be used as `hwm_column`",
            )

        if not strategy.has_lower_limit and not strategy.hwm:
            strategy.start = min_hwm_value

        if not strategy.has_upper_limit:
            strategy.stop = max_hwm_value

        return strategy

    @staticmethod
    def raise_wrong_hwm_type(reader: DBReader, hwm_type: type[HWM]) -> NoReturn:
        raise ValueError(
            f"{hwm_type.__name__} cannot be used with {reader.__class__.__name__}",
        )

    @staticmethod
    def detect_hwm_column_type(reader: DBReader, hwm_column: Column) -> type[HWM]:
        schema = {field.name.casefold(): field for field in reader.get_df_schema()}
        column = hwm_column.name.casefold()
        hwm_column_type = schema[column].dataType.typeName()
        return HWMClassRegistry.get(hwm_column_type)

    def save(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F  # noqa: N812

        log.info("|DBReader| Calculating max value for column %r in the dataframe...", self.hwm_column.name)
        max_df = df.select(F.max(self.hwm_column.name).alias("max_value"))
        row = max_df.collect()[0]
        max_hwm_value = row["max_value"]
        log.info("|DBReader| Max value is: %r", max_hwm_value)

        self.strategy.update_hwm(max_hwm_value)
        return df

    def get_boundaries(self) -> tuple[Statement | None, Statement | None]:
        start_from: Statement | None = None
        end_at: Statement | None = None
        hwm: ColumnHWM | None = self.strategy.hwm  # type: ignore

        if hwm is None:
            return None, None

        if self.strategy.current_value is not None:
            start_from = Statement(
                expression=self.hwm_expression or hwm.name,
                operator=self.strategy.current_value_comparator,
                value=self.strategy.current_value,
            )

        if self.strategy.next_value is not None:
            end_at = Statement(
                expression=self.hwm_expression or hwm.name,
                operator=self.strategy.next_value_comparator,
                value=self.strategy.next_value,
            )

        return start_from, end_at
