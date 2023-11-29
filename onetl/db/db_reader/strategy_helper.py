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

import textwrap
from logging import getLogger
from typing import TYPE_CHECKING, NoReturn, Optional, Tuple

from etl_entities.hwm import HWM, ColumnHWM
from pydantic import Field, root_validator, validator
from typing_extensions import Protocol

from onetl.db.db_reader import DBReader
from onetl.hwm import Statement
from onetl.impl import FrozenModel
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_strategy import HWMStrategy
from onetl.strategy.strategy_manager import StrategyManager

log = getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame


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
    hwm: ColumnHWM
    strategy: HWMStrategy = Field(default_factory=StrategyManager.get_current)

    class Config:
        validate_all = True

    @validator("strategy", always=True, pre=True)
    def validate_strategy_is_hwm(cls, strategy, values):
        reader = values.get("reader")

        if not isinstance(strategy, HWMStrategy):
            raise ValueError(
                f"{strategy.__class__.__name__} cannot be used "
                f"with `hwm.column` passed into {reader.__class__.__name__}",
            )

        return strategy

    @validator("strategy", always=True)
    def init_hwm(cls, strategy, values):
        reader = values.get("reader")
        hwm = values.get("hwm")

        if not strategy.hwm:
            strategy.hwm = hwm

        if (
            strategy.hwm.entity != hwm.entity
            or strategy.hwm.name != hwm.name
            or strategy.hwm.expression != hwm.expression
        ):
            # exception raised when inside one strategy >1 processes on the same table but with different hwm columns
            # are executed, example: test_postgres_strategy_incremental_hwm_set_twice
            raise ValueError(
                textwrap.dedent(
                    f"""
                    Incompatible HWM values.
                    Previous run within the same strategy:
                        {strategy.hwm!r}
                    Current run:
                        {hwm!r}

                    Probably you've executed code which looks like this:
                        with {strategy.__class__.__name__}(...):
                           DBReader(hwm=one_hwm, ...).run()
                           DBReader(hwm=another_hwm, ...).run()

                    Please change it to:
                        with {strategy.__class__.__name__}(...):
                            DBReader(hwm=one_hwm, ...).run()

                        with {strategy.__class__.__name__}(...):
                            DBReader(hwm=another_hwm, ...).run()
                    """,
                ),
            )

        if strategy.hwm.value is None:
            strategy.fetch_hwm()

        if isinstance(hwm, reader.AutoDetectHWM):
            strategy.hwm = reader.detect_hwm(strategy.hwm)

        return strategy

    @validator("strategy", always=True)
    def detect_hwm_column_boundaries(cls, strategy, values):
        if not isinstance(strategy, BatchHWMStrategy):
            return strategy

        if strategy.has_upper_limit and (strategy.has_lower_limit or strategy.hwm):
            # values already set by previous reader runs within the strategy
            return strategy

        reader = values.get("reader")
        hwm = values.get("hwm")
        hwm_column = hwm.entity
        hwm_expression = hwm.expression

        min_hwm_value, max_hwm_value = reader.get_min_max_bounds(hwm_column, hwm_expression)
        if min_hwm_value is None or max_hwm_value is None:
            raise ValueError(
                "Unable to determine max and min values. ",
                f"HWM instance '{hwm.name}' column '{hwm_column}' cannot be used as column for `hwm`",
            )

        if not strategy.has_lower_limit and (strategy.hwm is None or strategy.hwm.value is None):
            strategy.start = min_hwm_value

        if not strategy.has_upper_limit:
            strategy.stop = max_hwm_value

        return strategy

    @staticmethod
    def raise_wrong_hwm_type(reader: DBReader, hwm_type: type[HWM]) -> NoReturn:
        raise ValueError(
            f"{hwm_type.__name__} cannot be used with {reader.__class__.__name__}",
        )

    def save(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F  # noqa: N812

        log.info("|DBReader| Calculating max value for column %r in the dataframe...", self.hwm.entity)
        max_df = df.select(F.max(self.hwm.entity).alias("max_value"))
        row = max_df.collect()[0]
        max_hwm_value = row["max_value"]
        log.info("|DBReader| Max value is: %r", max_hwm_value)

        self.strategy.update_hwm(max_hwm_value)
        return df

    def get_boundaries(self) -> tuple[Statement | None, Statement | None]:
        start_from: Statement | None = None
        end_at: Statement | None = None
        hwm: ColumnHWM | None = self.strategy.hwm  # type: ignore

        if not hwm:
            return None, None

        if self.strategy.current_value is not None:
            start_from = Statement(
                expression=self.hwm.expression or hwm.entity,
                operator=self.strategy.current_value_comparator,
                value=self.strategy.current_value,
            )

        if self.strategy.next_value is not None:
            end_at = Statement(
                expression=self.hwm.expression or hwm.entity,
                operator=self.strategy.next_value_comparator,
                value=self.strategy.next_value,
            )

        return start_from, end_at
