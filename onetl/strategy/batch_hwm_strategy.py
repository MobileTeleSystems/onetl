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

import logging
import operator
from textwrap import dedent
from typing import Any, Callable, ClassVar

from pydantic import validator

from onetl.strategy.hwm_strategy import HWMStrategy

log = logging.getLogger(__name__)


class BatchHWMStrategy(HWMStrategy):
    step: Any

    start: Any = None
    stop: Any = None

    _iteration: int = -1

    MAX_ITERATIONS: ClassVar[int] = 100

    @validator("step", always=True)
    def step_is_not_none(cls, step):
        if not step:
            raise ValueError(f"'step' argument of {cls.__name__} cannot be empty!")

        return step

    def __iter__(self):
        self._iteration = -1
        return self

    def __next__(self):
        self._iteration += 1

        if self.is_finished:
            log.info(
                "|%s| Reached max HWM value, exiting after %s iteration(s)",
                self.__class__.__name__,
                self._iteration,
            )
            raise StopIteration

        if self.is_first_run:
            log.info("|%s| First iteration", self.__class__.__name__)
        else:
            log.info("|%s| Next iteration", self.__class__.__name__)

        return self.current_value

    @property
    def is_first_run(self) -> bool:
        return self._iteration == 0

    @property
    def is_finished(self) -> bool:
        return self.current_value is not None and self.has_upper_limit and self.current_value >= self.stop

    @property
    def has_lower_limit(self) -> bool:
        return self.start is not None

    @property
    def has_upper_limit(self) -> bool:
        return self.stop is not None

    @property
    def current_value(self) -> Any:
        if self._iteration < 0:
            raise RuntimeError(
                dedent(
                    f"""
                    Invalid {self.__class__.__name__} usage!

                    You should use it like:

                    with {self.__class__.__name__}(...) as batches:
                        for batch in batches:
                            reader.run()
                    """,
                ),
            )

        result = super().current_value

        if result is None:
            result = self.start

        self.check_argument_is_set("start", result)

        return result

    def check_argument_is_set(self, name: str, value: Any) -> None:
        if value is None and not self.is_first_run:
            raise ValueError(f"{name!r} argument of {self.__class__.__name__} cannot be empty!")

    def check_hwm_increased(self, next_value: Any) -> None:
        if self.current_value is None:
            return

        if self.stop is not None and self.current_value == self.stop:
            # if rows all have the same hwm_column value, this is not an error, read them all
            return

        if next_value is not None and self.current_value >= next_value:
            # negative or zero step - exception
            # DateHWM with step value less than one day - exception
            raise ValueError(
                f"HWM value is not increasing, please check options passed to {self.__class__.__name__}!",
            )

        if self.stop is not None:
            expected_iterations = int((self.stop - self.current_value) / self.step)
            if expected_iterations >= self.MAX_ITERATIONS:
                raise ValueError(
                    f"step={self.step!r} parameter of {self.__class__.__name__} leads to "
                    f"generating too many iterations ({expected_iterations}+)",
                )

    @property
    def next_value(self) -> Any:
        if self.current_value is not None:
            result = self.current_value + self.step
        else:
            result = self.stop

        self.check_argument_is_set("stop", result)

        if self.has_upper_limit:
            result = min(result, self.stop)

        self.check_hwm_increased(result)

        return result

    def update_hwm(self, value: Any) -> None:
        # no rows has been read, going to next iteration
        if self.hwm is not None:
            self.hwm.update(self.next_value)

        super().update_hwm(value)

    @property
    def current_value_comparator(self) -> Callable:
        if not self.hwm:
            # if start == 0 and hwm is not set
            # SQL should be `hwm_column >= 0` instead of `hwm_column > 0`
            return operator.ge

        return super().current_value_comparator
