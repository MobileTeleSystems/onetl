# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from textwrap import dedent
from typing import Any, ClassVar

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.hwm import Edge
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

        return self.current, self.next

    @property
    def is_first_run(self) -> bool:
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

        return self._iteration == 0

    @property
    def is_finished(self) -> bool:
        if self._iteration >= self.MAX_ITERATIONS:
            # prevent possible infinite loops in unexpected cases
            return True

        if self.current.is_set() and self.stop is not None:
            return self.current.value >= self.stop

        return False

    def check_has_data(self, value: Any):
        if not self.is_first_run and value is None:
            log.info(
                "|%s| No start or stop values are set, exiting after %s iteration(s)",
                self.__class__.__name__,
                self._iteration,
            )
            raise StopIteration

    @property
    def current(self) -> Edge:
        result = super().current
        if not result.is_set():
            result = Edge(
                value=self.start,
                including=True,
            )

        self.check_has_data(result.value)
        return result

    def check_hwm_increased(self, next_value: Any) -> None:
        if not self.current.is_set():
            return

        if self.stop is not None and self.current.value == self.stop:
            # if rows all have the same expression value, this is not an error, read them all
            return

        if next_value is not None and self.current.value >= next_value:
            # negative or zero step - exception
            # DateHWM with step value less than one day - exception
            raise ValueError(
                f"HWM value is not increasing, please check options passed to {self.__class__.__name__}!",
            )

        if self.stop is not None:
            expected_iterations = int((self.stop - self.current.value) / self.step)
            if expected_iterations >= self.MAX_ITERATIONS:
                raise ValueError(
                    f"step={self.step!r} parameter of {self.__class__.__name__} leads to "
                    f"generating too many iterations ({expected_iterations}+)",
                )

    @property
    def next(self) -> Edge:
        if self.current.is_set():
            if not hasattr(self.current.value, "__add__"):
                raise RuntimeError(f"HWM: {self.hwm!r} cannot be used with Batch strategies")

            result = Edge(value=self.current.value + self.step)
        else:
            result = Edge(value=self.stop)

        self.check_has_data(result.value)

        if self.stop is not None:
            result.value = min(result.value, self.stop)

        self.check_hwm_increased(result.value)
        return result

    def update_hwm(self, value: Any) -> None:
        # batch strategy ticks determined by step size only,
        # not by real HWM value read from source
        if self.hwm:
            self.hwm.update(self.next.value)
