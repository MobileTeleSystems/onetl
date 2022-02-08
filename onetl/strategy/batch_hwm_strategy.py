from __future__ import annotations

from typing import Any

from dataclasses import dataclass, field
import logging

from onetl.strategy.hwm_strategy import HWMStrategy

log = logging.getLogger(__name__)


@dataclass
class BatchHWMStrategy(HWMStrategy):
    step: Any = None

    start: Any = None
    stop: Any = None

    _iteration: int = field(init=False, repr=False, default=0)

    def __post_init__(self):
        if not self.step:
            raise ValueError(f"`step` argument of {self.__class__.__name__} cannot be empty!")

    def __iter__(self):
        self._iteration = -1  # noqa: WPS601
        return self

    def __next__(self):
        self._iteration += 1  # noqa: WPS601

        if self.is_finished:
            log.info(f"|{self.__class__.__name__}| Reached max HWM value, exiting after {self._iteration} iteration(s)")
            raise StopIteration

        iteration_name = "First" if self.is_first_run else "Next"
        log.info(f"|{self.__class__.__name__}| {iteration_name} iteration")

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
        result = super().current_value

        if result is None:
            result = self.start

        self.check_argument_is_set("start", result)

        return result

    def check_argument_is_set(self, name: str, value: Any) -> None:
        if value is None and not self.is_first_run:
            raise ValueError(f"`{name}` argument of {self.__class__.__name__} cannot be empty!")

    def check_cannot_decrease(self, value: Any) -> None:
        if (
            self.current_value is not None
            and value is not None
            and self.current_value_comparator(self.current_value, value)
        ):
            raise ValueError(
                f"HWM {self.hwm} value is started to decrease, "
                f"please check options passed to {self.__class__.__name__}!",
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

        self.check_cannot_decrease(result)

        return result

    def update_hwm(self, value: Any) -> None:
        # no rows has been read, going to next iteration
        if self.hwm is not None:
            self.hwm = self.hwm.with_value(self.next_value)

        super().update_hwm(value)
