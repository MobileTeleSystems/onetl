from __future__ import annotations

import logging
import operator
from dataclasses import dataclass, field
from typing import Any, Callable

from etl_entities import HWM

from onetl.log import LOG_INDENT
from onetl.strategy.base_strategy import BaseStrategy
from onetl.strategy.hwm_store.hwm_store_manager import HWMStoreManager

log = logging.getLogger(__name__)


@dataclass
class HWMStrategy(BaseStrategy):
    hwm: HWM | None = field(repr=False, default=None)

    @property
    def current_value(self) -> Any:
        if self.hwm:
            return self.hwm.value

        return super().current_value

    @property
    def current_value_comparator(self) -> Callable:
        return operator.gt

    @property
    def next_value_comparator(self) -> Callable:
        return operator.le

    def update_hwm(self, value: Any) -> None:
        if self.hwm is not None:
            self.hwm = self.hwm.with_value(value)  # noqa: WPS601

    def enter_hook(self) -> None:
        # TODO:(@mivasil6) Зачем здесь делать пустой fetch_hwm()
        self.fetch_hwm()

    def fetch_hwm(self) -> None:
        log_prefix = f"|{self.__class__.__name__}|"

        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            log.info(f"{log_prefix} Loading HWM from {hwm_store.__class__.__name__}:")
            log.info(" " * LOG_INDENT + f"qualified_name = {self.hwm.qualified_name}")

            value = hwm_store.get(self.hwm.qualified_name)

            if value is not None:
                log.info(f"{log_prefix} Got HWM:")
                log.info(" " * LOG_INDENT + f"type = {value.__class__.__name__}")
                log.info(" " * LOG_INDENT + f"value = {value.value}")
                self.hwm = value  # noqa: WPS601
            else:
                log.warning(
                    f"{log_prefix} HWM does not exist in {hwm_store.__class__.__name__}. ALL ROWS WILL BE READ!",
                )
        else:
            # TODO:(@mivasil6) спросить у Макса попадаем ли мы в это условие, и почему это не эксепшен
            log.debug(f"{log_prefix}: HWM will not be loaded, skipping")

    def exit_hook(self, failed: bool = False) -> None:
        if not failed:
            self.save_hwm()

    def save_hwm(self) -> None:
        log_prefix = f"|{self.__class__.__name__}|"

        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            # TODO:(@mivasil6) подумать над __repr__ hwm
            log.info(f"{log_prefix} Saving HWM to {hwm_store.__class__.__name__}:")
            log.info(" " * LOG_INDENT + f"type = {self.hwm.__class__.__name__}")
            log.info(" " * LOG_INDENT + f"value = {self.hwm.value}")
            log.info(" " * LOG_INDENT + f"qualified_name = {self.hwm.qualified_name}")

            location = hwm_store.save(self.hwm)
            log.info(f"{log_prefix} HWM has been saved")

            if location:
                log.info(" " * LOG_INDENT + f"location = {location}")
        else:
            log.debug(f"{log_prefix} HWM will not been saved, skipping")

    @classmethod
    def _log_exclude_field(cls, name: str) -> bool:
        return super()._log_exclude_field(name) or name == "hwm"
