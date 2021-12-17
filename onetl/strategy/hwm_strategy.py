from __future__ import annotations

from dataclasses import dataclass, field
import logging
import operator
from typing import Any, Callable

from etl_entities import HWM
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
        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            log.info(f"|onETL| Loading {self.hwm!r} from {hwm_store}")
            value = hwm_store.get(self.hwm.qualified_name)

            if value is not None:
                log.info(f"|onETL| Received HWM value: {value!r}")
                self.hwm = value  # noqa: WPS601
            else:
                log.info(f"|onETL| HWM is not exist in {hwm_store}. Using snapshot strategy instead incremental.")
        else:
            # TODO:(@mivasil6) спросить у Макса попадаем ли мы в это условие, и почему это не эксепшен
            log.debug(f"{self.__class__.__name__}: HWM will not be loaded, skipping")

    def exit_hook(self, failed: bool = False) -> None:
        if not failed:
            self.save_hwm()

    def save_hwm(self) -> None:
        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()
            # TODO:(@mivasil6) подумать над __repr__ hwm
            log.info(f"|onETL| Saving {self.hwm!r} to {hwm_store}")

            hwm_store.save(self.hwm)
            log.info("|onETL| HWM has been saved")
        else:
            log.debug(f"{self.__class__.__name__}: HWM will not been saved, skipping")
