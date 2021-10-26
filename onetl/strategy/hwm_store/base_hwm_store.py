from __future__ import annotations

import logging
from abc import abstractmethod

from onetl.strategy.hwm import HWM

log = logging.getLogger(__name__)


class BaseHWMStore:
    def __enter__(self):
        # hack to avoid circular imports
        from onetl.strategy.hwm_store import HWMStoreManager

        log.debug(f"{self.__class__.__name__}: Entered stack at level {HWMStoreManager.get_current_level()}")
        HWMStoreManager.push(self)
        log.info(f"{self.__class__.__name__}: Using {self} as HWM Store")
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        from onetl.strategy.hwm_store import HWMStoreManager

        log.debug(f"{self.__class__.__name__}: Exiting stack at level {HWMStoreManager.get_current_level()-1}")
        HWMStoreManager.pop()
        return False

    def __str__(self):
        return self.__class__.__name__

    @abstractmethod
    def get(self, name: str) -> HWM | None:
        return None  # noqa: WPS324

    @abstractmethod
    def save(self, hwm: HWM) -> None:
        pass  # noqa: WPS420
