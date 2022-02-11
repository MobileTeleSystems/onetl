from __future__ import annotations

import logging
from abc import abstractmethod

from etl_entities import HWM
from onetl.log import LOG_INDENT

log = logging.getLogger(__name__)


class BaseHWMStore:
    def __enter__(self):
        # hack to avoid circular imports
        from onetl.strategy.hwm_store import HWMStoreManager

        log.debug(f"|{self.__class__.__name__}| Entered stack at level {HWMStoreManager.get_current_level()}")
        HWMStoreManager.push(self)
        log.info(f"|onETL| Using {self.__class__.__name__} as HWM Store")
        options = {key: value for key, value in vars(self).items() if not key.startswith("_")}

        if options:
            log.info(f"|{self.__class__.__name__}| Using options:")
            for option, value in options.items():
                log.info(" " * LOG_INDENT + f"{option} = {value}")

        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        from onetl.strategy.hwm_store import HWMStoreManager

        log.debug(f"|{self}| Exiting stack at level {HWMStoreManager.get_current_level()-1}")
        HWMStoreManager.pop()
        return False

    def __str__(self):
        return self.__class__.__name__

    @abstractmethod  # noqa: WPS324
    def get(self, name: str) -> HWM | None:
        ...  # noqa: WPS428

    @abstractmethod
    def save(self, hwm: HWM) -> None:
        ...  # noqa: WPS428
