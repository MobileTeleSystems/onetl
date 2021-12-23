from __future__ import annotations

import logging
from abc import abstractmethod

from etl_entities import HWM
from onetl.connection.connection_helpers import get_indent

log = logging.getLogger(__name__)


class BaseHWMStore:
    def __enter__(self):
        # hack to avoid circular imports
        from onetl.strategy.hwm_store import HWMStoreManager

        indent = get_indent(f"|{self.__class__.__name__}|")

        log.debug(f"|{self}| Entered stack at level {HWMStoreManager.get_current_level()}")
        HWMStoreManager.push(self)
        log.info(f"|onETL| Using {self} as HWM Store")
        log.info(f"|{self.__class__.__name__}| Using options:")
        log.info(" " * indent + "opt=opt_val")

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
