from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from typing import Any

from etl_entities import HWM

from onetl.impl import BaseModel, path_repr
from onetl.log import log_with_indent

log = logging.getLogger(__name__)


class BaseHWMStore(BaseModel, ABC):
    def __enter__(self):
        # hack to avoid circular imports
        from onetl.strategy.hwm_store import HWMStoreManager

        log.debug(f"|{self.__class__.__name__}| Entered stack at level {HWMStoreManager.get_current_level()}")
        HWMStoreManager.push(self)

        self._log_parameters()
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
    def save(self, hwm: HWM) -> Any:
        ...  # noqa: WPS428

    def _log_parameters(self) -> None:
        log.info(f"|onETL| Using {self.__class__.__name__} as HWM Store")
        options = self.dict(by_alias=True, exclude_none=True)

        if options:
            log.info(f"|{self.__class__.__name__}| Using options:")
            for option, value in options.items():
                if isinstance(value, os.PathLike):
                    log_with_indent(f"{option} = {path_repr(value)}")
                else:
                    log_with_indent(f"{option} = {value!r}")
