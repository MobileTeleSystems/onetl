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
import os
from typing import Any, Callable, Collection, Optional

from etl_entities import HWM

from onetl.hwm.store import HWMStoreManager
from onetl.log import log_collection, log_with_indent
from onetl.strategy.base_strategy import BaseStrategy

log = logging.getLogger(__name__)


class HWMStrategy(BaseStrategy):
    hwm: Optional[HWM] = None

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
        if self.hwm is not None and value is not None:
            self.hwm.update(value)

    def enter_hook(self) -> None:
        # if HWM is already set (during previous run),
        # try to fetch its value using qualified_name
        self.fetch_hwm()

    def fetch_hwm(self) -> None:
        class_name = self.__class__.__name__

        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            log.info("|%s| Loading HWM from %s:", class_name, hwm_store.__class__.__name__)
            log_with_indent("qualified_name = %r", self.hwm.qualified_name)

            result = hwm_store.get(self.hwm.qualified_name)

            if result is not None:
                self.hwm = result
                log.info("|%s| Got HWM:", class_name)
                self._log_hwm(self.hwm)
            else:
                log.warning(
                    "|%s| HWM does not exist in %r. ALL ROWS/FILES WILL BE READ!",
                    class_name,
                    hwm_store.__class__.__name__,
                )
        else:
            # entering strategy context, HWM will be set later by DBReader.run or FileDownloader.run
            log.debug("|%s| HWM will not be loaded, skipping", class_name)

    def exit_hook(self, failed: bool = False) -> None:
        if not failed:
            self.save_hwm()

    def save_hwm(self) -> None:
        class_name = self.__class__.__name__

        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            log.info("|%s| Saving HWM to %r:", class_name, hwm_store.__class__.__name__)
            self._log_hwm(self.hwm)
            log_with_indent("qualified_name = %r", self.hwm.qualified_name)

            location = hwm_store.save(self.hwm)
            log.info("|%s| HWM has been saved", class_name)

            if location:
                if isinstance(location, os.PathLike):
                    log_with_indent("location = '%s'", os.fspath(location))
                else:
                    log_with_indent("location = %r", location)
        else:
            log.debug("|%s| HWM value is not set, do not save", class_name)

    def _log_hwm(self, hwm: HWM) -> None:
        log_with_indent("type = %s", hwm.__class__.__name__)

        if isinstance(hwm.value, Collection):
            if log.isEnabledFor(logging.DEBUG):
                # file list can be very large, dont' show it unless user asked for
                log_collection("value", hwm.value, level=logging.DEBUG)
            else:
                log_with_indent("value = %r<%d items>", hwm.value.__class__.__name__, len(hwm.value))
        else:
            log_with_indent("value = %r", hwm.value)

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        return super()._log_exclude_fields() | {"hwm"}
