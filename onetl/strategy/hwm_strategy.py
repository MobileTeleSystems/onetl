#  Copyright 2022 MTS (Mobile Telesystems)
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

from onetl.log import log_collection, log_with_indent
from onetl.strategy.base_strategy import BaseStrategy
from onetl.strategy.hwm_store.hwm_store_manager import HWMStoreManager

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
        log_prefix = f"|{self.__class__.__name__}|"

        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            log.info(f"{log_prefix} Loading HWM from {hwm_store.__class__.__name__}:")
            log_with_indent(f"qualified_name = {self.hwm.qualified_name!r}")

            result = hwm_store.get(self.hwm.qualified_name)

            if result is not None:
                self.hwm = result  # noqa: WPS601
                log.info(f"{log_prefix} Got HWM:")
                self._log_hwm(self.hwm)
            else:
                log.warning(
                    f"{log_prefix} HWM does not exist in {hwm_store.__class__.__name__}. ALL ROWS/FILES WILL BE READ!",
                )
        else:
            # entering strategy context, HWM will be set later by DBReader.run or FileDownloader.run
            log.debug(f"{log_prefix}: HWM will not be loaded, skipping")

    def exit_hook(self, failed: bool = False) -> None:
        if not failed:
            self.save_hwm()

    def save_hwm(self) -> None:
        log_prefix = f"|{self.__class__.__name__}|"

        if self.hwm is not None:
            hwm_store = HWMStoreManager.get_current()

            log.info(f"{log_prefix} Saving HWM to {hwm_store.__class__.__name__}:")
            self._log_hwm(self.hwm)
            log_with_indent(f"qualified_name = {self.hwm.qualified_name!r}")

            location = hwm_store.save(self.hwm)
            log.info(f"{log_prefix} HWM has been saved")

            if location:
                if isinstance(location, os.PathLike):
                    log_with_indent(f"location = '{os.fspath(location)}'")
                else:
                    log_with_indent(f"location = {location}")
        else:
            log.debug(f"{log_prefix} HWM value is not set, do not save")

    def _log_hwm(self, hwm: HWM) -> None:
        log_with_indent(f"type = {hwm.__class__.__name__}")

        if isinstance(hwm.value, Collection):
            if log.isEnabledFor(logging.DEBUG):
                # file list can be very large, dont' show it unless user asked for
                log_collection("value", hwm.value, level=logging.DEBUG)
            else:
                log_with_indent(f"value = [{len(hwm.value)} items]")
        else:
            log_with_indent(f"value = {hwm.value!r}")

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        return super()._log_exclude_fields() | {"hwm"}
