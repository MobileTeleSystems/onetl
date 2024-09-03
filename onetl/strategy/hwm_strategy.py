# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import textwrap
import warnings
from typing import Any, Optional

from etl_entities.hwm import HWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.hwm import Edge
from onetl.log import log_hwm, log_with_indent
from onetl.strategy.base_strategy import BaseStrategy

log = logging.getLogger(__name__)


class HWMStrategy(BaseStrategy):
    hwm: Optional[HWM] = None

    @property
    def current(self) -> Edge:
        if self.hwm and self.hwm.value is not None:
            return Edge(
                value=self.hwm.value,
                including=False,
            )

        return super().current

    def update_hwm(self, value: Any) -> None:
        if self.hwm and value is not None:
            self.hwm.update(value)

    def enter_hook(self) -> None:
        # if HWM is already set (during previous run),
        # try to fetch its value using qualified_name
        self.fetch_hwm()

    def fetch_hwm(self) -> None:
        class_name = self.__class__.__name__
        if not self.hwm:
            # entering strategy context, HWM will be set later by DBReader.run or FileDownloader.run
            log.debug("|%s| HWM will not be fetched, skipping", class_name)
            return

        hwm_store = HWMStoreStackManager.get_current()
        log.info("|%s| Fetching HWM from %s:", class_name, hwm_store.__class__.__name__)
        log_with_indent(log, "name = %r", self.hwm.name)

        result = hwm_store.get_hwm(self.hwm.name)
        if result is None:
            log.warning(
                "|%s| HWM does not exist in %r. ALL ROWS/FILES WILL BE READ!",
                class_name,
                hwm_store.__class__.__name__,
            )
            return

        log.info("|%s| Fetched HWM:", class_name)
        log_hwm(log, result)

        self.validate_hwm_type(self.hwm, result)
        self.validate_hwm_attributes(self.hwm, result, origin=hwm_store.__class__.__name__)

        self.hwm.set_value(result.value)
        if self.hwm != result:
            log.info("|%s| Final HWM:", class_name)
            log_hwm(log, self.hwm)

    def validate_hwm_type(self, current_hwm: HWM, new_hwm: HWM):
        hwm_type = type(current_hwm)

        if not isinstance(new_hwm, hwm_type):
            message = textwrap.dedent(
                f"""
                Cannot cast HWM of type {type(new_hwm).__name__!r} as {hwm_type.__name__!r}.

                Please:
                * Check that you set correct HWM name, it should be unique.
                * Check that your HWM store contains valid value and type for this HWM name.
                """,
            )
            raise TypeError(message)

    def validate_hwm_attributes(self, current_hwm: HWM, new_hwm: HWM, origin: str):
        attributes = [("entity", True), ("expression", False), ("description", False)]

        for attribute, mandatory in attributes:
            if getattr(current_hwm, attribute) != getattr(new_hwm, attribute):
                # exception raised when inside one strategy >1 processes on the same table but with different entities
                # are executed, example: test_postgres_strategy_incremental_hwm_set_twice
                message = textwrap.dedent(
                    f"""
                    Detected HWM with different `{attribute}` attribute.

                    Current HWM:
                        {current_hwm!r}
                    HWM in {origin}:
                        {new_hwm!r}

                    Please:
                    * Check that you set correct HWM name, it should be unique.
                    * Check that attributes are consistent in both code and HWM Store.
                    """,
                )
                if mandatory:
                    raise ValueError(message)

                warnings.warn(message, UserWarning, stacklevel=2)

    def exit_hook(self, failed: bool = False) -> None:
        if not failed:
            self.save_hwm()

    def save_hwm(self) -> None:
        class_name = self.__class__.__name__

        if not self.hwm:
            log.debug("|%s| HWM value is not set, do not save", class_name)
            return

        hwm_store = HWMStoreStackManager.get_current()

        log.info("|%s| Saving HWM to %r:", class_name, hwm_store.__class__.__name__)
        log_hwm(log, self.hwm)

        location = hwm_store.set_hwm(self.hwm)  # type: ignore
        log.info("|%s| HWM has been saved", class_name)

        if location:
            if isinstance(location, os.PathLike):
                log_with_indent(log, "location = '%s'", os.fspath(location))
            else:
                log_with_indent(log, "location = %r", location)

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        return super()._log_exclude_fields() | {"hwm"}
