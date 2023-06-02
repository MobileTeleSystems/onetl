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
import os
from abc import ABC, abstractmethod
from typing import Any

from etl_entities import HWM

from onetl.impl import BaseModel, path_repr
from onetl.log import log_with_indent

log = logging.getLogger(__name__)


class BaseHWMStore(BaseModel, ABC):
    def __enter__(self):
        """
        HWM store context manager.

        Enter this context to use this HWM store instance as current one (instead default).

        Examples
        --------

        .. code:: python

            with hwm_store:
                db_reader.run()
        """
        # hack to avoid circular imports
        from onetl.hwm.store import HWMStoreManager

        log.debug("|%s| Entered stack at level %d", self.__class__.__name__, HWMStoreManager.get_current_level())
        HWMStoreManager.push(self)

        self._log_parameters()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        from onetl.hwm.store import HWMStoreManager

        log.debug("|%s| Exiting stack at level %d", self, HWMStoreManager.get_current_level() - 1)
        HWMStoreManager.pop()
        return False

    @abstractmethod
    def get(self, name: str) -> HWM | None:
        """
        Get HWM by qualified name from HWM store. |support_hooks|

        Parameters
        ----------
        name : str
            HWM qualified name

        Returns
        -------
        HWM object, if it exists in HWM store, or None

        Examples
        --------

        .. code:: python

            from etl_entities import IntHWM

            # just to generate qualified name using HWM parts
            empty_hwm = IntHWM(column=..., table=..., process=...)
            real_hwm = hwm_store.get(empty_hwm.qualified_name)
        """

    @abstractmethod
    def save(self, hwm: HWM) -> Any:
        """
        Save HWM object to HWM Store. |support_hooks|

        Parameters
        ----------
        hwm : :obj:`etl_entities.hwm.HWM`
            HWM object

        Returns
        -------
        HWM location, like URL of file path.

        Examples
        --------

        .. code:: python

            from etl_entities import IntHWM

            hwm = IntHWM(value=..., column=..., table=..., process=...)
            hwm_location = hwm_store.save(hwm)
        """

    def _log_parameters(self) -> None:
        log.info("|onETL| Using %s as HWM Store", self.__class__.__name__)
        options = self.dict(by_alias=True, exclude_none=True)

        if options:
            log.info("|%s| Using options:", self.__class__.__name__)
            for option, value in options.items():
                if isinstance(value, os.PathLike):
                    log_with_indent("%s = %s", option, path_repr(value))
                else:
                    log_with_indent("%s = %r", option, value)
