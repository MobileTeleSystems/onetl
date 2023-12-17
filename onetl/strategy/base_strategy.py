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

from onetl.hwm import Edge
from onetl.impl import BaseModel
from onetl.log import log_with_indent

log = logging.getLogger(__name__)


class BaseStrategy(BaseModel):
    def __enter__(self):
        # hack to avoid circular imports
        from onetl.strategy.strategy_manager import StrategyManager

        log.debug("|%s| Entered stack at level %d", self.__class__.__name__, StrategyManager.get_current_level())
        StrategyManager.push(self)

        self._log_parameters()
        self.enter_hook()
        return self

    def __exit__(self, exc_type, _exc_value, _traceback):
        from onetl.strategy.strategy_manager import StrategyManager

        log.debug("|%s| Exiting stack at level %d", self.__class__.__name__, StrategyManager.get_current_level() - 1)
        strategy = StrategyManager.pop()

        failed = bool(exc_type)
        if failed:
            log.warning("|onETL| Exiting %s because of %s", self.__class__.__name__, exc_type.__name__)
        else:
            log.info("|onETL| Exiting %s", self.__class__.__name__)

        strategy.exit_hook(failed=failed)
        return False

    @property
    def current(self) -> Edge:
        return Edge()

    @property
    def next(self) -> Edge:
        return Edge()

    def enter_hook(self) -> None:
        pass

    def exit_hook(self, failed: bool = False) -> None:
        pass

    def _log_parameters(self) -> None:
        log.info("|onETL| Using %s as a strategy", self.__class__.__name__)
        parameters = self.dict(by_alias=True, exclude_none=True, exclude=self._log_exclude_fields())
        for attr, value in sorted(parameters.items()):
            log_with_indent(log, "%s = %r", attr, value)

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        return set()
