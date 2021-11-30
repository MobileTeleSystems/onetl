from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


class BaseStrategy:
    def __enter__(self):
        # hack to avoid circular imports
        from onetl.strategy.strategy_manager import StrategyManager

        log.debug(f"|{self.__class__.__name__}| Entered stack at level {StrategyManager.get_current_level()}")
        StrategyManager.push(self)
        log.info(f"|onETL| Using {self.__class__.__name__} as a strategy")
        self.enter_hook()
        return self

    def __exit__(self, exc_type, _exc_value, _traceback):
        from onetl.strategy.strategy_manager import StrategyManager

        log.debug(f"|{self.__class__.__name__}| Exiting stack at level {StrategyManager.get_current_level()-1}")
        strategy = StrategyManager.pop()

        strategy.exit_hook(failed=bool(exc_type))
        return False

    @property
    def current_value(self) -> Any:
        return None  # noqa: WPS324

    @property
    def next_value(self) -> Any:
        return None  # noqa: WPS324

    def enter_hook(self) -> None:
        pass  # noqa: WPS420

    def exit_hook(self, failed: bool = False) -> None:
        pass  # noqa: WPS420
