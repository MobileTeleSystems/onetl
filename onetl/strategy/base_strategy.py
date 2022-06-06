from __future__ import annotations

import logging
from typing import Any

from onetl.log import LOG_INDENT

log = logging.getLogger(__name__)


class BaseStrategy:
    def __enter__(self):

        # hack to avoid circular imports
        from onetl.strategy.strategy_manager import StrategyManager

        log.debug(f"|{self.__class__.__name__}| Entered stack at level {StrategyManager.get_current_level()}")
        StrategyManager.push(self)

        log.info(f"|onETL| Using {self.__class__.__name__} as a strategy")
        options = {key: value for key, value in vars(self).items() if not self._log_exclude_field(key)}

        if options:
            log.info(f"|{self.__class__.__name__}| Using options:")
            for option, value in options.items():
                log.info(LOG_INDENT + f"{option} = {value}")

        self.enter_hook()
        return self

    def __exit__(self, exc_type, _exc_value, _traceback):
        from onetl.strategy.strategy_manager import StrategyManager

        log.debug(f"|{self.__class__.__name__}| Exiting stack at level {StrategyManager.get_current_level()-1}")
        strategy = StrategyManager.pop()

        failed = bool(exc_type)
        reason = f" because of {exc_type.__name__}" if failed else ""
        log.info(f"|onETL| Exiting {self.__class__.__name__}{reason}")

        strategy.exit_hook(failed=failed)
        return False

    @property  # noqa: WPS324
    def current_value(self) -> Any:
        return None  # noqa: WPS324

    @property  # noqa: WPS324
    def next_value(self) -> Any:
        return None  # noqa: WPS324

    def enter_hook(self) -> None:
        pass  # noqa: WPS420

    def exit_hook(self, failed: bool = False) -> None:
        pass  # noqa: WPS420

    @classmethod
    def _log_exclude_field(cls, name: str) -> bool:
        return name.startswith("_")
