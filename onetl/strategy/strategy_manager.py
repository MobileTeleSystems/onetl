from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar

from onetl.strategy.base_strategy import BaseStrategy
from onetl.strategy.snapshot_strategy import SnapshotStrategy

log = logging.getLogger(__name__)


@dataclass
class StrategyManager:
    default_strategy: ClassVar[type] = SnapshotStrategy

    _stack: ClassVar[list[BaseStrategy]] = []

    @classmethod
    def push(cls, strategy: BaseStrategy) -> None:
        cls._stack.append(strategy)

    @classmethod
    def pop(cls) -> BaseStrategy:
        return cls._stack.pop()

    @classmethod
    def get_current_level(cls) -> int:
        return len(cls._stack)

    @classmethod
    def get_current(cls) -> BaseStrategy:
        if cls._stack:
            return cls._stack[-1]

        return cls.default_strategy()
