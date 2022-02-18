from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import HWMStoreClassRegistry


@dataclass
class HWMStoreManager:
    _stack: ClassVar[list[BaseHWMStore]] = []

    @classmethod
    def push(cls, hwm_store: BaseHWMStore) -> None:
        cls._stack.append(hwm_store)

    @classmethod
    def pop(cls) -> BaseHWMStore:
        return cls._stack.pop()

    @classmethod
    def get_current_level(cls) -> int:
        return len(cls._stack)

    @classmethod
    def get_current(cls) -> BaseHWMStore:
        if cls._stack:
            return cls._stack[-1]

        default_store_type = HWMStoreClassRegistry.get()
        return default_store_type()
