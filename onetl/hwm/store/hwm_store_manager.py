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

from typing import ClassVar

from onetl.hwm.store.base_hwm_store import BaseHWMStore
from onetl.hwm.store.hwm_store_class_registry import HWMStoreClassRegistry


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
