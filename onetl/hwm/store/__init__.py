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
import warnings

from etl_entities.hwm_store import (
    BaseHWMStore,
    HWMStoreClassRegistry,
    HWMStoreStackManager,
    detect_hwm_store,
    register_hwm_store_class,
)

from onetl.hwm.store.hwm_class_registry import HWMClassRegistry, register_hwm_class
from onetl.hwm.store.yaml_hwm_store import YAMLHWMStore, default_hwm_store_class


def __getattr__(name):
    if name == "MemoryHWMStore":
        warnings.warn(
            "Deprecation warning: 'onetl.hwm.store.MemoryHWMStore' is deprecated "
            "and will be removed in future versions. "
            "Please use 'etl_entities.hwm_store.MemoryHWMStore' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        from etl_entities.hwm_store import MemoryHWMStore

        return MemoryHWMStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
