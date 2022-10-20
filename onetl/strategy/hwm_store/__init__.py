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

from onetl.strategy.hwm_store.atlas_hwm_store import AtlasHWMStore
from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_class_registry import (
    HWMClassRegistry,
    register_hwm_class,
)
from onetl.strategy.hwm_store.hwm_store_class_registry import (
    HWMStoreClassRegistry,
    default_hwm_store_class,
    detect_hwm_store,
    register_hwm_store_class,
)
from onetl.strategy.hwm_store.hwm_store_manager import HWMStoreManager
from onetl.strategy.hwm_store.memory_hwm_store import MemoryHWMStore
from onetl.strategy.hwm_store.yaml_hwm_store import YAMLHWMStore
