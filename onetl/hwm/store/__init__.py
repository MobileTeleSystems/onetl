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
import textwrap
import warnings
from importlib import import_module

from onetl.hwm.store.hwm_class_registry import (
    SparkTypeToHWM,
    register_spark_type_to_hwm_type_mapping,
)
from onetl.hwm.store.yaml_hwm_store import YAMLHWMStore, default_hwm_store_class

deprecated_imports = {
    "MemoryHWMStore",
    "BaseHWMStore",
    "HWMStoreClassRegistry",
    "HWMStoreManager",
    "detect_hwm_store",
    "register_hwm_store_class",
}


def __getattr__(name: str):
    if name in deprecated_imports:
        msg = f"""
        This import is deprecated since v0.10.0:

            from onetl.hwm.store import {name}

        Please use instead:

            from etl_entities.hwm_store import {name}
        """

        warnings.warn(
            textwrap.dedent(msg),
            UserWarning,
            stacklevel=2,
        )

        if name == "HWMStoreManager":
            from etl_entities.hwm_store import HWMStoreStackManager

            return HWMStoreStackManager

        return getattr(import_module("etl_entities.hwm_store"), name)

    raise ImportError(f"cannot import name {name!r} from {__name__!r}")
