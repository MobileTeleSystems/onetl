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

# TODO: remove in 1.0.0

import textwrap
import warnings
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etl_entities.hwm_store import BaseHWMStore, HWMStoreClassRegistry
    from etl_entities.hwm_store import HWMStoreStackManager as HWMStoreManager
    from etl_entities.hwm_store import (
        MemoryHWMStore,
        detect_hwm_store,
        register_hwm_store_class,
    )

    from onetl.hwm.store import (
        SparkTypeToHWM,
        YAMLHWMStore,
        default_hwm_store_class,
        register_spark_type_to_hwm_type_mapping,
    )

__all__ = [  # noqa: WPS410
    "BaseHWMStore",
    "SparkTypeToHWM",
    "register_spark_type_to_hwm_type_mapping",
    "HWMStoreClassRegistry",
    "default_hwm_store_class",
    "detect_hwm_store",
    "register_hwm_store_class",
    "HWMStoreManager",
    "MemoryHWMStore",
    "YAMLHWMStore",
]


def __getattr__(name: str):
    if name not in __all__:
        raise ImportError(f"cannot import name {name!r} from {__name__!r}")

    message = f"""
        Imports from module {__name__!r} are deprecated since v0.6.0 and will be removed in v1.0.0.
        Please use instead:

        from onetl.hwm.store import {name}
    """

    warnings.warn(textwrap.dedent(message).strip(), category=UserWarning, stacklevel=2)
    return getattr(import_module("onetl.hwm.store"), name)
