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

import textwrap
import warnings
from importlib import import_module

new_module = "onetl.hwm.store"

imports_source = {
    "BaseHWMStore": "base_hwm_store",
    "HWMClassRegistry": "hwm_class_registry",
    "register_hwm_class": "hwm_class_registry",
    "HWMStoreClassRegistry": "hwm_store_class_registry",
    "default_hwm_store_class": "hwm_store_class_registry",
    "detect_hwm_store": "hwm_store_class_registry",
    "register_hwm_store_class": "hwm_store_class_registry",
    "HWMStoreManager": "hwm_store_manager",
    "MemoryHWMStore": "memory_hwm_store",
    "YAMLHWMStore": "yaml_hwm_store",
}

__all__ = list(imports_source.keys())  # noqa: WPS410


def __getattr__(name: str):
    if name not in imports_source:
        raise ImportError(f"cannot import name {name!r} from {__name__!r}")

    submodule = imports_source[name]

    message = f"""
        Imports from module {__name__!r} are deprecated since v0.6.0 and will be removed in v1.0.0.
        Please use instead:

        from {new_module} import {name}
    """

    warnings.warn(textwrap.dedent(message).strip(), category=DeprecationWarning, stacklevel=2)

    return getattr(import_module(f"{new_module}.{submodule}"), name)
