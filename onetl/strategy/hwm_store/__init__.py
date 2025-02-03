# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
# TODO: remove in 1.0.0

import textwrap
import warnings
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etl_entities.hwm_store import (
        BaseHWMStore,
        HWMStoreClassRegistry,
    )
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

__all__ = [
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
        raise AttributeError(name)

    message = f"""
        Imports from module {__name__!r} are deprecated since v0.6.0 and will be removed in v1.0.0.
        Please use instead:

        from onetl.hwm.store import {name}
    """

    warnings.warn(textwrap.dedent(message).strip(), category=UserWarning, stacklevel=2)
    return getattr(import_module("onetl.hwm.store"), name)
