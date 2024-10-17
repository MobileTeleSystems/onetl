# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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

    raise AttributeError(name)
