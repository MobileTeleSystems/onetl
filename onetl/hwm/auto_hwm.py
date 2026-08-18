# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from etl_entities.hwm import HWM


class AutoDetectHWM(HWM):
    value: None = None

    def __init__(self, **kwargs):
        # 'column' is an alias used specifically for instances of the ColumnHWM class.
        if "source" in kwargs and "entity" not in kwargs:
            kwargs["entity"] = kwargs.pop("source")

        # 'topic' is an alias used for instances of the KeyValueHWM class.
        elif "topic" in kwargs and "entity" not in kwargs:
            kwargs["entity"] = kwargs.pop("topic")

        super().__init__(**kwargs)

    def update(self, value: Any) -> "AutoDetectHWM":
        """Update current HWM value with some implementation-specific logic, and return HWM"""
        msg = "update method should be implemented in auto detected subclasses"
        raise NotImplementedError(msg)

    def reset(self) -> "AutoDetectHWM":
        raise NotImplementedError

    # pydantic v1
    def dict(self, **kwargs):
        result = super().dict(**kwargs)
        # as in HWM classes default value for 'value' may be any structure,
        # e.g. frozendict for KeyValueHWM, there should unified dict representation
        result.pop("value", None)
        return result

    # pydantic v2
    def model_dump(self, **kwargs):
        result = super().model_dump(**kwargs)
        # as in HWM classes default value for 'value' may be any structure,
        # e.g. frozendict for KeyValueHWM, there should unified dict representation
        result.pop("value", None)
        return result
