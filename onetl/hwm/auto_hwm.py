# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

from etl_entities.hwm import HWM

try:
    from pydantic.v1 import root_validator
except (ImportError, AttributeError):
    from pydantic import root_validator  # type: ignore[no-redef, assignment]

from typing_extensions import Literal


class AutoDetectHWM(HWM):
    value: Literal[None] = None

    @root_validator(pre=True)
    def handle_aliases(cls, values):
        # this validator is hack for accommodating multiple aliases for a single field in pydantic v1.

        # 'column' is an alias used specifically for instances of the ColumnHWM class.
        if "source" in values and "entity" not in values:
            values["entity"] = values.pop("source")

        # 'topic' is an alias used for instances of the KeyValueHWM class.
        elif "topic" in values and "entity" not in values:
            values["entity"] = values.pop("topic")

        return values

    def update(self: AutoDetectHWM, value: Any) -> AutoDetectHWM:
        """Update current HWM value with some implementation-specific logic, and return HWM"""
        raise NotImplementedError("update method should be implemented in auto detected subclasses")

    def dict(self, **kwargs):
        serialized_data = super().dict(**kwargs)
        # as in HWM classes default value for 'value' may be any structure,
        # e.g. frozendict for KeyValueHWM, there should unified dict representation
        serialized_data.pop("value")
        return serialized_data
