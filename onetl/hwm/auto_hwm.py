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

from typing import Any

from etl_entities.hwm import HWM
from pydantic import root_validator
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
        # e.g. frozendict for KeyValueHWM, there should unifed dict representation
        serialized_data.pop("value")
        return serialized_data
