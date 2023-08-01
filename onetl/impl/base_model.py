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

from typing import ClassVar

from pydantic import BaseModel as PydanticBaseModel


class BaseModel(PydanticBaseModel):
    _forward_refs_updated: ClassVar[bool] = False

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        extra = "forbid"
        underscore_attrs_are_private = True

    def __init__(self, **kwargs):
        if not self._forward_refs_updated:
            # if pydantic cannot detect type hints (referenced class is not imported yet),
            # it wraps annotation with ForwardRef(...), which should be resolved before creating the instance.
            # so using a small hack to detect all those refs and update them
            # when first object instance is being created
            refs = self._forward_refs()
            self.__class__.update_forward_refs(**refs)
            self.__class__._forward_refs_updated = True  # noqa: WPS437
        super().__init__(**kwargs)

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        refs: dict[str, type] = {}
        for item in dir(cls):
            if item.startswith("_") or item.startswith("package"):
                continue

            value = getattr(cls, item)
            if isinstance(value, type):
                refs[item] = value

        return refs
