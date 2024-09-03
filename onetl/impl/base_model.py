# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
# isort: skip_file

from __future__ import annotations

from typing import ClassVar

try:
    from pydantic.v1 import BaseModel as PydanticBaseModel
except (ImportError, AttributeError):
    from pydantic import BaseModel as PydanticBaseModel  # type: ignore[no-redef, assignment]


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
