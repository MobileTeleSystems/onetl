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

from typing import Any, Callable, ClassVar, Iterator, Optional

from etl_entities import HWM, DateHWM, DateTimeHWM, FileListHWM, IntHWM
from pydantic import StrictInt


class HWMClassRegistry:
    """Registry class for HWM types

    Examples
    --------

    .. code:: python

        from etl_entities import IntHWM, DateHWM
        from onetl.hwm.store import HWMClassRegistry

        HWMClassRegistry.get("int") == IntHWM
        HWMClassRegistry.get("integer") == IntHWM  # multiple type names are supported

        HWMClassRegistry.get("date") == DateHWM

        HWMClassRegistry.get("unknown")  # raise KeyError

    """

    _mapping: ClassVar[dict[str, type[HWM]]] = {
        "byte": IntHWM,
        "integer": IntHWM,
        "short": IntHWM,
        "long": IntHWM,
        "date": DateHWM,
        "timestamp": DateTimeHWM,
        "file_list": FileListHWM,
    }

    @classmethod
    def get(cls, type_name: str) -> type[HWM]:
        result = cls._mapping.get(type_name)
        if not result:
            raise KeyError(f"Unknown HWM type {type_name!r}")

        return result

    @classmethod
    def add(cls, type_name: str, klass: type[HWM]) -> None:
        cls._mapping[type_name] = klass


def register_hwm_class(*type_names: str):
    """Decorator for registering some HWM class with a type name or names

    Examples
    --------

    .. code:: python

        from etl_entities import HWM
        from onetl.hwm.store import HWMClassRegistry
        from onetl.hwm.store import HWMClassRegistry, register_hwm_class


        @register_hwm_class("somename", "anothername")
        class MyHWM(HWM):
            ...


        HWMClassRegistry.get("somename") == MyClass
        HWMClassRegistry.get("anothername") == MyClass

    """

    def wrapper(cls: type[HWM]):
        for type_name in type_names:
            HWMClassRegistry.add(type_name, cls)

        return cls

    return wrapper


class Decimal(StrictInt):
    @classmethod
    def __get_validators__(cls) -> Iterator[Callable]:
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> int:
        if round(float(value)) != float(value):
            raise ValueError(f"{cls.__name__} cannot have fraction part")
        return int(value)


@register_hwm_class("float", "double", "fractional", "decimal", "numeric")
class DecimalHWM(IntHWM):
    """Same as IntHWM, but allows to pass values like 123.000 (float without fractional part)"""

    value: Optional[Decimal] = None
