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

from etl_entities.hwm import HWM, ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM


class SparkTypeToHWM:
    """Registry class for HWM types

    Examples
    --------

    .. code:: python

        from etl_entities.hwm import ColumnIntHWM, ColumnDateHWM
        from onetl.hwm.store import SparkTypeToHWM

        assert SparkTypeToHWM.get("integer") == ColumnIntHWM
        assert SparkTypeToHWM.get("short") == ColumnIntHWM  # multiple type names are supported

        assert SparkTypeToHWM.get("date") == ColumnDateHWM

        assert SparkTypeToHWM.get("unknown") is None

    """

    _mapping: ClassVar[dict[str, type[HWM]]] = {
        "byte": ColumnIntHWM,
        "integer": ColumnIntHWM,
        "short": ColumnIntHWM,
        "long": ColumnIntHWM,
        "date": ColumnDateHWM,
        "timestamp": ColumnDateTimeHWM,
        # for Oracle which does not differ between int and float/double - everything is Decimal
        "float": ColumnIntHWM,
        "double": ColumnIntHWM,
        "fractional": ColumnIntHWM,
        "decimal": ColumnIntHWM,
        "numeric": ColumnIntHWM,
    }

    @classmethod
    def get(cls, type_name: str) -> type[HWM] | None:
        return cls._mapping.get(type_name)

    @classmethod
    def add(cls, type_name: str, klass: type[HWM]) -> None:
        cls._mapping[type_name] = klass


def register_spark_type_to_hwm_type_mapping(*type_names: str):
    """Decorator for registering some HWM class with a type name or names

    Examples
    --------

    .. code:: python

        from etl_entities import HWM
        from onetl.hwm.store import SparkTypeToHWM
        from onetl.hwm.store import SparkTypeToHWM, register_spark_type_to_hwm_type_mapping


        @register_spark_type_to_hwm_type_mapping("somename", "anothername")
        class MyHWM(HWM):
            ...


        assert SparkTypeToHWM.get("somename") == MyClass
        assert SparkTypeToHWM.get("anothername") == MyClass

    """

    def wrapper(cls: type[HWM]):
        for type_name in type_names:
            SparkTypeToHWM.add(type_name, cls)
        return cls

    return wrapper
