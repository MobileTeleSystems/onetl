from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    from onetl.strategy.hwm.hwm import HWM


class HWMClassRegistry:
    """Registry class for HWM types

    Examples
    --------

    .. code:: python

        from onetl.strategy.hwm import HWMClassRegistry, IntHWM, FloatHWM

        HWMClassRegistry.get("int") == IntHWM
        HWMClassRegistry.get("integer") == IntHWM  # multiple type names are supported

        HWMClassRegistry.get("float") == FloatHWM

        HWMClassRegistry.get("unknown")  # raise KeyError

    """

    _mapping: ClassVar[dict[str, type[HWM]]] = {}

    @classmethod
    def get(cls, type_name: str) -> type[HWM]:
        result = cls._mapping.get(type_name)
        if not result:
            raise KeyError(f"Unknown HWM type {type_name}")

        return result

    @classmethod
    def add(cls, type_name: str, klass: type[HWM]) -> None:
        cls._mapping[type_name] = klass


def register_hwm_class(*type_names: str):
    """Decorator for registering some HWM class with a type name or names

    Examples
    --------

    .. code:: python

        from onetl.strategy.hwm import HWMClassRegistry, register_hwm_class, HWM


        @register_hwm_class("somename", "anothername")
        class MyHWM(HWM):
            ...


        HWMClassRegistry.get("somename") == MyClass
        HWMClassRegistry.get("anothername") == MyClass

    """

    def wrapper(klass: type[HWM]):
        for type_name in type_names:
            HWMClassRegistry.add(type_name, klass)

        return klass

    return wrapper
