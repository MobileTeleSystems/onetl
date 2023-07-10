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

from functools import wraps
from typing import Any, Callable, ClassVar, Collection, Mapping

from onetl.hwm.store.base_hwm_store import BaseHWMStore


class HWMStoreClassRegistry:
    """Registry class of different HWM stores.

    Examples
    --------

    .. code:: python

        from onetl.hwm.store import HWMStoreClassRegistry, YAMLHWMStore, MemoryHWMStore

        HWMStoreClassRegistry.get("yml") == YAMLHWMStore
        HWMStoreClassRegistry.get("memory") == MemoryHWMStore

        HWMStoreClassRegistry.get() == YAMLHWMStore  # default

        HWMStoreClassRegistry.get("unknown")  # raise KeyError

    """

    _default: type[BaseHWMStore | None] = type(None)
    _mapping: ClassVar[dict[str, type[BaseHWMStore]]] = {}

    @classmethod
    def get(cls, type_name: str | None = None) -> type:
        if not type_name:
            return cls._default

        result = cls._mapping.get(type_name)
        if not result:
            raise KeyError(f"Unknown HWM Store type {type_name!r}")

        return result

    @classmethod
    def add(cls, type_name: str, klass: type[BaseHWMStore]) -> None:
        assert isinstance(type_name, str)  # noqa: S101
        assert issubclass(klass, BaseHWMStore)  # noqa: S101

        cls._mapping[type_name] = klass

    @classmethod
    def set_default(cls, klass: type[BaseHWMStore]) -> None:
        cls._default = klass

    @classmethod
    def known_types(cls) -> Collection[str]:
        return cls._mapping.keys()


def default_hwm_store_class(klass: type[BaseHWMStore]) -> type[BaseHWMStore]:
    """Decorator for setting up some Store class as default one

    Examples
    --------

    .. code:: python

        from onetl.hwm.store import (
            HWMStoreClassRegistry,
            default_hwm_store_class,
            BaseHWMStore,
        )


        @default_hwm_store_class
        class MyClass(BaseHWMStore):
            ...


        HWMStoreClassRegistry.get() == MyClass  # default

    """

    HWMStoreClassRegistry.set_default(klass)
    return klass


def register_hwm_store_class(*type_names: str):
    """Decorator for registering some Store class with a name

    Examples
    --------

    .. code:: python

        from onetl.hwm.store import (
            HWMStoreClassRegistry,
            register_hwm_store_class,
            BaseHWMStore,
        )


        @register_hwm_store_class("somename")
        class MyClass(BaseHWMStore):
            ...


        HWMStoreClassRegistry.get("somename") == MyClass

    """

    def wrapper(cls: type[BaseHWMStore]):
        for type_name in type_names:
            HWMStoreClassRegistry.add(type_name, cls)

        return cls

    return wrapper


def parse_config(value: Any, key: str) -> tuple[str, list, Mapping]:
    if not isinstance(value, (str, Mapping)):
        raise ValueError(f"Wrong value {value!r} for {key!r} config item")

    store_type = "unknown"
    args: list[Any] = []
    kwargs: Mapping[str, Any] = {}

    if isinstance(value, str):
        return value, args, kwargs

    for item in HWMStoreClassRegistry.known_types():
        if item not in value:
            continue

        store_type = item
        child = value[item]

        args, kwargs = parse_child_item(child)

    return store_type, args, kwargs


def parse_child_item(child: Any) -> tuple[list, Mapping]:
    store_args: list[Any] = []
    store_kwargs: Mapping[str, Any] = {}

    if not child:
        return store_args, store_kwargs

    if isinstance(child, str):
        store_args = [child]
    elif isinstance(child, Mapping):
        store_kwargs = child
    else:
        store_args = child

    return store_args, store_kwargs


def dict_item_getter(key: str) -> Callable:
    def wrapper(conf):  # noqa: WPS430
        return resolve_attr(conf, key)

    return wrapper


def resolve_attr(conf: Mapping, hwm_key: str) -> str | Mapping:
    obj = {}

    try:
        if "." not in hwm_key:
            obj = conf[hwm_key]
        else:
            for name in hwm_key.split("."):
                obj = conf[name]
                conf = obj
    except Exception as e:
        raise ValueError("The configuration does not contain a required key") from e

    return obj


def detect_hwm_store(key: str) -> Callable:
    """Detect HWM store by config object

    Parameters
    ----------
    key : str
        The name of the section in the config that stores information about hwm

        .. warning ::

            **DO NOT** use dot ``.`` in config keys

    Examples
    --------

    Config

    .. code:: yaml

        # if HWM store can be created with no args
        hwm_store: yaml

    or

    .. code:: yaml

        # named constructor args
        hwm_store:
            atlas:
                url: http://some.atlas.url
                user: username
                password: password

    Config could be nested:

    .. code:: yaml

        myetl:
            env:
                hwm_store: yaml

    ``run.py``

    .. code:: python

        import hydra
        from omegaconf import DictConfig
        from onetl.hwm.store import detect_hwm_store


        # key=... is a path to config item, delimited by dot ``.``
        @hydra.main(config="../conf")
        @detect_hwm_store(key="myetl.env.hwm_store")
        def main(config: DictConfig):
            pass

    """

    if not isinstance(key, str):
        raise ValueError("key name must be a string")

    def pre_wrapper(func: Callable):  # noqa: WPS430
        @wraps(func)
        def wrapper(config: Mapping, *args, **kwargs):
            if not config:
                raise ValueError("Config must be specified")

            if not key:
                raise ValueError("Key value must be specified")

            get_hwm_spec = dict_item_getter(key)
            root = get_hwm_spec(config)

            if not root:
                return func(config, *args, **kwargs)

            store_type, store_args, store_kwargs = parse_config(root, key)
            store = HWMStoreClassRegistry.get(store_type)

            with store(*store_args, **store_kwargs):
                return func(config, *args, **kwargs)

        return wrapper

    return pre_wrapper
