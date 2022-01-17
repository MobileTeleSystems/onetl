from __future__ import annotations

from functools import wraps

from typing import Any, Callable, ClassVar, Collection, Mapping

from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore

CONFIG_ROOT_KEY = "hwm_store"


class HWMStoreClassRegistry:
    """Registry class of different HWM stores

    Examples
    --------

    .. code:: python

        from onetl.strategy.hwm_store import HWMStoreClassRegistry, YAMLHWMStore, MemoryHWMStore

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
            raise KeyError(f"Unknown HWM Store type {type_name}")

        return result

    @classmethod
    def add(cls, type_name: str, klass: type[BaseHWMStore]) -> None:
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

        from onetl.strategy.hwm_store import (
            HWMStoreClassRegistry,
            default_hwm_store_class,
            BaseStore,
        )


        @default_hwm_store_class
        class MyClass(BaseStore):
            ...


        HWMStoreClassRegistry.get() == MyClass  # default

    """

    HWMStoreClassRegistry.set_default(klass)
    return klass


def register_hwm_store_class(*type_names: str):
    """Decorator for registering some Store class with a name or names

    Examples
    --------

    .. code:: python

        from onetl.strategy.hwm_store import (
            HWMStoreClassRegistry,
            register_hwm_store_class,
            BaseStore,
        )


        @register_hwm_store_class("somename", "anothername")
        class MyClass(BaseStore):
            ...


        HWMStoreClassRegistry.get("somename") == MyClass
        HWMStoreClassRegistry.get("anothername") == MyClass

    """

    def wrapper(cls: type[BaseHWMStore]):
        for type_name in type_names:
            HWMStoreClassRegistry.add(type_name, cls)

        return cls

    return wrapper


def get_root(config: Mapping) -> Any:
    if CONFIG_ROOT_KEY in config:
        return config.get(CONFIG_ROOT_KEY)

    for _key, value in config.items():
        if isinstance(value, Mapping):
            result = get_root(value)
            if result:
                return result

    return None


def parse_config(value: Any) -> tuple[str, list, Mapping]:
    if not isinstance(value, (str, Mapping)):
        raise ValueError(f"Wrong value {value} for `hwm_store` config item")

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


def detect_hwm_store(func: Callable) -> Callable:
    """Detect HWM store by config object

    Examples
    --------

    Config

    .. code:: yaml

        # no constructor args
        hwm_store: yaml

    or

    .. code:: yaml

        # one constructor args
        hwm_store:
            yaml: /some/path.yml

    or

    .. code:: yaml

        # positional constructor args
        hwm_store:
            atlas:
            - http://some.atlas.url
            - username
            - password

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

    .. code::

        @hydra.main(config="../conf")
        @detect_hwm_store
        def main(config: OmniConf):
            pass

    """

    @wraps(func)
    def wrapper(config: Mapping, *args, **kwargs):
        root = get_root(config)

        if not root:
            return func(config, *args, **kwargs)

        store_type, store_args, store_kwargs = parse_config(root)
        store = HWMStoreClassRegistry.get(store_type)

        with store(*store_args, **store_kwargs):
            return func(config, *args, **kwargs)

    return wrapper
