# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import operator
import re
from typing import ClassVar

import yaml
from etl_entities.hwm import HWM, HWMTypeRegistry
from etl_entities.hwm_store import (
    BaseHWMStore,
    HWMStoreClassRegistry,
    register_hwm_store_class,
)
from platformdirs import user_data_dir

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel, LocalPath

DATA_PATH = LocalPath(user_data_dir("onETL", "ONEtools"))


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
        class MyClass(BaseHWMStore): ...


        HWMStoreClassRegistry.get() == MyClass  # default

    """

    HWMStoreClassRegistry.set_default(klass)
    return klass


@default_hwm_store_class
@register_hwm_store_class("yaml")
@support_hooks
class YAMLHWMStore(BaseHWMStore, FrozenModel):
    r"""YAML **local store** for HWM values. Used as default HWM store. |support_hooks|

    Parameters
    ----------
    path : :obj:`pathlib.Path` or `str`

        Folder name there HWM value files will be stored.

        Default:

        * ``~/.local/share/onETL/yml_hwm_store`` on Linux
        * ``C:\Documents and Settings\<User>\Application Data\oneTools\onETL\yml_hwm_store`` on Windows
        * ``~/Library/Application Support/onETL/yml_hwm_store`` on MacOS

    encoding : str, default: ``utf-8``

        Encoding of files with HWM value

    Examples
    --------

    Default parameters

    .. code:: python

        from onetl.connection import Hive, Postgres
        from onetl.db import DBReader, DBWriter
        from onetl.strategy import IncrementalStrategy
        from onetl.hwm.store import YAMLHWMStore

        postgres = Postgres(...)
        hive = Hive(...)

        reader = DBReader(
            connection=postgres,
            source="public.mydata",
            columns=["id", "data"],
            hwm=DBReader.AutoDetectHWM(name="some_unique_name", expression="id"),
        )

        writer = DBWriter(connection=hive, target="db.newtable")

        with YAMLHWMStore():
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will create file
        # "~/.local/share/onETL/id__public.mydata__postgres_postgres.domain.com_5432__myprocess__myhostname.yml"
        # with encoding="utf-8" and save a serialized HWM values to this file

    With all options

    .. code:: python

        with YAMLHWMStore(path="/my/store", encoding="utf-8"):
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will create file
        # "/my/store/id__public.mydata__postgres_postgres.domain.com_5432__myprocess__myhostname.yml"
        # with encoding="utf-8" and save a serialized HWM values to this file

    File content example:

    .. code:: yaml

        - column:
            name: id
            partition: {}
          modified_time: '2023-02-11T17:10:49.659019'
          process:
              dag: ''
              host: myhostname
              name: myprocess
              task: ''
          source:
              db: public
              instance: postgres://postgres.domain.com:5432/target_database
              name: mydata
          type: int
          value: '1500'
        - column:
              name: id
              partition: {}
          modified_time: '2023-02-11T16:00:31.962150'
          process:
              dag: ''
              host: myhostname
              name: myprocess
              task: ''
          source:
              db: public
              instance: postgres://postgres.domain.com:5432/target_database
              name: mydata
          type: int
          value: '1000'
    """

    class Config:
        frozen = True

    path: LocalPath = DATA_PATH / "yml_hwm_store"
    encoding: str = "utf-8"

    ITEMS_DELIMITER_PATTERN: ClassVar[re.Pattern] = re.compile("[#@|]+")
    PROHIBITED_SYMBOLS_PATTERN: ClassVar[re.Pattern] = re.compile(r"[=:/\\]+")

    @validator("path", pre=True, always=True)
    def validate_path(cls, path):
        path = LocalPath(path).expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    @slot
    def get_hwm(self, name: str) -> HWM | None:  # type: ignore
        data = self._load(name)

        if not data:
            return None

        latest = sorted(data, key=operator.itemgetter("modified_time"))[-1]
        return HWMTypeRegistry.parse(latest)  # type: ignore

    @slot
    def set_hwm(self, hwm: HWM) -> LocalPath:  # type: ignore
        data = self._load(hwm.name)
        self._dump(hwm.name, [hwm.serialize()] + data)
        return self.get_file_path(hwm.name)

    @classmethod
    def cleanup_file_name(cls, name: str) -> str:
        # id|partition=value#db.table@proto://instance#process@host
        # ->
        # id__partition__value__db.table__proto_instance__process__host

        result = cls.ITEMS_DELIMITER_PATTERN.sub("__", name)
        result = cls.PROHIBITED_SYMBOLS_PATTERN.sub("_", result)
        return re.sub("_{2,}", "__", result)

    def get_file_path(self, name: str) -> LocalPath:
        file_name = self.cleanup_file_name(name)
        return self.path / f"{file_name}.yml"

    def _load(self, name: str) -> list[dict]:
        path = self.get_file_path(name)
        if not path.exists():
            return []

        with path.open("r", encoding=self.encoding) as file:
            return yaml.safe_load(file)

    def _dump(self, name: str, data: list[dict]) -> None:
        path = self.get_file_path(name)
        with path.open("w", encoding=self.encoding) as file:
            yaml.dump(data, file)
