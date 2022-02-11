from __future__ import annotations

import re
from typing import ClassVar
import yaml
import operator

from platformdirs import user_data_dir
from dataclasses import dataclass
from pathlib import Path

from etl_entities import HWM, HWMTypeRegistry
from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import default_hwm_store_class, register_hwm_store_class

DATA_PATH = Path(user_data_dir("onETL", "ONEtools"))


@default_hwm_store_class
@register_hwm_store_class("yaml", "yml")
@dataclass
class YAMLHWMStore(BaseHWMStore):
    r"""YAML local store for HWM values

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

        from onetl.connection import Postgres, Hive
        from onetl.reader import DBReader
        from onetl.strategy import IncrementalStrategy
        from onetl.strategy.hwm_store import YAMLHWMStore

        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="appmetrica_test",
            password="*****",
            database="target_database",
            spark=spark,
        )

        hive = Hive(spark=spark)

        reader = DBReader(
            postgres,
            table="public.mydata",
            columns=["id", "data"],
            hwm_column="id",
        )

        writer = DBWriter(hive, "newtable")

        with YAMLHWMStore():
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will create file
        # "~/.local/share/onETL/id__public.mydata__postgres_test-db-vip.msk.mts.ru_5432__myprocess__myhostname.yml"
        # with encoding="utf-8" and save a serialized HWM values to this file

    With all options

    .. code:: python

        with YAMLHWMStore(path="/my/store", encoding="utf-8"):
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will create file
        # "/my/store/id__public.mydata__postgres_test-db-vip.msk.mts.ru_5432__myprocess__myhostname.yml"
        # with encoding="utf-8" and save a serialized HWM values to this file

    File content example:

    .. code:: yaml

        - column:
            name: id
            partition: {}
          modified_time: '2022-02-11T17:10:49.659019'
          process:
              dag: ''
              host: myhostname
              name: myprocess
              task: ''
          source:
              db: public
              instance: postgres://test-db-vip.msk.mts.ru:5432/target_database
              name: mydata
          type: int
          value: '1500'
        - column:
              name: id
              partition: {}
          modified_time: '2022-02-11T16:00:31.962150'
          process:
              dag: ''
              host: myhostname
              name: myprocess
              task: ''
          source:
              db: public
              instance: postgres://test-db-vip.msk.mts.ru:5432/target_database
              name: mydata
          type: int
          value: '1000'
    """

    path: Path = DATA_PATH / "yml_hwm_store"
    encoding: str = "utf-8"

    ITEMS_DELIMITER_PATTERN: ClassVar[re.Pattern] = re.compile("[#@|]+")
    PROHIBITED_SYMBOLS_PATTERN: ClassVar[re.Pattern] = re.compile(r"[=:/\\]+")

    def __post_init__(self):
        self.path = Path(self.path).expanduser().absolute()  # noqa: WPS601
        self.path.mkdir(parents=True, exist_ok=True)

    def get(self, name: str) -> HWM | None:
        data = self._load(name)

        if not data:
            return None

        latest = sorted(data, key=operator.itemgetter("modified_time"))[-1]
        return HWMTypeRegistry.parse(latest)

    def save(self, hwm: HWM) -> None:
        data = self._load(hwm.qualified_name)
        self._dump(hwm.qualified_name, [hwm.serialize()] + data)

    @classmethod
    def cleanup_file_name(cls, name: str) -> str:
        # id|partition=value#db.table@proto://instance#process@host
        # ->
        # id__partition__value__db.table__proto_instance__process__host

        result = cls.ITEMS_DELIMITER_PATTERN.sub("__", name)
        result = cls.PROHIBITED_SYMBOLS_PATTERN.sub("_", result)
        return re.sub("_{2,}", "__", result)

    def _load(self, name: str) -> list[dict]:
        name = self.cleanup_file_name(name)
        path = self.path / f"{name}.yml"
        if not path.exists():
            return []

        with path.open("r", encoding=self.encoding) as file:
            return yaml.safe_load(file)

    def _dump(self, name: str, data: list[dict]) -> None:
        name = self.cleanup_file_name(name)
        path = self.path / f"{name}.yml"
        with path.open("w", encoding=self.encoding) as file:
            yaml.dump(data, file)
