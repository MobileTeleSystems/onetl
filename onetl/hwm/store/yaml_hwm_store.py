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

import operator
import re
from typing import ClassVar

import yaml
from etl_entities import HWM, HWMTypeRegistry
from platformdirs import user_data_dir
from pydantic import validator

from onetl.hwm.store.base_hwm_store import BaseHWMStore
from onetl.hwm.store.hwm_store_class_registry import (
    default_hwm_store_class,
    register_hwm_store_class,
)
from onetl.impl import FrozenModel, LocalPath

DATA_PATH = LocalPath(user_data_dir("onETL", "ONEtools"))


@default_hwm_store_class
@register_hwm_store_class("yaml", "yml")
class YAMLHWMStore(BaseHWMStore, FrozenModel):
    r"""YAML local store for HWM values. Used as default HWM store.

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
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy
        from onetl.hwm.store import YAMLHWMStore

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="myuser",
            password="*****",
            database="target_database",
            spark=spark,
        )

        hive = Hive(cluster="rnd-dwh", spark=spark)

        reader = DBReader(
            connection=postgres,
            source="public.mydata",
            columns=["id", "data"],
            hwm_column="id",
        )

        writer = DBWriter(connection=hive, target="newtable")

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

    def get(self, name: str) -> HWM | None:
        data = self._load(name)

        if not data:
            return None

        latest = sorted(data, key=operator.itemgetter("modified_time"))[-1]
        return HWMTypeRegistry.parse(latest)

    def save(self, hwm: HWM) -> LocalPath:
        data = self._load(hwm.qualified_name)
        self._dump(hwm.qualified_name, [hwm.serialize()] + data)
        return self.get_file_path(hwm.qualified_name)

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
