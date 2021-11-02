from __future__ import annotations

import pickle  # noqa: S403
from typing import AnyStr
import yaml
import base64

from platformdirs import user_data_dir
from dataclasses import dataclass
from pathlib import Path

from onetl.strategy.hwm import HWM
from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import default_hwm_store_class, register_hwm_store_class

DATA_PATH = Path(user_data_dir("onETL", "oneTools"))


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

    Default params

    .. code:: python

        from onetl.connection import Postgres, Hive
        from onetl.reader import DBReader
        from onetl.strategy import IncrementStrategy
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
            table="default.mydata",
            columns=["id", "data"],
            hwm_column="id",
        )

        writer = DBWriter(hive, "newtable")

        with YAMLHWMStore():
            with IncrementStrategy():
                df = reader.run()
                writer.run(df)

        # will create file "~/.local/share/onETL/yml_hwm_store/default.mydata.id.yml" with encoding="utf-8"
        # and store here a serialized HWM value like:

        # "value": 1000

    With all options

    .. code:: python

        with YAMLHWMStore(path="/my/store", encoding="utf-8"):
            with IncrementStrategy():
                df = reader.run()
                writer.run(df)

        # will create file "/my/store/default.mydata.id.yml" with encoding="utf-8"

    """

    path: Path = DATA_PATH / "yml_hwm_store"
    encoding: str = "utf-8"

    def __post_init__(self):
        self.path = Path(self.path).expanduser().absolute()  # noqa: WPS601
        self.path.mkdir(parents=True, exist_ok=True)

    def get(self, name: str) -> HWM | None:
        data = self._load(name)

        if "value" in data:
            decoded = base64.b64decode(data["value"])
            return pickle.loads(decoded)  # noqa: S301

        return None

    def save(self, hwm: HWM) -> None:
        pickled = pickle.dumps(hwm)
        encoded = base64.b64encode(pickled).decode(self.encoding)

        data = {"value": encoded}

        self._dump(str(hwm), data)

    def _load(self, name: str) -> dict[str, AnyStr]:
        path = self.path / f"{name}.yml"
        if not path.exists():
            return {}

        with path.open("r", encoding=self.encoding) as file:
            return yaml.safe_load(file)

    def _dump(self, name: str, data: dict[str, AnyStr]) -> None:
        path = self.path / f"{name}.yml"
        with path.open("w", encoding=self.encoding) as file:
            yaml.dump(data, file)
