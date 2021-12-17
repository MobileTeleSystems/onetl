from __future__ import annotations

from dataclasses import dataclass, field

from etl_entities import HWM
from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import register_hwm_store_class


@register_hwm_store_class("memory", "in-memory")
@dataclass
class MemoryHWMStore(BaseHWMStore):
    """In-memory local store for HWM values

    .. note::

        This class should be used in tests only, because all saved HWM values
        will be deleted after exiting the context

    Examples
    --------

    .. code:: python

        from onetl.connection import Postgres, Hive
        from onetl.reader import DBReader
        from onetl.strategy import IncrementStrategy
        from onetl.strategy.hwm_store import MemoryHWMStore

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

        with MemoryHWMStore():
            with IncrementStrategy():
                df = reader.run()
                writer.run(df)

        # will store HWM value in RAM

    """

    _data: dict[str, HWM] = field(init=False, repr=False, default_factory=dict)

    def get(self, name: str) -> HWM | None:
        result = self._data.get(name, None)

        if result is not None:
            return result

        return None

    def save(self, hwm: HWM) -> None:
        self._data[hwm.qualified_name] = hwm
