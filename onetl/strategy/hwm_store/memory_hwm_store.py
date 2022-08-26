from __future__ import annotations

from typing import Dict

from etl_entities import HWM
from pydantic import PrivateAttr

from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import register_hwm_store_class


@register_hwm_store_class("memory", "in-memory")
class MemoryHWMStore(BaseHWMStore):
    """In-memory local store for HWM values

    .. note::

        This class should be used in tests only, because all saved HWM values
        will be deleted after exiting the context

    Examples
    --------

    .. code:: python

        from onetl.connection import Hive, Postgres
        from onetl.core import DBReader
        from onetl.strategy import MemoryHWMStore, IncrementalStrategy

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

        with MemoryHWMStore():
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will store HWM value in RAM

    """

    _data: Dict[str, HWM] = PrivateAttr(default_factory=dict)

    def get(self, name: str) -> HWM | None:
        return self._data.get(name, None)

    def save(self, hwm: HWM) -> None:
        self._data[hwm.qualified_name] = hwm
