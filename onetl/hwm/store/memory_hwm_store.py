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

from typing import Dict

from etl_entities import HWM
from pydantic import PrivateAttr

from onetl.hooks import slot, support_hooks
from onetl.hwm.store.base_hwm_store import BaseHWMStore
from onetl.hwm.store.hwm_store_class_registry import register_hwm_store_class


@register_hwm_store_class("memory", "in-memory")
@support_hooks
class MemoryHWMStore(BaseHWMStore):
    """In-memory local store for HWM values. |support_hooks|

    .. note::

        This class should be used in tests only, because all saved HWM values
        will be deleted after exiting the context

    Examples
    --------

    .. code:: python

        from onetl.connection import Hive, Postgres
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy
        from onetl.hwm.store import MemoryHWMStore

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

        with MemoryHWMStore():
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

            # will store HWM value in RAM

        # values are lost after exiting the context
    """

    _data: Dict[str, HWM] = PrivateAttr(default_factory=dict)

    @slot
    def get(self, name: str) -> HWM | None:
        return self._data.get(name, None)

    @slot
    def save(self, hwm: HWM) -> None:
        self._data[hwm.qualified_name] = hwm

    @slot
    def clear(self) -> None:
        """
        Clears all stored HWM values. |support_hooks|
        """
        self._data.clear()
