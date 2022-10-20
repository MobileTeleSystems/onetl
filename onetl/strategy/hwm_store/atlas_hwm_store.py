#  Copyright 2022 MTS (Mobile Telesystems)
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

from typing import Optional

from etl_entities import HWM
from mts_apache_atlas import MTSAtlasClient
from pydantic import PrivateAttr, SecretStr

from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import register_hwm_store_class


@register_hwm_store_class("atlas")
class AtlasHWMStore(BaseHWMStore):
    """Atlas remote store for HWM values

    Parameters
    -----------

    url : str

        Atlas instance URL

    user : str, optional

        User for Basic Auth or ``None`` if no auth is required

    password : str, optional

        Password for Basic Auth or ``None`` if no auth is required

    Examples
    --------

    .. code:: python

        from onetl.connection import Hive, Postgres
        from onetl.core import DBReader
        from onetl.strategy import IncrementalStrategy
        from onetl.strategy.hwm_store import AtlasHWMStore

        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        postgres = Postgres(
            host="postgres.domain.com",
            user="myuser",
            password="*****",
            database="target_database",
            spark=spark,
        )

        hive = Hive(spark=spark)

        reader = DBReader(
            connection=postgres,
            table="public.mydata",
            columns=["id", "data"],
            hwm_column="id",
        )

        writer = DBWriter(connection=hive, table="newtable")

        with AtlasHWMStore(url="http://atlas.domain", user="atlas_user", password="*****"):
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will store HWM value in Atlas

    """

    class Config:
        frozen = True

    url: str
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    _client: MTSAtlasClient = PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        auth = None
        if self.user or self.password:
            if self.user and self.password:
                auth = (self.user, self.password.get_secret_value())
            else:
                raise ValueError(
                    f"You can pass to {self.__class__.__name__} only both `user` and `password`, or none of them",
                )

        self._client = MTSAtlasClient(self.url, auth)  # noqa: WPS601

    def get(self, name: str) -> HWM | None:
        return self._client.get_hwm(name)

    def save(self, hwm: HWM) -> str:
        guid = self._client.set_hwm(hwm)
        return f"{self.url}/index.html#!/detailPage/{guid}"
