from __future__ import annotations

from dataclasses import InitVar, dataclass, field

from etl_entities import HWM
from mts_apache_atlas import MTSAtlasClient

from onetl.strategy.hwm_store.base_hwm_store import BaseHWMStore
from onetl.strategy.hwm_store.hwm_store_class_registry import register_hwm_store_class


@register_hwm_store_class("atlas")
@dataclass
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
        from onetl.strategy import AtlasHWMStore, IncrementalStrategy

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

        with AtlasHWMStore(url="http://atlas.domain", user="atlas_user", password="*****"):
            with IncrementalStrategy():
                df = reader.run()
                writer.run(df)

        # will store HWM value in Atlas

    """

    url: InitVar[str]
    user: InitVar[str | None] = field(default=None)
    password: InitVar[str | None] = field(default=None)
    _client: MTSAtlasClient = field(init=False)

    def __post_init__(self, url, user, password):
        auth = None
        if user or password:
            if user and password:
                auth = (user, password)
            else:
                raise ValueError(
                    f"You can pass to {self.__class__.__name__} only both user and password or none of them",
                )

        self._client = MTSAtlasClient(url, auth)  # noqa: WPS601

    def get(self, name: str) -> HWM | None:
        result = self._client.get_hwm(name)

        if result is not None:
            return result

        return None

    def save(self, hwm: HWM) -> None:
        self._client.set_hwm(hwm)
