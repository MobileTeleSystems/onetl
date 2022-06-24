import os
import secrets
import tempfile
from datetime import date, datetime, timedelta

import pytest
from etl_entities import (
    Column,
    DateHWM,
    DateTimeHWM,
    FileListHWM,
    IntHWM,
    RemoteFolder,
    Table,
)

from onetl.connection import Postgres
from onetl.core import DBReader
from onetl.strategy import (
    AtlasHWMStore,
    IncrementalStrategy,
    MemoryHWMStore,
    YAMLHWMStore,
)

ATLAS_HOST = os.environ.get("ONETL_ATLAS_CONN_HOST")
ATLAS_PORT = os.environ.get("ONETL_ATLAS_CONN_PORT")
ATLAS_URL = f"http://{ATLAS_HOST}:{ATLAS_PORT}"
ATLAS_USER = os.environ.get("ONETL_ATLAS_CONN_USER")
ATLAS_PASSWORD = os.environ.get("ONETL_ATLAS_CONN_PASSWORD")


hwm_store = [
    MemoryHWMStore(),
    YAMLHWMStore(path=tempfile.mktemp("hwmstore")),  # noqa: S306 NOSONAR
    AtlasHWMStore(
        url=ATLAS_URL,
        user=ATLAS_USER,
        password=ATLAS_PASSWORD,
    ),
]

hwm_delta = [
    (
        IntHWM(
            source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
            column=Column(name=secrets.token_hex(5)),
            value=10,
        ),
        5,
    ),
    (
        DateHWM(
            source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
            column=Column(name=secrets.token_hex(5)),
            value=date(year=2022, month=8, day=15),
        ),
        timedelta(days=31),
    ),
    (
        DateTimeHWM(
            source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
            column=Column(name=secrets.token_hex(5)),
            value=datetime(year=2022, month=8, day=15, hour=11, minute=22, second=33),
        ),
        timedelta(seconds=50),
    ),
    (
        FileListHWM(
            source=RemoteFolder(name=f"/absolute/{secrets.token_hex(5)}", instance="ftp://ftp.server:21"),
            value=["some/path", "another.file"],
        ),
        "third.file",
    ),
]


@pytest.mark.parametrize("hwm_store", hwm_store)
@pytest.mark.parametrize("hwm, delta", hwm_delta)
def test_hwm_store_integration(hwm_store, hwm, delta):
    assert hwm_store.get(hwm.qualified_name) is None

    hwm_store.save(hwm)
    assert hwm_store.get(hwm.qualified_name) == hwm

    hwm2 = hwm + delta
    hwm_store.save(hwm2)
    assert hwm_store.get(hwm.qualified_name) == hwm2


@pytest.mark.parametrize("hwm_store", hwm_store)
def test_postgres_hwm_store_integration_with_reader(spark, processing, prepare_schema_table, hwm_store):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_column = "hwm_int"
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    # there is a span
    span_length = 100

    # 0..100
    span_begin = 0
    span_end = span_begin + span_length

    span = processing.create_pandas_df(min_id=span_begin, max_id=span_end)

    # insert span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=span,
    )

    with hwm_store:
        # incremental run
        with IncrementalStrategy() as strategy:
            reader.run()
            strategy_hwm = strategy.hwm

        # HWM value was saved into the storage
        saved_hwm = hwm_store.get(strategy_hwm.qualified_name)

        assert saved_hwm.value == span[hwm_column].max()
