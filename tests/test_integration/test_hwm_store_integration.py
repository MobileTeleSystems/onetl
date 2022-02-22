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
            source=Table(name="abc", db="cde", instance="proto://domain.com"),
            column=Column(name="int"),
            value=10,
        ),
        5,
    ),
    (
        DateHWM(
            source=Table(name="abc", db="cde", instance="proto://domain.com"),
            column=Column(name="date"),
            value=date(year=2022, month=8, day=15),
        ),
        timedelta(days=31),
    ),
    (
        DateTimeHWM(
            source=Table(name="abc", db="cde", instance="proto://domain.com"),
            column=Column(name="datetime"),
            value=datetime(year=2022, month=8, day=15, hour=11, minute=22, second=33),
        ),
        timedelta(seconds=50),
    ),
    (
        FileListHWM(
            source=RemoteFolder(name="/absolute/path", instance="ftp://ftp.server:21"),
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


@pytest.mark.parametrize("hwm, delta", hwm_delta)
def test_hwm_store_integration_yaml_path(tmp_path_factory, hwm, delta):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex()

    store = YAMLHWMStore(path)

    assert store.path == path
    assert path.exists()

    assert not list(path.glob("**/*"))

    store.save(hwm)

    empty = True
    for item in path.glob("**/*"):
        empty = False
        assert item.is_file()
        assert item.suffix == ".yml"

    assert not empty


def test_hwm_store_integration_yaml_path_not_folder(tmp_path_factory):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex()
    path.touch()

    with pytest.raises(OSError):
        YAMLHWMStore(path)


@pytest.mark.parametrize("hwm, delta", hwm_delta)
def test_hwm_store_integration_yaml_path_no_access(tmp_path_factory, hwm, delta):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex()
    path.mkdir()
    path.chmod(000)

    store = YAMLHWMStore(path)

    with pytest.raises(OSError):
        store.save(hwm)


@pytest.mark.parametrize(
    "user, password",
    [
        (None, ATLAS_PASSWORD),  # no user
        (ATLAS_USER, None),  # no password
    ],
)
def test_hwm_store_integration_atlas_wrong_input(user, password):
    with pytest.raises(ValueError):
        AtlasHWMStore(
            url=ATLAS_URL,
            user=user,
            password=password,
        )


@pytest.mark.parametrize(
    "url, user, password",
    [
        ("http://unknown.url", ATLAS_USER, ATLAS_PASSWORD),
        (ATLAS_HOST, ATLAS_USER, ATLAS_PASSWORD),  # closed port
        (f"{ATLAS_HOST}:{ATLAS_PORT}", ATLAS_USER, ATLAS_PASSWORD),  # no schema
        (ATLAS_URL, secrets.token_hex(), ATLAS_PASSWORD),  # wrong user
        (ATLAS_URL, ATLAS_USER, secrets.token_hex()),  # wrong password
    ],
)
@pytest.mark.parametrize("hwm, delta", hwm_delta)
def test_hwm_store_integration_atlas_no_access(url, user, password, hwm, delta):
    store = AtlasHWMStore(
        url=url,
        user=user,
        password=password,
    )

    with pytest.raises(Exception):
        store.save(hwm)


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
