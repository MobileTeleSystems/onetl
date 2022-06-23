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

from onetl.strategy import AtlasHWMStore, MemoryHWMStore, YAMLHWMStore

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


@pytest.mark.parametrize(
    "url, user, password",
    [
        ("http://unknown.url", ATLAS_USER, ATLAS_PASSWORD),
        (ATLAS_HOST, ATLAS_USER, ATLAS_PASSWORD),  # closed port
        (f"{ATLAS_HOST}:{ATLAS_PORT}", ATLAS_USER, ATLAS_PASSWORD),  # no schema
        (ATLAS_URL, secrets.token_hex(5), ATLAS_PASSWORD),  # wrong user
        (ATLAS_URL, ATLAS_USER, secrets.token_hex(5)),  # wrong password
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
