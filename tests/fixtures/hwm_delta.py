import secrets
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


@pytest.fixture(
    params=[
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
                value=date(year=2023, month=8, day=15),
            ),
            timedelta(days=31),
        ),
        (
            DateTimeHWM(
                source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
                column=Column(name=secrets.token_hex(5)),
                value=datetime(year=2023, month=8, day=15, hour=11, minute=22, second=33),
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
    ],
)
def hwm_delta(request):
    return request.param
