import secrets
from datetime import date, datetime, timedelta

import pytest
from etl_entities.hwm import ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM, FileListHWM


@pytest.fixture(
    params=[
        (
            ColumnIntHWM(
                name=secrets.token_hex(5),
                # no source
                expression=secrets.token_hex(5),
                value=10,
            ),
            5,
        ),
        (
            ColumnIntHWM(
                name=secrets.token_hex(5),
                source=secrets.token_hex(5),
                expression=secrets.token_hex(5),
                value=10,
            ),
            5,
        ),
        (
            ColumnDateHWM(
                name=secrets.token_hex(5),
                source=secrets.token_hex(5),
                expression=secrets.token_hex(5),
                value=date(year=2023, month=8, day=15),
            ),
            timedelta(days=31),
        ),
        (
            ColumnDateTimeHWM(
                name=secrets.token_hex(5),
                source=secrets.token_hex(5),
                expression=secrets.token_hex(5),
                value=datetime(year=2023, month=8, day=15, hour=11, minute=22, second=33),
            ),
            timedelta(seconds=50),
        ),
        (
            FileListHWM(
                name=secrets.token_hex(5),
                # not directory
                value=["/some/path", "/another.file"],
            ),
            "/third.file",
        ),
        (
            FileListHWM(
                name=secrets.token_hex(5),
                directory="/absolute/path",
                value=["/absolute/path/file1", "/absolute/path/file2"],
            ),
            "/absolute/path/file3",
        ),
    ],
)
def hwm_delta(request):
    return request.param
