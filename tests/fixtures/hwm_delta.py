import secrets
from datetime import date, datetime, timedelta

import pytest
from etl_entities.hwm import ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM, FileListHWM


@pytest.fixture(
    params=[
        (
            ColumnIntHWM(
                name="some_hwm_name_unique_for_specific_process_and_source",
                column=secrets.token_hex(5),
                value=10,
            ),
            5,
        ),
        (
            ColumnDateHWM(
                name="some_hwm_name_unique_for_specific_process_and_source",
                column=secrets.token_hex(5),
                value=date(year=2023, month=8, day=15),
            ),
            timedelta(days=31),
        ),
        (
            ColumnDateTimeHWM(
                name="some_hwm_name_unique_for_specific_process_and_source",
                column=secrets.token_hex(5),
                value=datetime(year=2023, month=8, day=15, hour=11, minute=22, second=33),
            ),
            timedelta(seconds=50),
        ),
        (
            FileListHWM(
                name="some_hwm_name_unique_for_specific_process_and_source",
                directory=f"/absolute/{secrets.token_hex(5)}",
                value=["some/path", "another.file"],
            ),
            "third.file",
        ),
    ],
)
def hwm_delta(request):
    return request.param
