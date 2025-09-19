import pytest

from onetl.db import DBWriter

pytestmark = pytest.mark.iceberg


def test_iceberg_writer_wrong_table_name(iceberg_mock):
    with pytest.raises(
        ValueError,
        match="Name should be passed in `schema.name` format",
    ):
        DBWriter(
            connection=iceberg_mock,
            target="table",  # Required format: target="schema.table"
        )
