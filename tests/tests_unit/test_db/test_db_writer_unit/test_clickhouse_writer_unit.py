import pytest

from onetl.connection import Clickhouse
from onetl.db import DBWriter

pytestmark = pytest.mark.clickhouse


def test_clickhouse_writer_wrong_table_name(spark_mock):
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=clickhouse,
            target="table",  # Required format: target="schema.table"
        )
