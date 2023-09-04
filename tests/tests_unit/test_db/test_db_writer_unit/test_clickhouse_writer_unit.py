import pytest

from onetl.connection import Clickhouse
from onetl.db import DBWriter

pytestmark = pytest.mark.clickhouse


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_clickhouse_writer_wrong_table_name(spark_mock, table):
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=clickhouse,
            table=table,  # Required format: table="shema.table"
        )
