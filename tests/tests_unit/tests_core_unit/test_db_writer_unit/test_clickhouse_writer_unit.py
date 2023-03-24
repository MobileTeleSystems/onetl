import pytest

from onetl.connection import Clickhouse
from onetl.core import DBWriter

pytestmark = pytest.mark.clickhouse


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_clickhouse_writer_wrong_table(spark_mock, table):
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Table name should be passed in `schema.name` format"):
        DBWriter(
            connection=clickhouse,
            table=table,  # Required format: table="shema.table"
        )
