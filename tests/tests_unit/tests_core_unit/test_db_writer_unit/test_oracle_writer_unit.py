import pytest

from onetl.connection import Oracle
from onetl.core import DBWriter


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_oracle_writer_wrong_table_name(spark_mock, table):
    oracle = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Table name should be passed in `schema.name` format"):
        DBWriter(
            connection=oracle,
            table=table,  # Required format: table="shema.table"
        )
