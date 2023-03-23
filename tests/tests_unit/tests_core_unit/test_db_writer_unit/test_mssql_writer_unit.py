import pytest

from onetl.connection import MSSQL
from onetl.core import DBWriter


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_mssql_writer_wrong_table(spark_mock, table):
    mssql = MSSQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Table name should be passed in `schema.name` format"):
        DBWriter(
            connection=mssql,
            table=table,  # Required format: table="shema.table"
        )
