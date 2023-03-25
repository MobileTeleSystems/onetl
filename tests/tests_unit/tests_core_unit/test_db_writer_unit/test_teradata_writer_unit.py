import pytest

from onetl.connection import Teradata
from onetl.core import DBWriter


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_teradata_writer_wrong_table_name(spark_mock, table):
    teradata = Teradata(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Table name should be passed in `schema.name` format"):
        DBWriter(
            connection=teradata,
            table=table,  # Required format: table="shema.table"
        )
