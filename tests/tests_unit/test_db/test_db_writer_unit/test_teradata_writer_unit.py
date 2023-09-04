import pytest

from onetl.connection import Teradata
from onetl.db import DBWriter

pytestmark = pytest.mark.teradata


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_teradata_writer_wrong_table_name(spark_mock, table):
    teradata = Teradata(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=teradata,
            table=table,  # Required format: table="shema.table"
        )
