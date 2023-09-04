import pytest

from onetl.connection import MySQL
from onetl.db import DBWriter

pytestmark = pytest.mark.mysql


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_mysql_writer_wrong_table_name(spark_mock, table):
    mysql = MySQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=mysql,
            table=table,  # Required format: table="shema.table"
        )
