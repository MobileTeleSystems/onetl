import pytest

from onetl.connection import Greenplum
from onetl.core import DBWriter

pytestmark = pytest.mark.greenplum


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_greenplum_writer_wrong_table(spark_mock, table):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Table name should be passed in `schema.name` format"):
        DBWriter(
            connection=greenplum,
            table=table,  # Required format: table="shema.table"
        )
