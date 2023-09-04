import pytest

from onetl.connection import Hive
from onetl.db import DBWriter

pytestmark = pytest.mark.hive


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_hive_writer_wrong_table_name(spark_mock, table):
    hive = Hive(cluster="rnd-dwh", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=hive,
            table=table,  # Required format: table="shema.table"
        )
