import pytest

from onetl.connection import Hive
from onetl.core import DBWriter


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_hive_writer_wrong_table(spark_mock, table):
    hive = Hive(cluster="rnd-dwh", spark=spark_mock)

    with pytest.raises(ValueError, match="Table name should be passed in `schema.name` format"):
        DBWriter(
            connection=hive,
            table=table,  # Required format: table="shema.table"
        )
