import pytest

from onetl.connection import Hive
from onetl.db import DBWriter

pytestmark = pytest.mark.hive


def test_hive_writer_wrong_table_name(spark_mock):
    hive = Hive(cluster="rnd-dwh", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Name should be passed in `schema.table`, `catalog.schema.table` or `catalog.namespace.schema.table` format",
    ):
        DBWriter(
            connection=hive,
            target="table",  # Required format: target="schema.table"
        )
