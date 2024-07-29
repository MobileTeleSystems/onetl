import pytest

from onetl.connection import Iceberg
from onetl.db import DBWriter

pytestmark = pytest.mark.iceberg


def test_iceberg_writer_wrong_table_name(spark_mock):
    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Name should be passed in `schema.name` format",
    ):
        DBWriter(
            connection=iceberg,
            target="table",  # Required format: target="schema.table"
        )
