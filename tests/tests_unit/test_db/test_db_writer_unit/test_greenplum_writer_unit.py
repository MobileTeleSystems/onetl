import pytest

from onetl.connection import Greenplum
from onetl.db import DBWriter

pytestmark = pytest.mark.greenplum


def test_greenplum_writer_wrong_table_name(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=greenplum,
            table="table",  # Required format: table="schema.table"
        )
