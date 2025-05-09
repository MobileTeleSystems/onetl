import pytest

from onetl.connection import Oracle
from onetl.db import DBWriter

pytestmark = pytest.mark.oracle


def test_oracle_writer_wrong_table_name(spark_mock):
    oracle = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=oracle,
            target="table",  # Required format: target="schema.table"
        )
