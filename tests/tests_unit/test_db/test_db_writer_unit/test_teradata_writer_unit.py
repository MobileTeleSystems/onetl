import pytest

from onetl.connection import Teradata
from onetl.db import DBWriter

pytestmark = pytest.mark.teradata


def test_teradata_writer_wrong_table_name(spark_mock):
    teradata = Teradata(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=teradata,
            target="table",  # Required format: target="schema.table"
        )
