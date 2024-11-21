import pytest

from onetl.connection import Postgres
from onetl.db import DBWriter

pytestmark = pytest.mark.postgres


def test_postgres_writer_wrong_table_name(spark_mock):
    postgres = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=postgres,
            table="table",  # Required format: table="schema.table"
        )
