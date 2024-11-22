import pytest

from onetl.connection import MSSQL
from onetl.db import DBWriter

pytestmark = pytest.mark.mssql


def test_mssql_writer_wrong_table_name(spark_mock):
    mssql = MSSQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=mssql,
            table="table",  # Required format: table="schema.table"
        )
