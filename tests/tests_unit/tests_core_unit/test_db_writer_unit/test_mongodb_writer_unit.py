import pytest

from onetl.connection import MongoDB
from onetl.core import DBWriter

pytestmark = pytest.mark.mongodb


def test_mongodb_writer_wrong_table_name(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(ValueError, match="Table name should be passed in `table_name` format"):
        DBWriter(
            connection=mongo,
            table="schema.table",  # Includes schema. Required format: table="table"
        )
