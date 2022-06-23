from unittest.mock import Mock

import pytest

from onetl.connection import Hive, Oracle
from onetl.core import DBWriter


spark = Mock()


def test_writer_without_schema():
    with pytest.raises(ValueError):
        DBWriter(
            connection=Hive(spark=spark),
            table="table",  # missing schema
        )


def test_writer_with_too_many_dots():
    with pytest.raises(ValueError):
        DBWriter(
            connection=Hive(spark=spark),
            table="schema.table.abc",  # wrong input
        )


@pytest.mark.parametrize(
    "connection,options",
    [
        (
            Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=spark),
            Hive.Options(bucket_by=(10, "hwm_int"), sort_by="hwm_int"),
        ),
        (
            Hive(spark=spark),
            Oracle.Options(fetchsize=1000, truncate=True, partition_column="id_int"),
        ),
    ],
    ids=["JDBC connection with Hive options.", "Hive connection with JDBC options."],
)
def test_db_writer_inappropriate_connection_and_options_types(connection, options):
    with pytest.raises(TypeError):
        DBWriter(
            connection=connection,
            table="onetl.some_table",
            options=options,
        )
