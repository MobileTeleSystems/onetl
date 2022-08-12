from unittest.mock import Mock

import pytest

from onetl.connection import Hive
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
