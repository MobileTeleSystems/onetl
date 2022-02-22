from unittest.mock import Mock

import pytest

from onetl.connection import Hive
from onetl.core import DBWriter


class TestDBWriter:
    spark = Mock()

    def test_writer_without_schema(self):
        with pytest.raises(ValueError):
            DBWriter(
                connection=Hive(spark=self.spark),
                table="table",  # missing schema
            )

    def test_writer_with_too_many_dots(self):
        with pytest.raises(ValueError):
            DBWriter(
                connection=Hive(spark=self.spark),
                table="schema.table.abc",  # wrong input
            )
