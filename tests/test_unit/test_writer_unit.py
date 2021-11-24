import pytest
from unittest.mock import Mock

from onetl.connection.db_connection import Hive
from onetl.writer import DBWriter


class TestWriterMock:
    spark = Mock()

    def test_hive_writer_jdbc_properties_raise_exception(self):
        """
        if the type of the connection parameter is equal to Hive,
        it will fall if the jdbc_options parameter is used
        """
        with pytest.raises(ValueError):
            DBWriter(
                connection=Hive(spark=self.spark),
                table="table_name",
                jdbc_options={"user": "some_user"},  # wrong parameter
            )
