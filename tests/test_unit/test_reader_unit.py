import pytest

from onetl.reader import DBReader
from onetl.connection.db_connection import Oracle, Postgres


class TestDBReader:

    # TODO: the functionality of the connection class in the reader class is tested
    def test_db_reader_set_lower_upper_bound(self):
        reader: DBReader = DBReader(
            Oracle(),
            table="default.test",
            jdbc_options={"lowerBound": 10, "upperBound": 1000},
        )

        assert "lowerBound" in reader.jdbc_options.keys()
        assert "upperBound" in reader.jdbc_options.keys()
        assert reader.jdbc_options.get("lowerBound") == 10
        assert reader.jdbc_options.get("upperBound") == 1000

    def test_db_reader_generate_jdbc_options(self):

        reader: DBReader = DBReader(
            connection=Postgres(host="local", user="admin", password="1234"),
            table="default.test",
            # some of the parameters below are not used together.
            # Such as fetchsize and batchsize.
            # This test only demonstrates how the parameters will be distributed
            # in the jdbc() function in the properties parameter.
            jdbc_options={
                "lowerBound": 10,
                "upperBound": 1000,
                "partitionColumn": "some_column",
                "numPartitions": 20,
                "fetchsize": 1000,
                "batchsize": 1000,
                "isolationLevel": "NONE",
                "sessionInitStatement": "BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
                "truncate": "true",
                "createTableOptions": "some_options",
                "createTableColumnTypes": "some_option",
                "customSchema": "id DECIMAL(38, 0)",
            },
            where="some_column_1 = 2 AND some_column_2 = 3",
            hint="some_hint",
            columns=["column_1", "column_2"],
        )

        jdbc_params = reader.connection.jdbc_params_creator(
            jdbc_options=reader.jdbc_options,
        )

        assert jdbc_params == {
            "lowerBound": "10",
            "upperBound": "1000",
            "url": "jdbc:postgresql://local:5432/default",
            "column": "some_column",
            "numPartitions": "20",
            "properties": {
                "user": "admin",
                "driver": "org.postgresql.Driver",
                "fetchsize": "1000",
                "batchsize": "1000",
                "isolationLevel": "NONE",
                "sessionInitStatement": "BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
                "truncate": "true",
                "createTableOptions": "some_options",
                "createTableColumnTypes": "some_option",
                "customSchema": "id DECIMAL(38, 0)",
                "password": "1234",
            },
        }

    def test_db_reader_jdbc_properties_value_error(self):
        reader = DBReader(
            connection=Oracle(),
            table="default.test",
            jdbc_options={"user": "some_user"},
        )
        with pytest.raises(ValueError):
            reader.run()
