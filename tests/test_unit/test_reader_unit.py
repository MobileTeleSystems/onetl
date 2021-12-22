import pytest
from unittest.mock import Mock

from onetl.reader import DBReader
from onetl.connection.db_connection import Oracle, Postgres, Hive


class TestDBReader:
    spark = Mock()

    def test_reader_without_schema(self):
        with pytest.raises(ValueError):
            DBReader(
                connection=Hive(spark=self.spark),
                table="table",  # missing schema
            )

    def test_reader_with_too_many_dots(self):
        with pytest.raises(ValueError):
            DBReader(
                connection=Hive(spark=self.spark),
                table="schema.table.abc",  # wrong input
            )

    def test_reader_jdbc_properties_raise_exception(self):
        """
        if the type of the connection parameter is equal to Hive,
        it will fall if the jdbc_properties parameter is used
        """

        reader = DBReader(
            connection=Hive(spark=self.spark),
            table="default.table",
            options=Hive.Options(user="some_user"),  # wrong parameter
        )

        with pytest.raises(ValueError):
            reader.run()

    # TODO: the functionality of the connection class in the reader class is tested
    def test_db_reader_set_lower_upper_bound(self):
        reader: DBReader = DBReader(
            Oracle(spark=self.spark, host="some_host", user="valid_user", password="pwd"),
            table="default.test",
            options=Oracle.Options(lowerBound=10, upperBound=1000),
        )

        assert reader.options.lower_bound == 10
        assert reader.options.upper_bound == 1000

    @pytest.mark.parametrize(
        "options",
        [
            {
                "lowerBound": 10,
                "upperBound": 1000,
                "partitionColumn": "some_column",
                "numPartitions": 20,
                "fetchsize": 1000,
                "batchsize": 1000,
                "isolationLevel": "NONE",
                "sessionInitStatement": "BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
                "truncate": True,
                "createTableOptions": "name CHAR(64)",
                "createTableColumnTypes": "name CHAR(64)",
                "customSchema": "id DECIMAL(38, 0)",
                "unknownProperty": "SomeValue",
                "mode": "append",
            },
            Postgres.Options(
                lowerBound=10,
                upperBound=1000,
                partitionColumn="some_column",
                numPartitions=20,
                fetchsize=1000,
                batchsize=1000,
                isolationLevel="NONE",
                sessionInitStatement="BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
                truncate=True,
                createTableOptions="name CHAR(64)",
                createTableColumnTypes="name CHAR(64)",
                customSchema="id DECIMAL(38, 0)",
                unknownProperty="SomeValue",
                mode="append",
            ),
            Postgres.Options(
                lower_bound=10,
                upper_bound=1000,
                partition_column="some_column",
                num_partitions=20,
                fetchsize=1000,
                batchsize=1000,
                isolation_level="NONE",
                session_init_statement="BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
                truncate=True,
                create_table_options="name CHAR(64)",
                create_table_column_types="name CHAR(64)",
                custom_schema="id DECIMAL(38, 0)",
                unknownProperty="SomeValue",
                mode="append",
            ),
        ],
        ids=["Options as dictionary.", "Options with camel case.", "Options with snake case."],
    )
    def test_db_reader_generate_jdbc_options(self, options):

        reader: DBReader = DBReader(
            connection=Postgres(host="local", user="admin", password="1234", spark=self.spark),
            table="default.test",
            # some of the parameters below are not used together.
            # Such as fetchsize and batchsize.
            # This test only demonstrates how the parameters will be distributed
            # in the jdbc() function in the properties parameter.
            options=options,
            where="some_column_1 = 2 AND some_column_2 = 3",
            hint="some_hint",
            columns=["column_1", "column_2"],
        )

        jdbc_params = reader.connection.jdbc_params_creator(
            jdbc_options=reader.options,
        )

        assert jdbc_params == {
            "column": "some_column",
            "lowerBound": "10",
            "numPartitions": "20",
            "mode": "append",
            "properties": {
                "batchsize": "1000",
                "createTableColumnTypes": "name CHAR(64)",
                "createTableOptions": "name CHAR(64)",
                "customSchema": "id DECIMAL(38, 0)",
                "driver": "org.postgresql.Driver",
                "fetchsize": "1000",
                "isolationLevel": "NONE",
                "password": "1234",
                "sessionInitStatement": "BEGIN execute immediate 'alter " "session set " "'_serial_direct_read'=true",
                "truncate": "true",
                "unknownProperty": "SomeValue",
                "user": "admin",
            },
            "upperBound": "1000",
            "url": "jdbc:postgresql://local:5432/default",
        }
