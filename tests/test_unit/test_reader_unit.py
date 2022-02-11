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

    @pytest.mark.parametrize(
        "options",
        [  # noqa: WPS317
            {"some", "option"},
            "Some_options",
            123,
            ["Option_1", "Option_2"],
            ("Option_1", "Option_2"),
        ],
        ids=[
            "Wrong type set of <options>.",
            "Wrong type str of <options>.",
            "Wrong type int of <options>.",
            "Wrong type list of <options>.",
            "Wrong type tuple of <options>.",
        ],
    )
    def test_reader_with_wrong_options(self, options):
        with pytest.raises(TypeError):
            DBReader(
                connection=Hive(spark=self.spark),
                table="schema.table",
                options=options,  # wrong options input
            )

    @pytest.mark.parametrize(
        "columns",
        [  # noqa: WPS317
            [],
            (),
            {},
            set(),
            " \t\n",
            [""],
            [" \t\n"],
            ["", "abc"],
            [" \t\n", "abc"],
            "",
            " \t\n",
            ",abc",
            "abc,",
            "cde,,abc",
            "cde, ,abc",
            "*,*,cde",
            "abc,abc,cde",
            ["*", "*", "cde"],
            ["abc", "abc", "cde"],
        ],
    )
    def test_reader_invalid_columns(self, columns):
        with pytest.raises(ValueError):
            DBReader(
                connection=Hive(spark=self.spark),
                table="schema.table",
                columns=columns,
            )

    @pytest.mark.parametrize(
        "columns, real_columns",
        [
            ("*", ["*"]),
            ("abc, cde", ["abc", "cde"]),
            ("*, abc", ["*", "abc"]),
            (["*"], ["*"]),
            (["abc", "cde"], ["abc", "cde"]),
            (["*", "abc"], ["*", "abc"]),
        ],
    )
    def test_reader_valid_columns(self, columns, real_columns):
        reader = DBReader(
            connection=Hive(spark=self.spark),
            table="schema.table",
            columns=columns,
        )

        assert reader.columns == real_columns

    def test_reader_jdbc_num_partitions_without_partition_column(self):
        with pytest.raises(ValueError):
            DBReader(
                connection=Postgres(
                    spark=self.spark,
                    database="db",
                    host="some_host",
                    user="valid_user",
                    password="pwd",
                ),
                table="default.table",
                options={"numPartitions": "200"},
            )

    # TODO: the functionality of the connection class in the reader class is tested
    def test_reader_set_lower_upper_bound(self):
        reader = DBReader(
            Oracle(spark=self.spark, host="some_host", user="valid_user", sid="sid", password="pwd"),
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
    def test_reader_generate_jdbc_options(self, options):
        reader = DBReader(
            connection=Postgres(host="local", user="admin", database="default", password="1234", spark=self.spark),
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
            options=reader.options,
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
