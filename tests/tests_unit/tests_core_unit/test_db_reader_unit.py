from unittest.mock import Mock

import pytest

from onetl.connection import Hive, Oracle, Postgres
from onetl.core import DBReader


spark = Mock()


def test_reader_without_schema():
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="table",  # missing schema
        )


def test_reader_with_too_many_dots():
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
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
def test_reader_with_wrong_options(options):
    with pytest.raises(TypeError):
        DBReader(
            connection=Hive(spark=spark),
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
        "abc,ABC,cde",
        ["*", "*", "cde"],
        ["abc", "abc", "cde"],
        ["abc", "ABC", "cde"],
    ],
)
def test_reader_invalid_columns(columns):
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
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
def test_reader_valid_columns(columns, real_columns):
    reader = DBReader(
        connection=Hive(spark=spark),
        table="schema.table",
        columns=columns,
    )

    assert reader.columns == real_columns


@pytest.mark.parametrize(
    "hwm_column",
    [  # noqa: WPS317
        "wrong/name",
        "wrong@name",
        "wrong=name",
        "wrong#name",
        [],
        {},
        (),
        set(),
        frozenset(),
        ("name",),
        ["name"],
        {"name"},
        ("wrong/name", "statement"),
        ("wrong@name", "statement"),
        ("wrong=name", "statement"),
        ("wrong#name", "statement"),
        ["wrong/name", "statement"],
        ["wrong@name", "statement"],
        ["wrong=name", "statement"],
        ["wrong#name", "statement"],
        ("wrong/name", "statement", "too", "many"),
        ("wrong@name", "statement", "too", "many"),
        ("wrong=name", "statement", "too", "many"),
        ("wrong#name", "statement", "too", "many"),
        ["wrong/name", "statement", "too", "many"],
        ["wrong@name", "statement", "too", "many"],
        ["wrong=name", "statement", "too", "many"],
        ["wrong#name", "statement", "too", "many"],
        {"wrong/name", "statement", "too", "many"},
        {"wrong@name", "statement", "too", "many"},
        {"wrong=name", "statement", "too", "many"},
        {"wrong#name", "statement", "too", "many"},
        (None, "statement"),
        [None, "statement"],
        # this is the same as hwm_column="name",
        # but if user implicitly passed a tuple
        # both of values should be set to avoid unexpected errors
        ("name", None),
        ["name", None],
    ],
)
def test_reader_invalid_hwm_column(hwm_column):
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table",
            hwm_column=hwm_column,
        )


@pytest.mark.parametrize(
    "hwm_column, real_hwm_column, real_hwm_expression",
    [
        ("hwm_column", "hwm_column", None),  # noqa: WPS317
        (("hwm_column", "expression"), "hwm_column", "expression"),  # noqa: WPS317
        (("hwm_column", "hwm_column"), "hwm_column", "hwm_column"),
    ],
)
def test_reader_valid_hwm_column(hwm_column, real_hwm_column, real_hwm_expression):
    reader = DBReader(
        connection=Hive(spark=spark),
        table="schema.table",
        hwm_column=hwm_column,
    )

    assert reader.hwm_column.name == real_hwm_column
    assert reader.hwm_expression == real_hwm_expression


@pytest.mark.parametrize(
    "columns, hwm_column",
    [  # noqa: WPS317
        (["a", "b", "c", "d"], "d"),
        (["a", "b", "c", "d"], "D"),
        (["a", "b", "c", "D"], "d"),
        ("a, b, c, d", "d"),
        ("a, b, c, d", "D"),
        ("a, b, c, D", "d"),
        (["*", "d"], "d"),
        (["*", "d"], "D"),
        (["*", "D"], "d"),
        ("*, d", "d"),
        ("*, d", "D"),
        ("*, D", "d"),
        (["*"], "d"),
        (["*"], "D"),
        (["*"], ("d", "cast")),
        (["*"], ("D", "cast")),
    ],
)
def test_reader_hwm_column_and_columns_are_not_in_conflict(columns, hwm_column):
    DBReader(
        connection=Hive(spark=spark),
        table="schema.table",
        columns=columns,
        hwm_column=hwm_column,
    )


@pytest.mark.parametrize(
    "columns, hwm_column",
    [  # noqa: WPS317
        (["a", "b", "c", "d"], ("d", "cast")),
        (["a", "b", "c", "d"], ("D", "cast")),
        (["a", "b", "c", "D"], ("d", "cast")),
        ("a, b, c, d", ("d", "cast")),
        ("a, b, c, d", ("D", "cast")),
        ("a, b, c, D", ("d", "cast")),
        (["*", "d"], ("d", "cast")),
        (["*", "d"], ("D", "cast")),
        (["*", "D"], ("d", "cast")),
        ("*, d", ("d", "cast")),
        ("*, d", ("D", "cast")),
        ("*, D", ("d", "cast")),
    ],
)
def test_reader_hwm_column_and_columns_are_in_conflict(columns, hwm_column):
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table",
            columns=columns,
            hwm_column=hwm_column,
        )


def test_reader_jdbc_num_partitions_without_partition_column():
    with pytest.raises(ValueError):
        DBReader(
            connection=Postgres(
                spark=spark,
                database="db",
                host="some_host",
                user="valid_user",
                password="pwd",
            ),
            table="default.table",
            options={"numPartitions": "200"},
        )


# TODO: the functionality of the connection class in the reader class is tested
def test_reader_set_lower_upper_bound():
    reader = DBReader(
        Oracle(spark=spark, host="some_host", user="valid_user", sid="sid", password="pwd"),
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
def test_reader_generate_jdbc_options(options):
    reader = DBReader(
        connection=Postgres(host="local", user="admin", database="default", password="1234", spark=spark),
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
