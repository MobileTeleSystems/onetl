import re

import pytest

from onetl._internal import to_camel
from onetl.connection import Postgres
from onetl.connection.db_connection.jdbc_connection import JDBCTableExistBehavior

pytestmark = [pytest.mark.postgres]


def test_jdbc_read_options_default():
    options = Postgres.ReadOptions()

    assert options.fetchsize == 100_000
    assert options.query_timeout is None


def test_jdbc_write_options_default():
    options = Postgres.WriteOptions()

    assert options.if_exists == JDBCTableExistBehavior.APPEND
    assert options.batchsize == 20_000
    assert options.isolation_level == "READ_UNCOMMITTED"
    assert options.query_timeout is None


def test_jdbc_options_default():
    options = Postgres.Options()

    assert options.if_exists == JDBCTableExistBehavior.APPEND
    assert options.fetchsize == 100_000
    assert options.batchsize == 20_000
    assert options.isolation_level == "READ_UNCOMMITTED"
    assert options.query_timeout is None


@pytest.mark.parametrize(
    "arg, value",
    [
        ("table", "mytable"),
        ("dbtable", "mytable"),
        ("query", "select * from mytable"),
        ("properties", {"abc": "cde"}),
    ],
)
def test_jdbc_read_write_options_populated_by_connection_class(arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a JDBCReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.ReadOptions.parse({arg: value})

    error_msg = rf"Options \['{arg}'\] are not allowed to use in a JDBCWriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.WriteOptions.parse({arg: value})

    # JDBCOptions does not have such restriction
    options = Postgres.JDBCOptions.parse({arg: value})
    assert options.dict()[arg] == value


@pytest.mark.parametrize(
    "arg, value",
    [
        ("column", "some"),
        ("mode", "append"),
        ("batchsize", 10),
        ("isolationLevel", "NONE"),
        ("isolation_level", "NONE"),
        ("truncate", True),
        ("cascadeTruncate", True),
        ("createTableOptions", "engine=innodb"),
        ("createTableColumnTypes", "a varchar"),
    ],
)
def test_jdbc_write_options_cannot_be_used_in_read_options(arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a JDBCReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.ReadOptions.parse({arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("column", "some"),
        ("partitionColumn", "part"),
        ("partition_column", "part"),
        ("lowerBound", 10),
        ("lower_bound", 10),
        ("upperBound", 10),
        ("upper_bound", 10),
        ("numPartitions", 10),
        ("num_partitions", 10),
        ("sessionInitStatement", "begin end;"),
        ("session_init_statement", "begin end;"),
        ("customSchema", "a varchar"),
        ("pushDownPredicate", True),
        ("pushDownAggregate", True),
        ("pushDownLimit", True),
        ("pushDownTableSample", True),
        ("predicates", "s"),
    ],
)
def test_jdbc_read_options_cannot_be_used_in_write_options(arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a JDBCWriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.WriteOptions.parse({arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("mode", "append"),
        ("batchsize", 10),
        ("isolationLevel", "NONE"),
        ("isolation_level", "NONE"),
        ("truncate", True),
        ("cascadeTruncate", True),
        ("createTableOptions", "engine=innodb"),
        ("createTableColumnTypes", "a varchar"),
        ("fetchsize", 10),
        ("sessionInitStatement", "begin end;"),
        ("session_init_statement", "begin end;"),
        ("customSchema", "a varchar"),
        ("pushDownPredicate", True),
        ("pushDownAggregate", True),
        ("pushDownLimit", True),
        ("pushDownTableSample", True),
        ("predicates", "s"),
    ],
)
def test_jdbc_old_options_allowed_but_deprecated(arg, value):
    warning_msg = "Please use 'ReadOptions' or 'WriteOptions' class instead. Will be removed in v1.0.0"
    with pytest.warns(UserWarning, match=warning_msg):
        options = Postgres.Options.parse({arg: value})

    assert options.dict(by_alias=True)[to_camel(arg)] == value


def test_jdbc_read_options_partitioning_is_not_valid():
    with pytest.raises(ValueError):
        Postgres.ReadOptions(numPartitions=200)

    with pytest.raises(ValueError):
        Postgres.ReadOptions(partitionColumn="test")


def test_jdbc_read_options_case():
    camel_case = Postgres.ReadOptions(
        lowerBound=10,
        upperBound=1000,
        partitionColumn="some_column",
        numPartitions=20,
        fetchsize=1000,
        sessionInitStatement="BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
        # unknown options names are NOT being converted from snake_case to CamelCase
        snake_case_option="left unchanged",
        camelCaseOption="left unchanged",
        CamelCaseOption="left unchanged",
    )
    snake_case = Postgres.ReadOptions(
        lower_bound=10,
        upper_bound=1000,
        partition_column="some_column",
        num_partitions=20,
        fetchsize=1000,
        session_init_statement="BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
        # unknown options names are NOT being converted from snake_case to CamelCase
        snake_case_option="left unchanged",
        camelCaseOption="left unchanged",
        CamelCaseOption="left unchanged",
    )

    assert camel_case == snake_case


def test_jdbc_write_options_case():
    camel_case = Postgres.WriteOptions(
        batchsize=1000,
        truncate=True,
        isolationLevel="NONE",
        snake_case_option="left unchanged",
        # unknown options names are NOT being converted from snake_case to CamelCase
        camelCaseOption="left unchanged",
        CamelCaseOption="left unchanged",
    )

    snake_case = Postgres.WriteOptions(
        batchsize=1000,
        truncate=True,
        isolation_level="NONE",
        # unknown options names are NOT being converted from snake_case to CamelCase
        snake_case_option="left unchanged",
        camelCaseOption="left unchanged",
        CamelCaseOption="left unchanged",
    )

    assert camel_case == snake_case


def test_jdbc_read_options_to_jdbc(spark_mock):
    connection = Postgres(host="local", user="admin", database="default", password="1234", spark=spark_mock)
    jdbc_params = connection.options_to_jdbc_params(
        options=Postgres.ReadOptions(
            lowerBound=10,
            upperBound=1000,
            partitionColumn="some_column",
            numPartitions=20,
            fetchsize=1000,
            sessionInitStatement="BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
            snake_case_option="left unchanged",
            camelCaseOption="left unchanged",
            CamelCaseOption="left unchanged",
        ),
    )

    assert jdbc_params == {
        "column": "some_column",
        "lowerBound": "10",
        "numPartitions": "20",
        "properties": {
            "driver": "org.postgresql.Driver",
            "fetchsize": "1000",
            "password": "1234",
            "sessionInitStatement": "BEGIN execute immediate 'alter session set '_serial_direct_read'=true",
            "user": "admin",
            "snake_case_option": "left unchanged",
            "camelCaseOption": "left unchanged",
            "CamelCaseOption": "left unchanged",
        },
        "upperBound": "1000",
        "url": "jdbc:postgresql://local:5432/default?ApplicationName=abc",
    }


def test_jdbc_write_options_to_jdbc(spark_mock):
    connection = Postgres(host="local", user="admin", database="default", password="1234", spark=spark_mock)
    jdbc_params = connection.options_to_jdbc_params(
        options=Postgres.WriteOptions(
            batchsize=1000,
            truncate=True,
            isolation_level="NONE",
            snake_case_option="left unchanged",
            camelCaseOption="left unchanged",
            CamelCaseOption="left unchanged",
        ),
    )

    assert jdbc_params == {
        "properties": {
            "batchsize": "1000",
            "driver": "org.postgresql.Driver",
            "password": "1234",
            "isolationLevel": "NONE",
            "truncate": "true",
            "user": "admin",
            "snake_case_option": "left unchanged",
            "camelCaseOption": "left unchanged",
            "CamelCaseOption": "left unchanged",
        },
        "url": "jdbc:postgresql://local:5432/default?ApplicationName=abc",
    }


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, JDBCTableExistBehavior.APPEND),
        ({"if_exists": "append"}, JDBCTableExistBehavior.APPEND),
        ({"if_exists": "replace_entire_table"}, JDBCTableExistBehavior.REPLACE_ENTIRE_TABLE),
    ],
)
def test_jdbc_write_options_if_exists(options, value):
    assert Postgres.WriteOptions(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "append"},
            JDBCTableExistBehavior.APPEND,
            "Option `WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_table"},
            JDBCTableExistBehavior.REPLACE_ENTIRE_TABLE,
            "Option `WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            JDBCTableExistBehavior.REPLACE_ENTIRE_TABLE,
            "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_table` instead",
        ),
    ],
)
def test_jdbc_write_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = Postgres.WriteOptions(**options)
        assert options.if_exists == value


@pytest.mark.parametrize(
    "options",
    [
        # disallowed modes
        {"mode": "error"},
        {"mode": "ignore"},
        # wrong mode
        {"mode": "wrong_mode"},
    ],
)
def test_jdbc_write_options_mode_wrong(options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        Postgres.WriteOptions(**options)
