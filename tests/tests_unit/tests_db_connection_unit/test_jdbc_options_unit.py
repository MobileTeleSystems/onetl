import re

import pytest

from onetl.connection import MSSQL, Clickhouse, MySQL, Oracle, Postgres, Teradata
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
@pytest.mark.parametrize(
    "options_class, read_write_restriction",
    [
        (Postgres.FetchOptions, False),
        (Postgres.ExecuteOptions, False),
        (Postgres.ReadOptions, True),
        (Postgres.WriteOptions, True),
        (Clickhouse.FetchOptions, False),
        (Clickhouse.ExecuteOptions, False),
        (Clickhouse.ReadOptions, True),
        (Clickhouse.WriteOptions, True),
        (MSSQL.FetchOptions, False),
        (MSSQL.ExecuteOptions, False),
        (MSSQL.ReadOptions, True),
        (MSSQL.WriteOptions, True),
        (MySQL.FetchOptions, False),
        (MySQL.ExecuteOptions, False),
        (MySQL.ReadOptions, True),
        (MySQL.WriteOptions, True),
        (Teradata.FetchOptions, False),
        (Teradata.ExecuteOptions, False),
        (Teradata.ReadOptions, True),
        (Teradata.WriteOptions, True),
        (Oracle.FetchOptions, False),
        (Oracle.ExecuteOptions, False),
        (Oracle.ReadOptions, True),
        (Oracle.WriteOptions, True),
    ],
)
def test_jdbc_read_write_options_populated_by_connection_class(arg, value, options_class, read_write_restriction):
    if read_write_restriction:
        error_msg = rf"Options \['{arg}'\] are not allowed to use in a {options_class.__name__}"
        with pytest.raises(ValueError, match=error_msg):
            options_class.parse({arg: value})
    else:
        # FetchOptions & ExecuteOptions does not have such restriction
        options = options_class.parse({arg: value})
        assert options.dict()[arg] == value


@pytest.mark.parametrize(
    "options_class, options_class_name",
    [
        (Postgres.ReadOptions, "PostgresReadOptions"),
        (Clickhouse.ReadOptions, "ClickhouseReadOptions"),
        (MSSQL.ReadOptions, "MSSQLReadOptions"),
        (MySQL.ReadOptions, "MySQLReadOptions"),
        (Teradata.ReadOptions, "TeradataReadOptions"),
        (Oracle.ReadOptions, "OracleReadOptions"),
    ],
)
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
def test_jdbc_write_options_cannot_be_used_in_read_options(arg, value, options_class, options_class_name):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a {options_class_name}"
    with pytest.raises(ValueError, match=error_msg):
        options_class.parse({arg: value})


@pytest.mark.parametrize(
    "options_class, options_class_name",
    [
        (Postgres.WriteOptions, "PostgresWriteOptions"),
        (Clickhouse.WriteOptions, "ClickhouseWriteOptions"),
        (MSSQL.WriteOptions, "MSSQLWriteOptions"),
        (MySQL.WriteOptions, "MySQLWriteOptions"),
        (Teradata.WriteOptions, "TeradataWriteOptions"),
        (Oracle.WriteOptions, "OracleWriteOptions"),
    ],
)
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
def test_jdbc_read_options_cannot_be_used_in_write_options(options_class, options_class_name, arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a {options_class_name}"
    with pytest.raises(ValueError, match=error_msg):
        options_class.parse({arg: value})


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
    warning_msg = "Deprecated in 0.5.0 and will be removed in 1.0.0. Use 'ReadOptions' or 'WriteOptions' instead"
    with pytest.warns(UserWarning, match=warning_msg):
        options = Postgres.Options.parse({arg: value})

    parsed_value = options.dict().get(arg) or options.dict(by_alias=True).get(arg)
    assert parsed_value == value


@pytest.mark.parametrize(
    "options_class",
    [
        Postgres.ReadOptions,
        Clickhouse.ReadOptions,
        MSSQL.ReadOptions,
        MySQL.ReadOptions,
        Teradata.ReadOptions,
        Oracle.ReadOptions,
    ],
)
def test_jdbc_read_options_partitioning_is_not_valid(options_class):
    with pytest.raises(ValueError):
        options_class(numPartitions=200)

    with pytest.raises(ValueError):
        options_class(partitionColumn="test")


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


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, JDBCTableExistBehavior.APPEND),
        ({"if_exists": "append"}, JDBCTableExistBehavior.APPEND),
        ({"if_exists": "ignore"}, JDBCTableExistBehavior.IGNORE),
        ({"if_exists": "error"}, JDBCTableExistBehavior.ERROR),
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
        (
            {"mode": "ignore"},
            JDBCTableExistBehavior.IGNORE,
            "Option `WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "error"},
            JDBCTableExistBehavior.ERROR,
            "Option `WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `WriteOptions(if_exists=...)` instead",
        ),
    ],
)
def test_jdbc_write_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = Postgres.WriteOptions(**options)
        assert options.if_exists == value


@pytest.mark.parametrize(
    "options_class, options",
    [
        (Postgres.WriteOptions, {"if_exists": "wrong_mode"}),
        (Clickhouse.WriteOptions, {"if_exists": "wrong_mode"}),
        (MSSQL.WriteOptions, {"if_exists": "wrong_mode"}),
        (MySQL.WriteOptions, {"if_exists": "wrong_mode"}),
        (Teradata.WriteOptions, {"if_exists": "wrong_mode"}),
        (Oracle.WriteOptions, {"if_exists": "wrong_mode"}),
    ],
)
def test_jdbc_write_options_mode_wrong(options_class, options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        options_class(**options)


@pytest.mark.parametrize(
    "options, expected_message",
    [
        ({"numPartitions": 2}, "lowerBound and upperBound must be set if numPartitions > 1"),
        ({"numPartitions": 2, "lowerBound": 0}, "lowerBound and upperBound must be set if numPartitions > 1"),
        ({"numPartitions": 2, "upperBound": 10}, "lowerBound and upperBound must be set if numPartitions > 1"),
    ],
)
def test_jdbc_sql_options_partition_bounds(options, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        Postgres.SQLOptions(**options)


@pytest.mark.parametrize(
    "options_class",
    [
        Postgres.SQLOptions,
        Clickhouse.SQLOptions,
        MSSQL.SQLOptions,
        MySQL.SQLOptions,
        Teradata.SQLOptions,
        Oracle.SQLOptions,
    ],
)
def test_jdbc_sql_options_partitioning_mode_prohibited(options_class):
    with pytest.raises(ValueError, match=r"Options \['partitioning_mode'\] are not allowed"):
        options_class(partitioning_mode="range")


@pytest.mark.parametrize(
    "options_class",
    [
        Postgres.SQLOptions,
        Clickhouse.SQLOptions,
        MSSQL.SQLOptions,
        MySQL.SQLOptions,
        Teradata.SQLOptions,
        Oracle.SQLOptions,
    ],
)
def test_jdbc_sql_options_default(options_class):
    options = options_class()
    assert options.fetchsize == 100_000
    assert options.query_timeout is None


def test_jdbc_deprecated_jdbcoptions():
    deprecated_warning = "Deprecated in 0.11.0 and will be removed in 1.0.0. Use FetchOptions or ExecuteOptions instead"

    with pytest.warns(DeprecationWarning, match=deprecated_warning):
        Postgres.JDBCOptions(fetchsize=10, queryTimeout=30)
