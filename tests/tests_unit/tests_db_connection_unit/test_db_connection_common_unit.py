import logging
import re
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl._internal import to_camel  # noqa: WPS436
from onetl.connection import Hive, Oracle, Postgres
from onetl.connection.db_connection.jdbc_connection import JDBCWriteMode

spark = Mock(spec=SparkSession)


def test_secure_str_and_repr():
    conn = Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=spark)

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_jdbc_connection_without_host_and_credentials():
    with pytest.raises(ValueError):
        Postgres(spark=spark)  # noqa: F841


def test_jdbc_read_options_default():
    options = Oracle.ReadOptions()

    assert options.fetchsize == 100_000
    assert options.query_timeout == 0


def test_jdbc_write_options_default():
    options = Oracle.WriteOptions()

    assert options.mode == JDBCWriteMode.APPEND
    assert options.batchsize == 20_000
    assert options.isolation_level == "READ_UNCOMMITTED"
    assert options.query_timeout == 0


def test_jdbc_options_default():
    options = Oracle.Options()

    assert options.mode == JDBCWriteMode.APPEND
    assert options.fetchsize == 100_000
    assert options.batchsize == 20_000
    assert options.isolation_level == "READ_UNCOMMITTED"
    assert options.query_timeout == 0


@pytest.mark.parametrize(
    "options_class",
    [
        Postgres.ReadOptions,
        Postgres.WriteOptions,
        Postgres.JDBCOptions,
        Postgres.Options,
    ],
)
@pytest.mark.parametrize(
    "arg, value",
    [
        ("url", "jdbc:ora:thin/abc"),
        ("driver", "com.oracle.jdbc.Driver"),
        ("user", "me"),
        ("password", "abc"),
    ],
)
def test_jdbc_options_connection_parameters_cannot_be_passed(options_class, arg, value):
    with pytest.raises(ValueError, match=f"Option '{arg}' is not allowed to use in a {options_class.__name__}"):
        options_class(**{arg: value})


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
    error_msg = f"Option '{arg}' is not allowed to use in a ReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.ReadOptions(**{arg: value})

    error_msg = f"Option '{arg}' is not allowed to use in a WriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.WriteOptions(**{arg: value})

    # JDBCOptions does not have such restriction
    options = Postgres.JDBCOptions(**{arg: value})
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
    error_msg = f"Option '{arg}' is not allowed to use in a ReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.ReadOptions(**{arg: value})


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
def test_jdbc_read_options_cannot_be_used_in_write_options(arg, value):
    error_msg = f"Option '{arg}' is not allowed to use in a WriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Postgres.WriteOptions(**{arg: value})


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
def test_jdbc_old_options_allowed_but_deprecated(arg, value, caplog):
    with caplog.at_level(logging.WARNING):
        options = Postgres.Options(**{arg: value})

        assert (
            "`SomeDB.Options` class is deprecated since v0.5.0 and will be removed in v1.0.0. "
            "Please use `SomeDB.ReadOptions` or `SomeDB.WriteOptions` classes instead"
        ) in caplog.text

    assert options.dict(by_alias=True)[to_camel(arg)] == value


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"lowerBound": 10},
        {"upperBound": 100},
        {"lowerBound": 10, "upperBound": 100},
    ],
)
def test_jdbc_read_options_partitioning_is_not_valid(kwargs):
    with pytest.raises(ValueError):
        Postgres.ReadOptions(numPartitions=200, **kwargs)

    with pytest.raises(ValueError):
        Postgres.ReadOptions(partitionColumn="test", **kwargs)

    if kwargs:
        with pytest.raises(ValueError):
            Postgres.ReadOptions(**kwargs)


@pytest.mark.parametrize(
    "options_class, options_class_name, known_options",
    [
        (Hive.WriteOptions, "WriteOptions", {"mode": "overwrite_partitions"}),
        (Hive.Options, "Options", {"mode": "overwrite_partitions"}),
        (Postgres.ReadOptions, "ReadOptions", {"fetchsize": 10, "keytab": "a/b/c"}),
        (Postgres.WriteOptions, "WriteOptions", {"mode": "overwrite", "keytab": "a/b/c"}),
        (Postgres.Options, "Options", {"mode": "overwrite", "keytab": "a/b/c"}),
    ],
)
def test_jdbc_options_warn_for_unknown(options_class, options_class_name, known_options, caplog):
    with caplog.at_level(logging.WARNING):
        options_class(some_unknown_option="value", **known_options)

        assert (
            f"Option 'some_unknown_option' is not known by {options_class_name}, are you sure it is valid?"
        ) in caplog.text

        options_class(option1="value1", option2=None, **known_options)

        assert (
            f"Options 'option1', 'option2' are not known by {options_class_name}, are you sure they are valid?"
        ) in caplog.text

        for known_option in known_options:
            assert known_option not in caplog.text


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
        mode="append",
        isolationLevel="NONE",
        snake_case_option="left unchanged",
        # unknown options names are NOT being converted from snake_case to CamelCase
        camelCaseOption="left unchanged",
        CamelCaseOption="left unchanged",
    )

    snake_case = Postgres.WriteOptions(
        batchsize=1000,
        truncate=True,
        mode="append",
        isolation_level="NONE",
        # unknown options names are NOT being converted from snake_case to CamelCase
        snake_case_option="left unchanged",
        camelCaseOption="left unchanged",
        CamelCaseOption="left unchanged",
    )

    assert camel_case == snake_case


def test_jdbc_read_options_to_jdbc():
    connection = Postgres(host="local", user="admin", database="default", password="1234", spark=spark)
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
            "queryTimeout": "0",
            "snake_case_option": "left unchanged",
            "camelCaseOption": "left unchanged",
            "CamelCaseOption": "left unchanged",
        },
        "upperBound": "1000",
        "url": "jdbc:postgresql://local:5432/default",
    }


def test_jdbc_write_options_to_jdbc():
    connection = Postgres(host="local", user="admin", database="default", password="1234", spark=spark)
    jdbc_params = connection.options_to_jdbc_params(
        options=Postgres.WriteOptions(
            batchsize=1000,
            truncate=True,
            mode="append",
            isolation_level="NONE",
            snake_case_option="left unchanged",
            camelCaseOption="left unchanged",
            CamelCaseOption="left unchanged",
        ),
    )

    assert jdbc_params == {
        "mode": "append",
        "properties": {
            "batchsize": "1000",
            "driver": "org.postgresql.Driver",
            "password": "1234",
            "isolationLevel": "NONE",
            "truncate": "true",
            "user": "admin",
            "queryTimeout": "0",
            "snake_case_option": "left unchanged",
            "camelCaseOption": "left unchanged",
            "CamelCaseOption": "left unchanged",
        },
        "url": "jdbc:postgresql://local:5432/default",
    }


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
def test_jdbc_write_options_wrong_mode(options):
    oracle = Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=spark)

    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        oracle.WriteOptions(**options)


@pytest.mark.parametrize(
    "options_class,options",
    [
        (
            Oracle.ReadOptions,
            Oracle.WriteOptions(bucket_by=(10, "hwm_int"), sort_by="hwm_int"),
        ),
        (
            Oracle.WriteOptions,
            Oracle.ReadOptions(fetchsize=1000),
        ),
    ],
    ids=[
        "Write options object passed to ReadOptions",
        "Read options object passed to WriteOptions",
    ],
)
def test_jdbc_options_parse_mismatch_class(options_class, options):
    with pytest.raises(TypeError):
        options_class.parse(options)


@pytest.mark.parametrize(
    "connection,options",
    [
        (
            Oracle,
            Hive.WriteOptions(bucket_by=(10, "hwm_int"), sort_by="hwm_int"),
        ),
        (
            Hive,
            Oracle.WriteOptions(truncate=True),
        ),
    ],
    ids=["JDBC connection with Hive options.", "Hive connection with JDBC options."],
)
def test_db_options_parse_mismatch_connection_and_options_types(connection, options):
    with pytest.raises(TypeError):
        connection.WriteOptions.parse(options)


@pytest.mark.parametrize(
    "options_class",
    [
        Postgres.ReadOptions,
        Postgres.WriteOptions,
        Postgres.JDBCOptions,
        Hive.WriteOptions,
        Postgres.Options,
        Hive.Options,
    ],
)
@pytest.mark.parametrize(
    "options",
    [
        {"some", "option"},
        "Some_options",
        123,
        ["Option_1", "Option_2"],
        ("Option_1", "Option_2"),
    ],
    ids=[
        "set",
        "str",
        "int",
        "list",
        "tuple",
    ],
)
def test_db_options_cannot_be_parsed(options_class, options):
    with pytest.raises(
        TypeError,
        match=re.escape(f"{type(options).__name__} is not a {options_class.__name__} instance"),
    ):
        options_class.parse(options)


@pytest.mark.parametrize("hint, real_hint", [(None, ""), ("NOWAIT", " /*+ NOWAIT */")])
@pytest.mark.parametrize(
    "columns, real_columns",
    [
        (None, "*"),
        (["*"], "*"),
        (["d_id", "d_name", "d_age"], "d_id, d_name, d_age"),
    ],
)
@pytest.mark.parametrize("where, real_where", [(None, ""), ("d_id > 100", " WHERE d_id > 100")])
def test_jdbc_get_sql_query(
    hint,
    real_hint,
    columns,
    real_columns,
    where,
    real_where,
):
    conn = Postgres(host="some_host", user="user", password="passwd", database="abc", spark=spark)

    table_sql = conn.get_sql_query(
        table="default.test",
        hint=hint,
        columns=columns,
        where=where,
    )

    assert table_sql == f"SELECT{real_hint} {real_columns} FROM default.test{real_where}"
