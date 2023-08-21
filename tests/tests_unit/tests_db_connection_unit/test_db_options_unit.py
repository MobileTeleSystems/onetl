import logging
import re

import pytest

from onetl.connection import Greenplum, Hive, Postgres

pytestmark = [pytest.mark.postgres]


@pytest.mark.parametrize(
    "options_class",
    [
        Postgres.ReadOptions,
        Postgres.WriteOptions,
        Postgres.JDBCOptions,
        Postgres.Options,
        Greenplum.ReadOptions,
        Greenplum.WriteOptions,
    ],
)
@pytest.mark.parametrize(
    "arg, value",
    [
        ("url", "jdbc:postgresql://localhost:5432/postgres"),
        ("driver", "org.postgresql.Driver"),
        ("user", "me"),
        ("password", "abc"),
    ],
)
def test_db_options_connection_parameters_cannot_be_passed(options_class, arg, value):
    with pytest.raises(ValueError, match=rf"Options \['{arg}'\] are not allowed to use in a {options_class.__name__}"):
        options_class.parse({arg: value})


@pytest.mark.parametrize(
    "options_class, options_class_name, known_options",
    [
        (Hive.WriteOptions, "HiveWriteOptions", {"if_exists": "replace_overlapping_partitions"}),
        (Hive.Options, "HiveLegacyOptions", {"if_exists": "replace_overlapping_partitions"}),
        (Postgres.ReadOptions, "JDBCReadOptions", {"fetchsize": 10, "keytab": "a/b/c"}),
        (Postgres.WriteOptions, "JDBCWriteOptions", {"if_exists": "replace_entire_table", "keytab": "a/b/c"}),
        (Postgres.Options, "JDBCLegacyOptions", {"if_exists": "replace_entire_table", "keytab": "a/b/c"}),
        (Greenplum.ReadOptions, "GreenplumReadOptions", {"partitions": 10}),
        (Greenplum.WriteOptions, "GreenplumWriteOptions", {"if_exists": "replace_entire_table"}),
    ],
)
def test_db_options_warn_for_unknown(options_class, options_class_name, known_options, caplog):
    with caplog.at_level(logging.WARNING):
        options_class(some_unknown_option="value", **known_options)
        assert (
            f"Options ['some_unknown_option'] are not known by {options_class_name}, are you sure they are valid?"
        ) in caplog.text

        options_class(option1="value1", option2=None, **known_options)
        assert (
            f"Options ['option1', 'option2'] are not known by {options_class_name}, are you sure they are valid?"
        ) in caplog.text

        for known_option in known_options:
            assert known_option not in caplog.text


@pytest.mark.parametrize(
    "options_class,options",
    [
        (
            Postgres.ReadOptions,
            Postgres.WriteOptions(),
        ),
        (
            Postgres.WriteOptions,
            Postgres.ReadOptions(),
        ),
    ],
    ids=[
        "Write options object passed to ReadOptions",
        "Read options object passed to WriteOptions",
    ],
)
def test_db_options_parse_mismatch_class(options_class, options):
    with pytest.raises(TypeError):
        options_class.parse(options)


@pytest.mark.parametrize(
    "connection,options",
    [
        (
            Postgres,
            Hive.WriteOptions(format="orc"),
        ),
        (
            Hive,
            Postgres.WriteOptions(truncate=True),
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
        Greenplum.ReadOptions,
        Greenplum.WriteOptions,
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
