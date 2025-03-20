import re

import pytest

from onetl import __version__ as onetl_version
from onetl.connection import Greenplum
from onetl.connection.db_connection.greenplum import GreenplumTableExistBehavior

pytestmark = [pytest.mark.greenplum, pytest.mark.db_connection, pytest.mark.connection]


def test_greenplum_driver():
    assert Greenplum.DRIVER == "org.postgresql.Driver"


def test_greenplum_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `Greenplum.get_packages(spark_version=")
    with pytest.warns(UserWarning, match=warning_msg):
        assert Greenplum.package_spark_2_3 == "io.pivotal:greenplum-spark_2.11:2.2.0"
        assert Greenplum.package_spark_2_4 == "io.pivotal:greenplum-spark_2.11:2.2.0"
        assert Greenplum.package_spark_3_2 == "io.pivotal:greenplum-spark_2.12:2.2.0"


def test_greenplum_get_packages_no_input():
    with pytest.raises(ValueError, match="You should pass either `scala_version` or `spark_version`"):
        Greenplum.get_packages()


@pytest.mark.parametrize(
    "spark_version",
    [
        "2.2",
        "3.3",
        "3.4",
        "3.5",
    ],
)
def test_greenplum_get_packages_spark_version_not_supported(spark_version):
    with pytest.raises(ValueError, match=f"Spark version must be 2.3.x - 3.2.x, got {spark_version}"):
        Greenplum.get_packages(spark_version=spark_version)


@pytest.mark.parametrize(
    "scala_version",
    [
        "2.10",
        "2.13",
        "3.0",
    ],
)
def test_greenplum_get_packages_scala_version_not_supported(scala_version):
    with pytest.raises(ValueError, match=f"Scala version must be 2.11 - 2.12, got {scala_version}"):
        Greenplum.get_packages(scala_version=scala_version)


@pytest.mark.parametrize(
    "spark_version, scala_version, package",
    [
        # use Scala version directly
        (None, "2.11", "io.pivotal:greenplum-spark_2.11:2.2.0"),
        (None, "2.12", "io.pivotal:greenplum-spark_2.12:2.2.0"),
        # Detect Scala version by Spark version
        ("2.3", None, "io.pivotal:greenplum-spark_2.11:2.2.0"),
        ("2.4", None, "io.pivotal:greenplum-spark_2.11:2.2.0"),
        ("3.2", None, "io.pivotal:greenplum-spark_2.12:2.2.0"),
        # Override Scala version detected automatically
        ("2.3", "2.11", "io.pivotal:greenplum-spark_2.11:2.2.0"),
        ("2.4", "2.12", "io.pivotal:greenplum-spark_2.12:2.2.0"),
        # Scala version contain three digits when only two needed
        ("3.2.4", "2.11.1", "io.pivotal:greenplum-spark_2.11:2.2.0"),
    ],
)
def test_greenplum_get_packages(spark_version, scala_version, package):
    assert Greenplum.get_packages(spark_version=spark_version, scala_version=scala_version) == [package]


@pytest.mark.parametrize(
    "package_version, scala_version, package",
    [
        (None, "2.12", "io.pivotal:greenplum-spark_2.12:2.2.0"),
        ("2.3.0", "2.12", "io.pivotal:greenplum-spark_2.12:2.3.0"),
        ("2.1.4", "2.12", "io.pivotal:greenplum-spark_2.12:2.1.4"),
    ],
)
def test_greenplum_get_packages_explicit_version(package_version, scala_version, package):
    assert Greenplum.get_packages(package_version=package_version, scala_version=scala_version) == [package]


def test_greenplum_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'io.pivotal.greenplum.spark.GreenplumRelationProvider'"
    with pytest.raises(ValueError, match=msg):
        Greenplum(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_greenplum_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Greenplum(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_stopped,
        )


def test_greenplum(spark_mock):
    conn = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5432
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://some_host:5432/database",
        "ApplicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
        "tcpKeepAlive": "true",
    }
    assert conn._get_connector_params("some.table") == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": (
            "jdbc:postgresql://some_host:5432/database?"
            f"ApplicationName=local-123%20abc%20onETL%2F{onetl_version}%20Spark%2F{spark_mock.version}&"
            "tcpKeepAlive=true"
        ),
        "dbschema": "some",
        "dbtable": "table",
    }

    assert "passwd" not in repr(conn)

    assert conn.instance_url == "greenplum://some_host:5432/database"
    assert str(conn) == "Greenplum[some_host:5432/database]"


def test_greenplum_with_port(spark_mock):
    conn = Greenplum(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5000/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://some_host:5000/database",
        "ApplicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
        "tcpKeepAlive": "true",
    }
    assert conn._get_connector_params("some.table") == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": (
            "jdbc:postgresql://some_host:5000/database?"
            f"ApplicationName=local-123%20abc%20onETL%2F{onetl_version}%20Spark%2F{spark_mock.version}&"
            "tcpKeepAlive=true"
        ),
        "dbschema": "some",
        "dbtable": "table",
    }

    assert conn.instance_url == "greenplum://some_host:5000/database"
    assert str(conn) == "Greenplum[some_host:5000/database]"


def test_greenplum_without_database_error(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Greenplum(host="some_host", port=5000, user="user", password="passwd", spark=spark_mock)


def test_greenplum_with_extra(spark_mock):
    conn = Greenplum(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={
            "autosave": "always",
            "tcpKeepAlive": "false",
            "ApplicationName": "override",
            "options": "-c search_path=public",
            "server.port": 8000,
            "pool.maxSize": 40,
        },
        spark=spark_mock,
    )

    # `server.*` and `pool.*` options are ignored while generating jdbc_url
    # they are used only in `read_source_as_df` and `write_df_to_target`
    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://some_host:5432/database",
        "ApplicationName": "override",
        "tcpKeepAlive": "false",
        "autosave": "always",
        "options": "-c search_path=public",
    }
    assert conn._get_connector_params("some.table") == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": (
            "jdbc:postgresql://some_host:5432/database?ApplicationName=override&"
            "autosave=always&options=-c%20search_path%3Dpublic&tcpKeepAlive=false"
        ),
        "dbschema": "some",
        "dbtable": "table",
        "pool.maxSize": 40,
        "server.port": 8000,
    }


def test_greenplum_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Greenplum()

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            host="some_host",
            database="database",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            host="some_host",
            database="database",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            host="some_host",
            database="database",
            password="passwd",
            spark=spark_mock,
        )


def test_greenplum_write_options_default():
    options = Greenplum.WriteOptions()

    assert options.if_exists == GreenplumTableExistBehavior.APPEND


@pytest.mark.parametrize(
    "klass, name",
    [
        (Greenplum.ReadOptions, "GreenplumReadOptions"),
        (Greenplum.WriteOptions, "GreenplumWriteOptions"),
        (Greenplum.FetchOptions, "GreenplumFetchOptions"),
        (Greenplum.ExecuteOptions, "GreenplumExecuteOptions"),
        (Greenplum.Extra, "GreenplumExtra"),
    ],
)
def test_greenplum_jdbc_options_populated_by_connection_class(klass, name):
    error_msg = rf"Options \['driver', 'password', 'url', 'user'\] are not allowed to use in a {name}"
    with pytest.raises(ValueError, match=error_msg):
        klass(user="me", password="abc", driver="some.Class", url="jdbc:postgres://some/db")


@pytest.mark.parametrize("options_class", [Greenplum.FetchOptions, Greenplum.ExecuteOptions])
def test_greenplum_read_write_options_populated_by_connection_class(options_class):
    error_msg = r"Options \['dbschema', 'dbtable'\] are not allowed to use in a GreenplumReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.ReadOptions(dbschema="myschema", dbtable="mytable")

    error_msg = r"Options \['dbschema', 'dbtable'\] are not allowed to use in a GreenplumWriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.WriteOptions(dbschema="myschema", dbtable="mytable")

    # FetchOptions & ExecuteOptions does not have such restriction
    options = options_class(dbschema="myschema", dbtable="mytable")
    assert options.dbschema == "myschema"
    assert options.dbtable == "mytable"


@pytest.mark.parametrize(
    "arg, value",
    [
        ("mode", "append"),
        ("truncate", "true"),
        ("distributedBy", "abc"),
        ("iteratorOptimization", "true"),
    ],
)
def test_greenplum_write_options_cannot_be_used_in_read_options(arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a GreenplumReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.ReadOptions.parse({arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("partitions", 10),
        ("numPartitions", 10),
        ("partitionColumn", "abc"),
        ("gpdb.matchDistributionPolicy", "true"),
    ],
)
def test_greenplum_read_options_cannot_be_used_in_write_options(arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a GreenplumWriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.WriteOptions.parse({arg: value})


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, GreenplumTableExistBehavior.APPEND),
        ({"if_exists": "append"}, GreenplumTableExistBehavior.APPEND),
        ({"if_exists": "replace_entire_table"}, GreenplumTableExistBehavior.REPLACE_ENTIRE_TABLE),
        ({"if_exists": "error"}, GreenplumTableExistBehavior.ERROR),
        ({"if_exists": "ignore"}, GreenplumTableExistBehavior.IGNORE),
    ],
)
def test_greenplum_write_options_if_exists(options, value):
    assert Greenplum.WriteOptions(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "append"},
            GreenplumTableExistBehavior.APPEND,
            "Option `Greenplum.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Greenplum.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_table"},
            GreenplumTableExistBehavior.REPLACE_ENTIRE_TABLE,
            "Option `Greenplum.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Greenplum.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            GreenplumTableExistBehavior.REPLACE_ENTIRE_TABLE,
            "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_table` instead",
        ),
        (
            {"mode": "ignore"},
            GreenplumTableExistBehavior.IGNORE,
            "Option `Greenplum.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Greenplum.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "error"},
            GreenplumTableExistBehavior.ERROR,
            "Option `Greenplum.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Greenplum.WriteOptions(if_exists=...)` instead",
        ),
    ],
)
def test_greenplum_write_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = Greenplum.WriteOptions(**options)
        assert options.if_exists == value


def test_greenplum_write_options_mode_wrong():
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        Greenplum.WriteOptions(if_exists="wrong_mode")
