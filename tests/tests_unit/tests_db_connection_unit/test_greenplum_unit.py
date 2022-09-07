from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl.connection import Greenplum
from onetl.connection.db_connection.greenplum import GreenplumWriteMode

spark = Mock(spec=SparkSession)
spark.sparkContext = Mock()
spark.sparkContext.appName = "abc"


def test_greenplum_class_attributes():
    assert Greenplum.driver == "org.postgresql.Driver"
    assert Greenplum.package_spark_2_3 == "io.pivotal:greenplum-spark_2.11:2.1.2"
    assert Greenplum.package_spark_2_4 == "io.pivotal:greenplum-spark_2.11:2.1.2"
    assert Greenplum.package_spark_3_2 == "io.pivotal:greenplum-spark_2.12:2.1.2"
    assert Greenplum.package_spark_3_3 == "io.pivotal:greenplum-spark_2.12:2.1.2"


def test_greenplum():
    conn = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 5432
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database?ApplicationName=abc&tcpKeepAlive=true"


def test_greenplum_with_port():
    conn = Greenplum(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5000/database?ApplicationName=abc&tcpKeepAlive=true"


def test_greenplum_without_database_error():
    with pytest.raises(ValueError, match="field required"):
        Greenplum(host="some_host", port=5000, user="user", password="passwd", spark=spark)


def test_greenplum_with_extra():
    conn = Greenplum(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={
            "autosave": "always",
            "tcpKeepAlive": "false",
            "ApplicationName": "override",
            "server.port": 8000,
            "pool.maxSize": 40,
        },
        spark=spark,
    )

    # `server.*` and `pool.*` options are ignored while generating jdbc_url
    # they are used only in `read_table` and `save_df`
    assert conn.jdbc_url == (
        "jdbc:postgresql://some_host:5432/database?ApplicationName=override&autosave=always&tcpKeepAlive=false"
    )


def test_greenplum_without_mandatory_args():
    with pytest.raises(ValueError, match="field required"):
        Greenplum()

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            host="some_host",
            database="database",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            host="some_host",
            database="database",
            user="user",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Greenplum(
            host="some_host",
            database="database",
            password="passwd",
            spark=spark,
        )


def test_greenplum_write_options_default():
    options = Greenplum.WriteOptions()

    assert options.mode == GreenplumWriteMode.APPEND
    assert options.query_timeout is None


def test_greenplum_read_write_options_populated_by_connection_class():
    error_msg = "Options 'dbschema', 'dbtable' are not allowed to use in a ReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.ReadOptions(dbschema="myschema", dbtable="mytable")

    error_msg = "Options 'dbschema', 'dbtable' are not allowed to use in a WriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.WriteOptions(dbschema="myschema", dbtable="mytable")

    error_msg = "Options 'dbschema', 'dbtable' are not allowed to use in a Extra"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.Extra(dbschema="myschema", dbtable="mytable")

    # JDBCOptions does not have such restriction
    options = Greenplum.JDBCOptions(dbschema="myschema", dbtable="mytable")
    assert options.dbschema == "myschema"
    assert options.dbtable == "mytable"


@pytest.mark.parametrize(
    "options_class",
    [
        Greenplum.ReadOptions,
        Greenplum.WriteOptions,
    ],
)
@pytest.mark.parametrize(
    "arg, value",
    [
        ("server.port", 8000),
        ("pool.maxSize", "40"),
    ],
)
def test_greenplum_read_write_options_prohibited(arg, value, options_class):
    with pytest.raises(ValueError, match=f"Option '{arg}' is not allowed to use in a {options_class.__name__}"):
        options_class(**{arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("mode", "append"),
        ("truncate", "true"),
        ("distributedBy", "abc"),
        ("distributed_by", "abc"),
        ("iteratorOptimization", "true"),
        ("iterator_optimization", "true"),
    ],
)
def test_greenplum_write_options_cannot_be_used_in_read_options(arg, value):
    error_msg = f"Option '{arg}' is not allowed to use in a ReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.ReadOptions(**{arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("partitions", 10),
        ("num_partitions", 10),
        ("numPartitions", 10),
        ("partitionColumn", "abc"),
        ("partition_column", "abc"),
    ],
)
def test_greenplum_read_options_cannot_be_used_in_write_options(arg, value):
    error_msg = f"Option '{arg}' is not allowed to use in a WriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Greenplum.WriteOptions(**{arg: value})


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
def test_greenplum_write_options_wrong_mode(options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        Greenplum.WriteOptions(**options)
