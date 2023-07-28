import os
import shutil
from pathlib import Path

import pytest

from onetl._util.spark import get_pyspark_version


@pytest.fixture(scope="session")
def warehouse_dir(tmp_path_factory):
    # https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
    path = tmp_path_factory.mktemp("spark-warehouse")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def spark_metastore_dir(tmp_path_factory):
    # https://stackoverflow.com/a/44048667
    path = tmp_path_factory.mktemp("metastore_db")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def ivysettings_path():
    path = Path(__file__).parent.parent / "ivysettings.xml"
    assert path.exists()
    return path


@pytest.fixture(scope="session")
def spark_packages():
    from onetl.connection import (
        MSSQL,
        Clickhouse,
        Greenplum,
        Kafka,
        MongoDB,
        MySQL,
        Oracle,
        Postgres,
        Teradata,
    )

    pyspark_version = get_pyspark_version()
    packages = [
        Clickhouse.package,
        MSSQL.package,
        MySQL.package,
        Oracle.package,
        Postgres.package,
        Teradata.package,
        ",".join(Kafka.get_package_spark(spark_version=str(pyspark_version))),
    ]

    with_greenplum = os.getenv("ONETL_DB_WITH_GREENPLUM", "false").lower() == "true"

    if pyspark_version.digits(2) == (2, 3):
        if with_greenplum:
            packages.extend([Greenplum.package_spark_2_3])
        return packages

    if pyspark_version.digits(2) == (2, 4):
        if with_greenplum:
            packages.extend([Greenplum.package_spark_2_4])
        return packages

    if pyspark_version.digits(2) == (3, 2):
        packages.extend([MongoDB.package_spark_3_2])
        if with_greenplum:
            packages.extend([Greenplum.package_spark_3_2])
        return packages

    if pyspark_version.digits(2) == (3, 3):
        packages.extend([MongoDB.package_spark_3_3])
        if not with_greenplum:
            return packages

        raise ValueError(f"Greenplum connector does not support Spark {pyspark_version}")

    if pyspark_version.digits(2) == (3, 4):
        packages.extend([MongoDB.package_spark_3_4])
        if not with_greenplum:
            return packages

        raise ValueError(f"Greenplum connector does not support Spark {pyspark_version}")

    raise ValueError(f"Unsupported Spark version: {pyspark_version}")


@pytest.fixture(
    scope="session",
    name="spark",
    params=[
        pytest.param("real-spark", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def get_spark_session(warehouse_dir, spark_metastore_dir, ivysettings_path, spark_packages):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")  # noqa: WPS221
        .config("spark.master", "local[*]")
        .config("spark.jars.packages", ",".join(spark_packages))
        .config("spark.jars.ivySettings", os.fspath(ivysettings_path))
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.allowSparkContext", "true")  # Greenplum uses SparkContext on executor if master==local
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={os.fspath(spark_metastore_dir)}")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )

    yield spark
    spark.sparkContext.stop()
    spark.stop()
