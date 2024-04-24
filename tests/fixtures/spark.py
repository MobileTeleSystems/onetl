import os
import shutil
from pathlib import Path

import pytest

from onetl._util.spark import get_pyspark_version
from onetl._util.version import Version


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
def maven_packages(request):
    from onetl.connection import (
        MSSQL,
        Clickhouse,
        Greenplum,
        Kafka,
        MongoDB,
        MySQL,
        Oracle,
        Postgres,
        SparkS3,
        Teradata,
    )
    from onetl.file.format import XML, Avro, Excel

    pyspark_version = get_pyspark_version()
    packages: list[str] = []

    # get markers from all downstream tests
    markers = set()
    for func in request.session.items:
        markers.update(marker.name for marker in func.iter_markers())

    if "clickhouse" in markers:
        packages.extend(Clickhouse.get_packages())

    if "mssql" in markers:
        packages.extend(MSSQL.get_packages())

    if "mysql" in markers:
        packages.extend(MySQL.get_packages())

    if "oracle" in markers:
        packages.extend(Oracle.get_packages())

    if "postgres" in markers:
        packages.extend(Postgres.get_packages())

    if "teradata" in markers:
        packages.extend(Teradata.get_packages())

    if "greenplum" in markers:
        packages.extend(
            Greenplum.get_packages(
                spark_version=str(pyspark_version),
                package_version=os.getenv("ONETL_GP_PACKAGE_VERSION") or None,
            ),
        )

    if pyspark_version >= Version("2.4"):
        if "avro" in markers:
            # There is no Avro package for Spark 2.3
            packages.extend(Avro.get_packages(spark_version=str(pyspark_version)))
        if "kafka" in markers:
            # Kafka connector for Spark 2.3 is too old and not supported
            packages.extend(Kafka.get_packages(spark_version=str(pyspark_version)))

    if pyspark_version >= Version("3.2"):
        if "s3" in markers:
            # There is no SparkS3 connector for Spark less than 3
            packages.extend(SparkS3.get_packages(spark_version=str(pyspark_version)))

        if "xml" in markers:
            # There is no XML files support for Spark less than 3
            packages.extend(XML.get_packages(spark_version=str(pyspark_version)))

        if "mongodb" in markers:
            # There is no MongoDB connector for Spark less than 3.2
            packages.extend(MongoDB.get_packages(spark_version=str(pyspark_version)))

        if "excel" in markers:
            # There is no Excel files support for Spark less than 3.2
            packages.extend(Excel.get_packages(spark_version=str(pyspark_version)))

    return packages


@pytest.fixture(scope="session")
def excluded_packages():
    # These packages are a part of org.apache.spark:spark-hadoop-cloud, but not used in tests
    return [
        "com.google.cloud.bigdataoss:gcs-connector",
        "org.apache.hadoop:hadoop-aliyun",
        "org.apache.hadoop:hadoop-azure-datalake",
        "org.apache.hadoop:hadoop-azure",
    ]


@pytest.fixture(
    scope="session",
    name="spark",
    params=[
        pytest.param("real-spark", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def get_spark_session(warehouse_dir, spark_metastore_dir, ivysettings_path, maven_packages, excluded_packages):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")  # noqa: WPS221
        .config("spark.master", "local[*]")
        .config("spark.jars.packages", ",".join(maven_packages))
        .config("spark.jars.excludes", ",".join(excluded_packages))
        .config("spark.jars.ivySettings", os.fspath(ivysettings_path))
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")  # prevent Spark from unreachable network connection
        .config("spark.driver.host", "127.0.0.1")
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
