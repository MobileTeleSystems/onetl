import os
import shutil
from pathlib import Path

import pytest
from filelock import FileLock

from onetl._util.version import Version


@pytest.fixture(scope="session")
def warehouse_dir(tmp_path_factory, worker_id):
    # https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
    path = tmp_path_factory.mktemp("spark-warehouse") / worker_id
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def spark_metastore_dir(tmp_path_factory, worker_id):
    # https://stackoverflow.com/a/44048667
    path = tmp_path_factory.mktemp("metastore-db") / worker_id
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def ivysettings_path():
    path = Path(__file__).parent.parent / "ivysettings.xml"
    assert path.exists()
    return path


@pytest.fixture(scope="session")
def maven_packages(request):  # noqa: C901, PLR0912
    import pyspark

    from onetl.connection import (
        MSSQL,
        Clickhouse,
        Greenplum,
        Iceberg,
        Kafka,
        MongoDB,
        MySQL,
        Oracle,
        Postgres,
        SparkS3,
    )
    from onetl.file.format import XML, Avro, Excel

    pyspark_version = Version(pyspark.__version__)
    version = (pyspark_version.major, pyspark_version.minor)

    # get markers from all downstream tests
    markers = set()
    for func in request.session.items:
        markers.update(marker.name for marker in func.iter_markers())

    packages: list[str] = []
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

    gp_package_version = os.getenv("ONETL_GP_PACKAGE_VERSION")
    if "greenplum" in markers and gp_package_version != "local":
        packages.extend(
            Greenplum.get_packages(package_version=gp_package_version),
        )

    if "avro" in markers:
        packages.extend(Avro.get_packages())

    if {"kafka", "kafka_oauth"} & markers:
        packages.extend(Kafka.get_packages())

    if "s3" in markers:
        packages.extend(SparkS3.get_packages())

    if "xml" in markers:
        packages.extend(XML.get_packages())

    if "mongodb" in markers:
        packages.extend(MongoDB.get_packages())

    if "excel" in markers:
        # There are package versions only for specific Spark versions,
        # see https://github.com/nightscape/spark-excel/issues/902
        if version == (3, 2):
            packages.extend(Excel.get_packages(package_version="0.31.2", spark_version="3.2.4"))
        elif version == (3, 3):
            packages.extend(Excel.get_packages(package_version="0.31.2", spark_version="3.3.4"))
        elif version == (3, 4):
            packages.extend(Excel.get_packages(package_version="0.31.2", spark_version="3.4.4"))
        elif version == (3, 5):
            packages.extend(Excel.get_packages(package_version="0.31.2", spark_version="3.5.6"))
        elif version == (4, 0):
            packages.extend(Excel.get_packages(package_version="0.31.2", spark_version="4.0.0"))

    # There is no package for Iceberg Runtime for Spark 4.2 for now
    if "iceberg" in markers and version < (4, 2):
        if version == (3, 2):
            # https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-3.2_2.12/
            iceberg_version = "1.4.3"
        elif version == (3, 3):
            # https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-3.3_2.12/
            iceberg_version = "1.8.1"
        else:
            iceberg_version = "1.11.0"

        packages.extend(Iceberg.get_packages(package_version=iceberg_version))
        if "s3" in markers:
            packages.extend(Iceberg.S3Warehouse.get_packages(package_version=iceberg_version))

    return packages


@pytest.fixture(scope="session")
def excluded_packages():
    from onetl.connection import Kafka, SparkS3

    return [
        *SparkS3.get_exclude_packages(),
        *Kafka.get_exclude_packages(),
    ]


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("real-spark", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def spark(
    warehouse_dir,
    spark_metastore_dir,
    ivysettings_path,
    maven_packages,
    excluded_packages,
    worker_id,
    tmp_path_factory,
):
    from pyspark.sql import SparkSession

    spark_builder = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.master", "local[*]")
        .config("spark.jars.packages", ",".join(maven_packages))
        .config("spark.jars.excludes", ",".join(excluded_packages))
        .config("spark.jars.ivySettings", os.fspath(ivysettings_path))
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.allowSparkContext", "true")  # Greenplum uses SparkContext on executor if master==local
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        .config("spark.default.parallelism", "1")
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={os.fspath(spark_metastore_dir)}",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
    )

    if worker_id == "master":
        spark = spark_builder.getOrCreate()
    else:
        # https://pytest-xdist.readthedocs.io/en/stable/how-to.html#making-session-scoped-fixtures-execute-only-once
        # Parallel start of Spark sessions can fail to write same files into ~/.ivy2 cache
        # So we starting sessions one by one
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        with FileLock(root_tmp_dir / "spark_session.lock"):
            spark = spark_builder.getOrCreate()

    yield spark
    spark.sparkContext.stop()
    spark.stop()


@pytest.fixture(scope="session")
def iceberg_warehouse_dir(tmp_path_factory, worker_id):
    path = tmp_path_factory.mktemp("iceberg-warehouse") / worker_id
    # Iceberg warehouse dir should be created beforehand
    path.mkdir(exist_ok=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)
