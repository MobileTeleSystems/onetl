from __future__ import annotations

import pytest

from onetl.connection import Iceberg
from onetl.connection.db_connection.iceberg.catalog.rest import IcebergRESTCatalog

pytestmark = [pytest.mark.iceberg, pytest.mark.db_connection, pytest.mark.connection]


def test_iceberg_missing_args(spark_mock):
    # no spark
    with pytest.raises(ValueError, match="field required"):
        Iceberg()

    # no catalog_name
    with pytest.raises(ValueError, match="field required"):
        Iceberg(spark=spark_mock)


def test_iceberg_with_rest_catalog(spark_mock):
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergRESTCatalog(
            uri="http://localhost:8080",
            headers={
                "X-Custom-Header": "123",
            },
            extra={
                "warehouse": "s3a://bucket/",
                "hadoop.fs.s3a.endpoint": "http://localhost:9010",
                "hadoop.fs.s3a.access.key": "onetl",
                "hadoop.fs.s3a.secret.key": "123UsedForTestOnly@!",
                "hadoop.fs.s3a.path.style.access": "true",
            },
        ),
        spark=spark_mock,
    )
    assert iceberg
    assert iceberg.catalog.get_config() == {
        "type": "rest",
        "uri": "http://localhost:8080",
        "header.X-Custom-Header": "123",
        "warehouse": "s3a://bucket/",
        "hadoop.fs.s3a.endpoint": "http://localhost:9010",
        "hadoop.fs.s3a.access.key": "onetl",
        "hadoop.fs.s3a.secret.key": "123UsedForTestOnly@!",
        "hadoop.fs.s3a.path.style.access": "true",
    }


def test_iceberg_rest_catalog_missing_args():
    with pytest.raises(ValueError, match="field required"):
        IcebergRESTCatalog()


def test_iceberg_instance_url(spark_mock):
    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)
    assert iceberg.instance_url == "iceberg://my_catalog"


def test_iceberg_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Iceberg(catalog_name="my_catalog", spark=spark_stopped)


@pytest.mark.parametrize(
    "package_version,spark_version,scala_version,package",
    [
        ("1.4.0", "3.3", None, "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0"),
        ("1.9.2", "3.5", "2.12", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"),
    ],
)
def test_iceberg_get_packages(package_version, spark_version, scala_version, package):
    assert Iceberg.get_packages(
        package_version=package_version,
        spark_version=spark_version,
        scala_version=scala_version,
    ) == [package]


@pytest.mark.local_fs
def test_iceberg_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.apache.iceberg.spark.SparkSessionCatalog'"
    with pytest.raises(ValueError, match=msg):
        Iceberg(spark=spark_no_packages, catalog_name="my_catalog")
