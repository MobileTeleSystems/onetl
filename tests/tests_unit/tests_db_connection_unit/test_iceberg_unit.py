from __future__ import annotations

from datetime import timedelta
from unittest.mock import call

import pytest

from onetl import __version__ as onetl_version
from onetl.connection import Iceberg
from onetl.connection.db_connection.iceberg.catalog import IcebergRESTCatalog
from onetl.connection.db_connection.iceberg.catalog.auth import (
    IcebergRESTCatalogBasicAuth,
    IcebergRESTCatalogBearerAuth,
    IcebergRESTCatalogOAuth2,
)
from onetl.connection.db_connection.iceberg.catalog.filesystem import (
    IcebergFilesystemCatalog,
)
from onetl.connection.db_connection.iceberg.warehouse.filesystem import (
    IcebergFilesystemWarehouse,
)
from onetl.connection.file_df_connection.spark_local_fs import SparkLocalFS
from onetl.connection.file_df_connection.spark_s3.connection import SparkS3

pytestmark = [pytest.mark.iceberg, pytest.mark.db_connection, pytest.mark.connection]


def test_iceberg_missing_args(spark_mock):
    # no spark
    with pytest.raises(ValueError, match="field required"):
        Iceberg()

    # no catalog_name
    with pytest.raises(ValueError, match="field required"):
        Iceberg(spark=spark_mock)


def test_iceberg_with_filesystem_catalog_local_connection(spark_mock, iceberg_warehouse_dir):
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=SparkLocalFS(spark=spark_mock),
            path=iceberg_warehouse_dir,
        ),
        spark=spark_mock,
    )
    assert iceberg
    assert iceberg.catalog.get_config(iceberg.warehouse) == {
        "type": "hadoop",
        "warehouse": f"file://{iceberg_warehouse_dir}",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "hadoop"),
        call("spark.sql.catalog.my_catalog.warehouse", f"file://{iceberg_warehouse_dir}"),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_with_filesystem_catalog_hdfs_connection(spark_mock, mocker, iceberg_warehouse_dir):
    from onetl.connection.file_df_connection.spark_hdfs.connection import SparkHDFS

    def conn_str(self):
        return f"hdfs://{self.host}:{self.ipc_port}"

    mocker.patch.object(SparkHDFS, "_get_conn_str", conn_str)

    connection = SparkHDFS(spark=spark_mock, host="namenode", cluster="rnd-dwh")
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=connection,
            path=iceberg_warehouse_dir,
        ),
        spark=spark_mock,
    )
    assert iceberg
    assert iceberg.catalog.get_config(iceberg.warehouse) == {
        "type": "hadoop",
        "warehouse": f"{connection._get_conn_str()}{iceberg_warehouse_dir}",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "hadoop"),
        call("spark.sql.catalog.my_catalog.warehouse", f"{connection._get_conn_str()}{iceberg_warehouse_dir}"),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_with_filesystem_catalog_s3_connection(spark_mock, iceberg_warehouse_dir):
    connection = SparkS3(
        spark=spark_mock,
        host="localhost",
        port=9010,
        bucket="onetl",
        access_key="onetl",
        secret_key="123UsedForTestOnly@!",
        protocol="http",
        path_style_access=True,
    )
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=connection,
            path=iceberg_warehouse_dir,
        ),
        spark=spark_mock,
    )
    assert iceberg
    assert iceberg.catalog.get_config(iceberg.warehouse) == {
        "type": "hadoop",
        "hadoop.fs.s3a.bucket.onetl.access.key": "onetl",
        "hadoop.fs.s3a.bucket.onetl.secret.key": "123UsedForTestOnly@!",
        "hadoop.fs.s3a.bucket.onetl.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "hadoop.fs.s3a.bucket.onetl.connection.ssl.enabled": "false",
        "hadoop.fs.s3a.bucket.onetl.endpoint": "http://localhost:9010",
        "hadoop.fs.s3a.bucket.onetl.path.style.access": "true",
        "hadoop.fs.s3a.user.agent.prefix": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
        "warehouse": f"s3a://onetl{iceberg_warehouse_dir}",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "hadoop"),
        call("spark.sql.catalog.my_catalog.warehouse", f"s3a://onetl{iceberg_warehouse_dir}"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.bucket.onetl.access.key", "onetl"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.bucket.onetl.secret.key", "123UsedForTestOnly@!"),
        call(
            "spark.sql.catalog.my_catalog.hadoop.fs.s3a.bucket.onetl.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        ),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.bucket.onetl.connection.ssl.enabled", "false"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.bucket.onetl.endpoint", "http://localhost:9010"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.bucket.onetl.path.style.access", "true"),
        call(
            "spark.sql.catalog.my_catalog.hadoop.fs.s3a.user.agent.prefix",
            f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
        ),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_with_rest_catalog(spark_mock):
    warehouse = IcebergFilesystemWarehouse(
        connection=SparkLocalFS(spark=spark_mock),
        path="/data",
    )
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergRESTCatalog(
            uri="http://localhost:8080",
            headers={
                "X-Custom-Header": "123",
            },
            extra={
                "hadoop.fs.s3a.endpoint": "http://localhost:9010",
                "hadoop.fs.s3a.access.key": "onetl",
                "hadoop.fs.s3a.secret.key": "123UsedForTestOnly@!",
                "hadoop.fs.s3a.path.style.access": "true",
            },
        ),
        warehouse=warehouse,
        spark=spark_mock,
    )
    assert iceberg
    assert iceberg.catalog.get_config(warehouse) == {
        "type": "rest",
        "uri": "http://localhost:8080",
        "header.X-Custom-Header": "123",
        "warehouse": "file:///data",
        "hadoop.fs.s3a.endpoint": "http://localhost:9010",
        "hadoop.fs.s3a.access.key": "onetl",
        "hadoop.fs.s3a.secret.key": "123UsedForTestOnly@!",
        "hadoop.fs.s3a.path.style.access": "true",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "rest"),
        call("spark.sql.catalog.my_catalog.uri", "http://localhost:8080"),
        call("spark.sql.catalog.my_catalog.warehouse", "file:///data"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.endpoint", "http://localhost:9010"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.access.key", "onetl"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.secret.key", "123UsedForTestOnly@!"),
        call("spark.sql.catalog.my_catalog.hadoop.fs.s3a.path.style.access", "true"),
        call("spark.sql.catalog.my_catalog.header.X-Custom-Header", "123"),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_rest_catalog_missing_args():
    with pytest.raises(ValueError, match="field required"):
        IcebergRESTCatalog()


def test_iceberg_rest_catalog_with_basic_auth(spark_mock):
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergRESTCatalog(
            uri="http://localhost:8080",
            auth=IcebergRESTCatalogBasicAuth(
                user="my_username",
                password="my_password",
            ),
        ),
        warehouse=IcebergFilesystemWarehouse(
            connection=SparkLocalFS(spark=spark_mock),
            path="/data",
        ),
        spark=spark_mock,
    )
    assert iceberg.catalog.auth.get_config() == {
        "rest.auth.type": "basic",
        "rest.auth.basic.username": "my_username",
        "rest.auth.basic.password": "my_password",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "rest"),
        call("spark.sql.catalog.my_catalog.uri", "http://localhost:8080"),
        call("spark.sql.catalog.my_catalog.rest.auth.type", "basic"),
        call("spark.sql.catalog.my_catalog.rest.auth.basic.username", "my_username"),
        call("spark.sql.catalog.my_catalog.rest.auth.basic.password", "my_password"),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_rest_catalog_with_bearer_auth(spark_mock):
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergRESTCatalog(
            uri="http://localhost:8080",
            auth=IcebergRESTCatalogBearerAuth(
                access_token="my_access_token",
            ),
        ),
        warehouse=IcebergFilesystemWarehouse(
            connection=SparkLocalFS(spark=spark_mock),
            path="/data",
        ),
        spark=spark_mock,
    )
    assert iceberg.catalog.auth.get_config() == {
        "rest.auth.type": "oauth2",
        "token": "my_access_token",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "rest"),
        call("spark.sql.catalog.my_catalog.uri", "http://localhost:8080"),
        call("spark.sql.catalog.my_catalog.rest.auth.type", "oauth2"),
        call("spark.sql.catalog.my_catalog.token", "my_access_token"),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_rest_catalog_with_oauth2_secret_only(spark_mock):
    iceberg = Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergRESTCatalog(
            uri="http://localhost:8080",
            auth=IcebergRESTCatalogOAuth2(
                client_secret="my_secret",
            ),
        ),
        warehouse=IcebergFilesystemWarehouse(
            connection=SparkLocalFS(spark=spark_mock),
            path="/data",
        ),
        spark=spark_mock,
    )
    assert iceberg.catalog.auth.get_config() == {
        "rest.auth.type": "oauth2",
        "credential": "my_secret",
        "token-expires-in-ms": "3600000",  # default 1 hour
        "token-refresh-enabled": "true",
        "token-exchange-enabled": "true",
    }
    expected_calls = [
        call("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        call("spark.sql.catalog.my_catalog.type", "rest"),
        call("spark.sql.catalog.my_catalog.uri", "http://localhost:8080"),
        call("spark.sql.catalog.my_catalog.rest.auth.type", "oauth2"),
        call("spark.sql.catalog.my_catalog.credential", "my_secret"),
        call("spark.sql.catalog.my_catalog.token-expires-in-ms", "3600000"),
        call("spark.sql.catalog.my_catalog.token-refresh-enabled", "true"),
        call("spark.sql.catalog.my_catalog.token-exchange-enabled", "true"),
    ]
    spark_mock.conf.set.assert_has_calls(expected_calls, any_order=True)


def test_iceberg_rest_catalog_with_oauth2_client_and_secret():
    catalog = IcebergRESTCatalog(
        uri="http://localhost:8080",
        auth=IcebergRESTCatalogOAuth2(
            client_id="my_client",
            client_secret="my_secret",
            oauth2_server_uri="http://my-server/oauth/tokens",
            token_refresh_interval=timedelta(minutes=30),
            token_exchange_enabled=False,
        ),
    )
    assert catalog.auth.get_config() == {
        "rest.auth.type": "oauth2",
        "credential": "my_client:my_secret",
        "oauth2-server-uri": "http://my-server/oauth/tokens",
        "token-expires-in-ms": "1800000",
        "token-exchange-enabled": "false",
        "token-refresh-enabled": "true",
    }


def test_iceberg_rest_catalog_with_oauth2_extra_fields():
    catalog = IcebergRESTCatalog(
        uri="http://localhost:8080",
        auth=IcebergRESTCatalogOAuth2(
            client_secret="my_secret",
            scopes=["catalog:read", "catalog:write"],
            audience="iceberg-service",
            resource="catalog-api",
        ),
    )
    assert catalog.auth.get_config() == {
        "rest.auth.type": "oauth2",
        "token-refresh-enabled": "true",
        "token-exchange-enabled": "true",
        "token-expires-in-ms": "3600000",
        "credential": "my_secret",
        "audience": "iceberg-service",
        "resource": "catalog-api",
        "scope": "catalog:read catalog:write",
    }


def test_iceberg_rest_catalog_with_oauth2_no_token_refresh():
    catalog = IcebergRESTCatalog(
        uri="http://localhost:8080",
        auth=IcebergRESTCatalogOAuth2(
            client_secret="my_secret",
            token_refresh_interval=None,
        ),
    )
    assert catalog.auth.get_config() == {
        "rest.auth.type": "oauth2",
        "token-refresh-enabled": "false",
        "token-exchange-enabled": "true",
        "credential": "my_secret",
    }


def test_iceberg_instance_url(iceberg_mock):
    assert iceberg_mock.instance_url == "iceberg://my_catalog"


def test_iceberg_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Iceberg(catalog_name="my_catalog", spark=spark_stopped)


@pytest.mark.parametrize(
    "package_version,spark_version,scala_version,package",
    [
        ("1.4.0", "3.3", None, "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0"),
        ("1.10.0", "3.5", "2.12", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"),
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
