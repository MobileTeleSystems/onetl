import os
from collections import namedtuple

import pytest
from pytest_lazyfixture import lazy_fixture

from onetl.connection.db_connection.iceberg.catalog.filesystem import (
    IcebergFilesystemCatalog,
)
from onetl.connection.db_connection.iceberg.catalog.rest import IcebergRESTCatalog
from onetl.connection.db_connection.iceberg.connection import Iceberg
from onetl.connection.db_connection.iceberg.warehouse.filesystem import (
    IcebergFilesystemWarehouse,
)
from onetl.connection.db_connection.iceberg.warehouse.s3 import IcebergS3Warehouse
from onetl.connection.file_df_connection.spark_local_fs import SparkLocalFS


@pytest.fixture(
    params=[
        pytest.param("fs-catalog", marks=[pytest.mark.fs_catalog]),
    ],
)
def iceberg_connection_fs_catalog_local_fs_warehouse(spark, iceberg_warehouse_dir, local_fs_file_df_connection):
    iceberg = Iceberg(
        spark=spark,
        catalog_name="my_fs_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=local_fs_file_df_connection,
            path=iceberg_warehouse_dir,
        ),
    )
    return iceberg


@pytest.fixture(
    params=[
        pytest.param("fs-catalog", marks=[pytest.mark.fs_catalog]),
    ],
)
def iceberg_connection_fs_catalog_hdfs_warehouse(spark, iceberg_warehouse_dir, hdfs_file_df_connection):
    iceberg = Iceberg(
        spark=spark,
        catalog_name="my_fs_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=hdfs_file_df_connection,
            path=iceberg_warehouse_dir,
        ),
    )
    return iceberg


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("iceberg-rest-catalog", marks=[pytest.mark.iceberg]),
    ],
)
def iceberg_rest_catalog_server():
    IcebergRESTCatalog = namedtuple("IcebergRESTCatalog", ["host", "port"])

    return IcebergRESTCatalog(
        host=os.getenv("ONETL_ICEBERG_REST_CATALOG_HOST"),
        port=os.getenv("ONETL_ICEBERG_REST_CATALOG_PORT"),
    )


@pytest.fixture(
    params=[
        pytest.param("fs-catalog", marks=[pytest.mark.fs_catalog]),
    ],
)
def iceberg_connection_fs_catalog_s3_warehouse(spark, iceberg_warehouse_dir, s3_file_df_connection):
    iceberg = Iceberg(
        spark=spark,
        catalog_name="my_fs_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=s3_file_df_connection,
            path=iceberg_warehouse_dir,
        ),
    )
    return iceberg


@pytest.fixture(
    params=[
        pytest.param("rest-catalog", marks=[pytest.mark.rest_catalog]),
    ],
)
def iceberg_connection_rest_catalog_s3_warehouse(
    spark,
    s3_file_df_connection,
    iceberg_rest_catalog_server,
):
    s3 = s3_file_df_connection
    iceberg = Iceberg(
        spark=spark,
        catalog_name="my_rest_catalog",
        catalog=IcebergRESTCatalog(
            uri=f"http://{iceberg_rest_catalog_server.host}:{iceberg_rest_catalog_server.port}",
        ),
        warehouse=IcebergS3Warehouse(
            path="/data",
            host=s3.host,
            port=s3.port,
            protocol=s3.protocol,
            bucket=s3.bucket,
            path_style_access=s3.path_style_access,
            access_key=s3.access_key,
            secret_key=s3.secret_key,
            region=s3.region,
        ),
    )
    return iceberg


@pytest.fixture(
    params=[
        lazy_fixture("iceberg_connection_fs_catalog_local_fs_warehouse"),
        lazy_fixture("iceberg_connection_fs_catalog_hdfs_warehouse"),
        lazy_fixture("iceberg_connection_fs_catalog_s3_warehouse"),
        lazy_fixture("iceberg_connection_rest_catalog_s3_warehouse"),
    ],
)
def iceberg_connection(request):
    return request.param


@pytest.fixture
def iceberg_mock(spark_mock):
    return Iceberg(
        catalog_name="my_catalog",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=SparkLocalFS(spark=spark_mock),
            path="/data",
        ),
        spark=spark_mock,
    )


@pytest.fixture
def processing_after_connection(iceberg_connection, processing):
    return processing
