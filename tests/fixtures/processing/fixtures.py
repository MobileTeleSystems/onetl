import secrets
from collections import namedtuple
from importlib import import_module

import pytest
from pytest_lazyfixture import lazy_fixture

from onetl.connection.db_connection.iceberg.catalog.filesystem import (
    IcebergFilesystemCatalog,
)
from onetl.connection.db_connection.iceberg.connection import Iceberg
from onetl.connection.db_connection.iceberg.warehouse.filesystem import (
    IcebergFilesystemWarehouse,
)
from onetl.connection.file_df_connection.spark_local_fs import SparkLocalFS

PreparedDbInfo = namedtuple("PreparedDbInfo", ["full_name", "schema", "table"])


@pytest.fixture()
def processing(request, spark):
    processing_classes = {
        "clickhouse": ("tests.fixtures.processing.clickhouse", "ClickhouseProcessing"),
        "greenplum": ("tests.fixtures.processing.greenplum", "GreenplumProcessing"),
        "hive": ("tests.fixtures.processing.hive", "HiveProcessing"),
        "iceberg": ("tests.fixtures.processing.iceberg", "IcebergProcessing"),
        "mongodb": ("tests.fixtures.processing.mongodb", "MongoDBProcessing"),
        "mssql": ("tests.fixtures.processing.mssql", "MSSQLProcessing"),
        "mysql": ("tests.fixtures.processing.mysql", "MySQLProcessing"),
        "oracle": ("tests.fixtures.processing.oracle", "OracleProcessing"),
        "postgres": ("tests.fixtures.processing.postgres", "PostgresProcessing"),
        "kafka": ("tests.fixtures.processing.kafka", "KafkaProcessing"),
    }

    test_name_parts = set(request.function.__name__.split("_"))
    matches = set(processing_classes.keys()) & test_name_parts
    if not matches or len(matches) > 1:
        raise ValueError(
            f"Test name {request.function.__name__} should have one of these components: {list(processing_classes.keys())}",
        )

    db_storage_name = matches.pop()
    module_name, class_name = processing_classes[db_storage_name]
    module = import_module(module_name)
    db_processing = getattr(module, class_name)

    if db_storage_name in ("hive", "iceberg"):
        yield db_processing(spark)
    else:
        with db_processing() as result:
            yield result


@pytest.fixture
def get_schema_table(processing):
    schema = processing.schema
    processing.create_schema(schema=schema)

    table = f"test_{secrets.token_hex(5)}"
    full_name = f"{schema}.{table}"

    yield PreparedDbInfo(full_name=full_name, schema=schema, table=table)

    try:
        processing.drop_table(
            table=table,
            schema=schema,
        )
    except Exception:  # noqa: S110
        pass


@pytest.fixture
def prepare_schema_table(processing, get_schema_table):
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    _, schema, table = get_schema_table

    processing.create_table(schema=schema, table=table, fields=fields)

    return get_schema_table


@pytest.fixture
def load_table_data(prepare_schema_table, processing):
    _, schema, table = prepare_schema_table

    processing.insert_data(
        schema=schema,
        table=table,
        values=processing.create_pandas_df(),
    )

    return prepare_schema_table


@pytest.fixture
def iceberg_connection_local_fs(spark, iceberg_warehouse_dir, local_fs_file_df_connection):
    iceberg = Iceberg(
        spark=spark,
        catalog_name="hadoop",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=local_fs_file_df_connection,
            path=iceberg_warehouse_dir,
        ),
    )
    return iceberg


@pytest.fixture
def iceberg_connection_hdfs(spark, iceberg_warehouse_dir, hdfs_file_df_connection):
    iceberg = Iceberg(
        spark=spark,
        catalog_name="hadoop",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=hdfs_file_df_connection,
            path=iceberg_warehouse_dir,
        ),
    )
    return iceberg


@pytest.fixture
def iceberg_connection_s3(spark, iceberg_warehouse_dir, s3_file_df_connection):
    iceberg = Iceberg(
        spark=spark,
        catalog_name="hadoop",
        catalog=IcebergFilesystemCatalog(),
        warehouse=IcebergFilesystemWarehouse(
            connection=s3_file_df_connection,
            path=iceberg_warehouse_dir,
        ),
    )
    return iceberg


@pytest.fixture(
    params=[
        lazy_fixture("iceberg_connection_local_fs"),
        lazy_fixture("iceberg_connection_hdfs"),
        lazy_fixture("iceberg_connection_s3"),
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
def kafka_topic(processing, request):
    topic = secrets.token_hex(6)
    processing.create_topic(topic, num_partitions=1)

    def delete_topic():
        processing.delete_topic([topic])

    request.addfinalizer(delete_topic)
    return topic


@pytest.fixture
def kafka_dataframe_schema():
    from pyspark.sql.types import (
        FloatType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("id_int", LongType(), nullable=True),
            StructField("text_string", StringType(), nullable=True),
            StructField("hwm_int", LongType(), nullable=True),
            StructField("float_value", FloatType(), nullable=True),
        ],
    )
