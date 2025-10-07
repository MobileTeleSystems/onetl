import secrets
from collections import namedtuple
from importlib import import_module

import pytest

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
        yield db_processing(spark, request)
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
