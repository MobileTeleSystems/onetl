import logging
import os

import pytest

from onetl.connection import Iceberg
from onetl.db import DBWriter

pytestmark = pytest.mark.iceberg


def test_iceberg_writer(spark, iceberg_connection, processing_after_connection, get_schema_table):
    processing = processing_after_connection
    df = processing.create_spark_df(spark)
    table = f"{iceberg_connection.catalog_name}.{get_schema_table.full_name}"

    writer = DBWriter(
        connection=iceberg_connection,
        target=get_schema_table.full_name,
    )
    writer.run(df)

    ddl = iceberg_connection.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]
    assert "USING iceberg" in ddl

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )


def test_iceberg_writer_with_custom_location(
    spark,
    iceberg_connection_rest_catalog_s3_warehouse,
    processing,
    get_schema_table,
):
    connection = iceberg_connection_rest_catalog_s3_warehouse
    df = processing.create_spark_df(spark)
    table = f"{connection.catalog_name}.{get_schema_table.full_name}"
    location = "s3a://" + os.path.join(
        connection.warehouse.bucket,
        connection.warehouse.path.as_posix().lstrip("/"),
        get_schema_table.schema,
        get_schema_table.table,
        "custom",
    )

    writer = DBWriter(
        connection=connection,
        target=get_schema_table.full_name,
        options=Iceberg.WriteOptions(
            table_properties={
                "location": location,
            },
        ),
    )
    writer.run(df)

    ddl = connection.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]
    assert "USING iceberg" in ddl
    assert f"LOCATION '{location}'" in ddl

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )


# TODO: add test cases when table exist
@pytest.mark.parametrize(
    "if_exists",
    [
        "append",
        "replace_overlapping_partitions",
        "replace_entire_table",
    ],
)
def test_iceberg_writer_target_does_not_exist(
    spark,
    iceberg_connection_fs_catalog_local_fs_warehouse,
    processing,
    get_schema_table,
    if_exists,
    caplog,
):
    connection = iceberg_connection_fs_catalog_local_fs_warehouse
    df = processing.create_spark_df(spark)

    writer = DBWriter(
        connection=connection,
        target=get_schema_table.full_name,  # new table
        options=Iceberg.WriteOptions(if_exists=if_exists),
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)
        table = f"{connection.catalog_name}.{get_schema_table.full_name}"
        assert f"|Iceberg| Saving data to a table '{table}'" in caplog.text
        assert f"|Iceberg| Table '{table}' is successfully created" in caplog.text

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )
