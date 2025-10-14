import logging
import os
import re

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


@pytest.mark.parametrize(
    "if_exists",
    [
        "replace_entire_table",
        "replace_overlapping_partitions",
    ],
)
def test_iceberg_writer_target_exists_replace(
    spark,
    iceberg_connection_fs_catalog_local_fs_warehouse,
    processing,
    load_table_data,
    if_exists,
    caplog,
):
    connection = iceberg_connection_fs_catalog_local_fs_warehouse
    df = processing.create_spark_df(spark=spark)

    writer = DBWriter(
        connection=connection,
        target=load_table_data.full_name,
        options=Iceberg.WriteOptions(if_exists=if_exists),
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
        order_by="id_int",
    )


def test_iceberg_writer_table_exists_ignore(
    spark,
    iceberg_connection_fs_catalog_local_fs_warehouse,
    processing,
    load_table_data,
):
    connection = iceberg_connection_fs_catalog_local_fs_warehouse
    df = processing.create_spark_df(spark=spark)

    writer = DBWriter(
        connection=connection,
        target=load_table_data.full_name,
        options=Iceberg.WriteOptions(if_exists="ignore"),
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=processing.get_expected_dataframe(load_table_data.schema, load_table_data.table),
        order_by="id_int",
    )


def test_iceberg_writer_table_exists_append(
    spark,
    iceberg_connection_fs_catalog_local_fs_warehouse,
    processing,
    get_schema_table,
    caplog,
):
    df = processing.create_spark_df(spark=spark)
    df1 = df[df.id_int <= 25]
    df2 = df.where("id_int > 25 AND id_int <= 50")
    df3 = df[df.id_int > 50]
    connection = iceberg_connection_fs_catalog_local_fs_warehouse
    table = f"{connection.catalog_name}.{get_schema_table.full_name}"

    writer1 = DBWriter(
        connection=connection,
        target=get_schema_table.full_name,
        options=Iceberg.WriteOptions(if_exists="append"),
    )
    writer1.run(df1.union(df2))

    writer2 = DBWriter(
        connection=connection,
        target=get_schema_table.full_name,
        options=Iceberg.WriteOptions(if_exists="append"),
    )
    with caplog.at_level(logging.INFO):
        writer2.run(df1.union(df3))

        assert f"|Iceberg| Inserting data into existing table '{table}' ..." in caplog.text
        assert f"|Iceberg| Data is successfully inserted into table '{table}'." in caplog.text

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1.union(df1).union(df2).union(df3),
        order_by="id_int",
    )


@pytest.mark.parametrize(
    "table_name_case_modifier",
    [
        pytest.param(lambda x: x, id="original_name"),
        pytest.param(lambda x: x.upper(), id="upper_case_name"),
    ],
)
def test_iceberg_writer_table_exists_error(
    spark,
    iceberg_connection_fs_catalog_local_fs_warehouse,
    processing,
    get_schema_table,
    table_name_case_modifier,
):
    df = processing.create_spark_df(spark=spark)
    connection = iceberg_connection_fs_catalog_local_fs_warehouse
    table = f"{connection.catalog_name}.{get_schema_table.full_name}"

    writer1 = DBWriter(
        connection=connection,
        target=get_schema_table.full_name,
    )

    writer1.run(df)
    old_ddl = connection.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]

    new_table_name = table_name_case_modifier(get_schema_table.table)

    writer2 = DBWriter(
        connection=connection,
        target=f"{get_schema_table.schema}.{new_table_name}",
        options=Iceberg.WriteOptions(if_exists="error"),
    )

    with pytest.raises(
        ValueError,
        match=re.escape("Operation stopped due to Iceberg.WriteOptions(if_exists='error')"),
    ):
        writer2.run(df)

    # table DDL remains the same
    new_ddl = connection.sql(f"SHOW CREATE TABLE {table}").collect()[0][0]
    assert new_ddl == old_ddl

    # table contains only old data
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )
