import logging

import pytest

try:
    import pandas
except ImportError:
    pytest.skip("Missing pandas or pyspark", allow_module_level=True)

pytestmark = pytest.mark.iceberg


def test_iceberg_check(iceberg_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert iceberg_connection.check() == iceberg_connection

    assert "|Iceberg|" in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


@pytest.mark.parametrize("suffix", ["", ";"])
def test_iceberg_connection_sql(iceberg_connection, processing, load_table_data, suffix):
    database_table_column = database_name_column = "namespace"

    schema = f"{iceberg_connection.catalog_name}.{load_table_data.schema}"
    table = f"{iceberg_connection.catalog_name}.{load_table_data.full_name}"

    df = iceberg_connection.sql(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = iceberg_connection.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

    df = iceberg_connection.sql("SHOW NAMESPACES")
    result_df = pandas.DataFrame([["default"]], columns=[database_table_column])
    processing.assert_equal_df(df=df, other_frame=result_df)

    df = iceberg_connection.sql(f"SHOW TABLES IN {schema}")
    result_df = pandas.DataFrame(
        [[schema.split(".")[-1], load_table_data.table, False]],
        columns=[database_name_column, "tableName", "isTemporary"],
    )
    processing.assert_equal_df(df=df, other_frame=result_df)

    # wrong syntax
    with pytest.raises(Exception):
        iceberg_connection.sql(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_iceberg_connection_execute_ddl(iceberg_connection, processing, get_schema_table, suffix):
    table_name, schema, table = get_schema_table
    fields = {
        column_name: processing.get_column_type(column_name)
        for column_name in processing.column_names
        if column_name != "id_int"
    }

    id_int_type = processing.get_column_type("id_int")

    assert not iceberg_connection.execute(processing.create_schema_ddl(schema) + suffix)

    assert not iceberg_connection.execute(
        processing.create_table_ddl(table, fields, schema) + f" PARTITIONED BY (id_int {id_int_type})" + suffix,
    )
    assert not iceberg_connection.execute(processing.drop_table_ddl(table, schema) + suffix)
    with pytest.raises(Exception):
        iceberg_connection.execute(f"DROP TABLE {iceberg_connection.catalog_name}.{schema}.missing_table{suffix}")

    assert not iceberg_connection.execute(
        processing.create_table_ddl(table, fields, schema) + suffix,
    )
    # not supported by Iceberg
    with pytest.raises(Exception):
        iceberg_connection.execute(f"DROP NAMESPACE {iceberg_connection.catalog_name}.{schema} CASCADE{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_iceberg_connection_execute_dml(request, iceberg_connection, processing, load_table_data, suffix):
    table_name, schema, table = load_table_data
    temp_name = f"{table}_temp"
    temp_table = f"{iceberg_connection.catalog_name}.{schema}.{temp_name}"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    fields = {col: processing.get_column_type(col) for col in processing.column_names}

    assert not iceberg_connection.execute(
        processing.create_table_ddl(temp_name, fields, schema) + suffix,
    )

    def table_finalizer():
        iceberg_connection.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)

    assert not iceberg_connection.sql(f"SELECT * FROM {temp_table}{suffix}").count()

    assert not iceberg_connection.execute(
        f"INSERT INTO {temp_table} SELECT * FROM {iceberg_connection.catalog_name}.{table_name}",
    )
    df = iceberg_connection.sql(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    assert not iceberg_connection.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not iceberg_connection.sql(f"SELECT * FROM {temp_table}{suffix}").count()
