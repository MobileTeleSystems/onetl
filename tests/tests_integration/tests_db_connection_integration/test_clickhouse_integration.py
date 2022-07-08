"""The test name affects how the test works: the second and third words define the behavior of the test.
For example: test_<storage_name>_<reader/writer>_...
<storage_name> - the name of the database in which the table will be pre-created.
<reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
if writer is specified then only preliminary table creation will be performed.
The name of the test will be given to the test table."""
import pandas
import pytest

from onetl.connection import Clickhouse


@pytest.mark.parametrize("suffix", ["", ";"])
def test_clickhouse_connection_sql(spark, processing, load_table_data, suffix):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    table = load_table_data.full_name
    df = clickhouse.sql(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")
    df = clickhouse.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")
    # wrong syntax
    with pytest.raises(Exception):
        clickhouse.sql(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_clickhouse_connection_fetch(spark, processing, load_table_data, suffix):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    schema = load_table_data.schema
    table = load_table_data.full_name
    df = clickhouse.fetch(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")
    clickhouse.close()
    df = clickhouse.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")
    df = clickhouse.fetch(f"SHOW TABLES IN {schema}{suffix}")
    result_df = pandas.DataFrame([[load_table_data.table]], columns=["name"])
    processing.assert_equal_df(df=df, other_frame=result_df)
    # wrong syntax
    with pytest.raises(Exception):
        clickhouse.fetch(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_clickhouse_connection_execute_ddl(spark, processing, get_schema_table, suffix):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    table_name, schema, table = get_schema_table
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    assert not clickhouse.execute(processing.create_schema_ddl(schema) + suffix)
    assert not clickhouse.execute(processing.create_table_ddl(table, fields, schema) + suffix)
    with clickhouse:
        assert not clickhouse.execute(f"ALTER TABLE {table_name} ADD COLUMN new_column Int32{suffix}")
        assert not clickhouse.execute(f"ALTER TABLE {table_name} MODIFY COLUMN new_column Float32{suffix}")
    assert not clickhouse.execute(
        f"""
        ALTER TABLE {table_name} ADD INDEX {table}_id_int_idx (id_int) TYPE minmax GRANULARITY 8192{suffix}
    """,
    )
    assert not clickhouse.execute(f"ALTER TABLE {table_name} DROP INDEX {table}_id_int_idx{suffix}")
    assert not clickhouse.execute(f"ALTER TABLE {table_name} DROP COLUMN new_column{suffix}")
    with pytest.raises(Exception):
        clickhouse.execute(f"ALTER TABLE {table_name} ADD COLUMN non_existing WRONG_TYPE{suffix}")
    with pytest.raises(Exception):
        clickhouse.execute(f"ALTER TABLE {table_name} MODIFY COLUMN non_existing Int32")
    with pytest.raises(Exception):
        clickhouse.execute(f"ALTER TABLE {table_name} DROP COLUMN non_existing{suffix}")
    assert not clickhouse.execute(processing.drop_table_ddl(table, schema) + suffix)
    assert not clickhouse.execute(processing.drop_database_ddl(schema) + suffix)
    with pytest.raises(Exception):
        clickhouse.execute(
            processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
        )
    with pytest.raises(Exception):
        clickhouse.execute(
            processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema),
        )
    with pytest.raises(Exception):
        clickhouse.execute(f"ALTER TABLE {table_name} ADD COLUMN new_column Int32{suffix}")
    with pytest.raises(Exception):
        clickhouse.execute(f"DROP INDEX rand_index{suffix}")
    with pytest.raises(Exception):
        clickhouse.execute(f"DROP TABLE {schema}.missing_table{suffix}")
    with pytest.raises(Exception):
        clickhouse.execute(f"DROP DATABASE rand_db{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_clickhouse_connection_execute_dml(request, spark, processing, load_table_data, suffix):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    table_name, schema, table = load_table_data
    temp_name = f"{table}_temp"
    temp_table = f"{schema}.{temp_name}"
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    assert not clickhouse.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)
    assert not clickhouse.fetch(f"SELECT * FROM {temp_table}{suffix}").count()

    def table_finalizer():
        clickhouse.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)
    assert not clickhouse.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}{suffix}")
    df = clickhouse.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(
        df=df,
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    # not supported by Clickhouse
    with pytest.raises(Exception):
        clickhouse.execute(f"UPDATE {temp_table} SET id_int = 1 WHERE id_int < 50{suffix}")
    # not supported by Clickhouse
    with pytest.raises(Exception):
        clickhouse.execute(f"DELETE FROM {temp_table} WHERE id_int < 80{suffix}")
    assert not clickhouse.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not clickhouse.fetch(f"SELECT * FROM {temp_table}{suffix}").count()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_clickhouse_connection_execute_function(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    table = load_table_data.full_name
    func = f"{load_table_data.table}_func"
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    assert not clickhouse.execute(f"CREATE FUNCTION {func} AS (a, b) -> a + b{suffix}")

    def func_finalizer():
        clickhouse.execute(f"DROP FUNCTION {func}")

    request.addfinalizer(func_finalizer)
    df = clickhouse.fetch(f"SELECT {func}(id_int, hwm_int) AS abc FROM {table} ORDER BY id_int{suffix}")
    # make same calculations using Pandas
    other_df = table_df[["id_int", "hwm_int"]]
    other_df["abc"] = table_df.id_int + table_df.hwm_int
    other_df = other_df[["abc"]]
    processing.assert_equal_df(df=df, other_frame=other_df)
    # not enough arguments
    with pytest.raises(Exception):
        clickhouse.fetch(f"SELECT {func}(id_int) FROM {table}{suffix}")
    # too many arguments
    with pytest.raises(Exception):
        clickhouse.fetch(f"SELECT {func}(id_int, hwm_int, 10) FROM {table}{suffix}")
    # missing
    with pytest.raises(Exception):
        clickhouse.execute(f"DROP FUNCTION missing_function{suffix}")
    # wrong syntax
    with pytest.raises(Exception):
        clickhouse.execute(f"CREATE FUNCTION wrong_function AS (a, b) -> {suffix}")