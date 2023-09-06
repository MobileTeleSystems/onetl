import logging

import pytest

from onetl._util.spark import get_spark_version

try:
    import pandas
except ImportError:
    # pandas can be missing if someone runs tests for file connections only
    pass

from onetl.connection import Hive

pytestmark = pytest.mark.hive


def test_hive_check(spark, caplog):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    with caplog.at_level(logging.INFO):
        assert hive.check() == hive

    assert "|Hive|" in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


@pytest.mark.parametrize("suffix", ["", ";"])
def test_hive_connection_sql(spark, processing, load_table_data, suffix):
    if get_spark_version(spark).major < 3:
        database_table_column = "databaseName"
        database_name_column = "database"
    else:
        database_table_column = database_name_column = "namespace"

    hive = Hive(cluster="rnd-dwh", spark=spark)
    schema = load_table_data.schema
    table = load_table_data.full_name

    df = hive.sql(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = hive.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

    df = hive.sql("SHOW DATABASES")
    result_df = pandas.DataFrame([["default"], [schema]], columns=[database_table_column])
    processing.assert_equal_df(df=df, other_frame=result_df)

    df = hive.sql(f"SHOW TABLES IN {schema}")
    result_df = pandas.DataFrame(
        [[schema, load_table_data.table, False]],
        columns=[database_name_column, "tableName", "isTemporary"],
    )
    processing.assert_equal_df(df=df, other_frame=result_df)
    # wrong syntax
    with pytest.raises(Exception):
        hive.sql(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_hive_connection_execute_ddl(spark, processing, get_schema_table, suffix):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    table_name, schema, table = get_schema_table
    fields = {
        column_name: processing.get_column_type(column_name)
        for column_name in processing.column_names
        if column_name != "id_int"
    }

    id_int_type = processing.get_column_type("id_int")

    assert not hive.execute(processing.create_schema_ddl(schema) + suffix)
    assert not hive.execute(
        processing.create_table_ddl(table, fields, schema) + f" PARTITIONED BY (id_int {id_int_type})" + suffix,
    )
    assert not hive.execute(f"ALTER SCHEMA {schema} SET DBPROPERTIES ('a' = 'b'){suffix}")
    assert not hive.execute(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(id_int = 1){suffix}")

    with pytest.raises(Exception):
        hive.execute(f"ALTER TABLE {table_name} DROP PARTITION(id_int = 999){suffix}")

    assert not hive.execute(f"MSCK REPAIR TABLE {table_name}{suffix}")
    assert not hive.execute(processing.drop_table_ddl(table, schema) + suffix)
    assert not hive.execute(processing.create_table_ddl(table, fields, schema) + suffix)
    assert not hive.execute(f"DROP TABLE {table_name} PURGE{suffix}")

    with pytest.raises(Exception):
        hive.execute(
            processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
        )
    with pytest.raises(Exception):
        hive.execute(
            processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema) + suffix,
        )
    with pytest.raises(Exception):
        hive.execute(f"DROP TABLE {schema}.missing_table{suffix}")
    with pytest.raises(Exception):
        hive.execute(f"DROP DATABASE rand_db{suffix}")
    assert not hive.execute(f"DROP DATABASE {schema}{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_hive_connection_execute_dml(request, spark, processing, load_table_data, suffix):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    table_name, schema, table = load_table_data
    temp_name = f"{table}_temp"
    temp_table = f"{schema}.{temp_name}"
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    assert not hive.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

    def table_finalizer():
        hive.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)
    assert not hive.sql(f"SELECT * FROM {temp_table}{suffix}").count()
    assert not hive.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}")
    df = hive.sql(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")
    # not supported by Hive
    with pytest.raises(Exception):
        hive.execute(f"UPDATE {temp_table} SET id_int = 1 WHERE id_int < 50{suffix}")
    # not supported by Hive
    with pytest.raises(Exception):
        hive.execute(f"DELETE FROM {temp_table} WHERE id_int < 80{suffix}")
    assert not hive.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not hive.sql(f"SELECT * FROM {temp_table}{suffix}").count()
