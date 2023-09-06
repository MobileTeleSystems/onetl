import logging

import pytest

try:
    import pandas
except ImportError:
    # pandas can be missing if someone runs tests for file connections only
    pass

from onetl.connection import MSSQL

pytestmark = pytest.mark.mssql


def test_mssql_connection_check(spark, processing, caplog):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )

    with caplog.at_level(logging.INFO):
        assert mssql.check() == mssql

    assert "|MSSQL|" in caplog.text
    assert f"host = '{processing.host}'" in caplog.text
    assert f"port = {processing.port}" in caplog.text
    assert f"database = '{processing.database}'" in caplog.text
    assert f"user = '{processing.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert processing.password not in caplog.text
    assert "extra = {'trustServerCertificate': 'true'}" in caplog.text

    assert "package = " not in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_mssql_connection_check_fail(spark):
    mssql = MSSQL(
        host="host",
        user="some_user",
        password="pwd",
        database="abc",
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        mssql.check()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_sql(spark, processing, load_table_data, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table = load_table_data.full_name

    df = mssql.sql(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mssql.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

    # wrong syntax
    with pytest.raises(Exception):
        mssql.sql(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_fetch(spark, processing, load_table_data, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )

    table = load_table_data.full_name
    df = mssql.fetch(f"SELECT * FROM {table}{suffix}")

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mssql.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]

    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")
    # wrong syntax
    with pytest.raises(Exception):
        mssql.fetch(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_ddl(spark, processing, get_schema_table, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table_name, schema, table = get_schema_table
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    assert not mssql.execute(processing.create_schema_ddl(schema) + suffix)
    assert not mssql.execute(processing.create_table_ddl(table, fields, schema) + suffix)
    assert not mssql.execute(f"CREATE INDEX {table}_id_int_idx ON {table_name} (id_int){suffix}")
    assert not mssql.execute(f"DROP INDEX {table}_id_int_idx ON {table_name}{suffix}")
    assert not mssql.execute(f"ALTER TABLE {table_name} ADD new_column INT{suffix}")
    assert not mssql.execute(f"ALTER TABLE {table_name} ALTER COLUMN new_column BIGINT NOT NULL{suffix}")
    assert not mssql.execute(f"ALTER TABLE {table_name} DROP COLUMN new_column{suffix}")
    assert not mssql.execute(processing.drop_table_ddl(table, schema))

    with pytest.raises(Exception):
        mssql.execute(
            processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        mssql.execute(
            processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        mssql.execute(f"DROP INDEX rand_index{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"DROP INDEX rand_index ON {table_name}{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"DROP INDEX rand_index ON missing_table{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"DROP TABLE {schema}.missing_table{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"DROP DATABASE rand_db{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"DROP DATABASE {schema}{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_dml(request, spark, processing, load_table_data, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table_name, schema, table = load_table_data
    temp_name = f"{table}_temp"
    temp_table = f"{schema}.{temp_name}"
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    assert not mssql.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

    def table_finalizer():
        mssql.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)

    assert not mssql.fetch(f"SELECT * FROM {temp_table}{suffix}").count()
    assert not mssql.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}{suffix}")

    df = mssql.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    assert not mssql.execute(f"UPDATE {temp_table} SET hwm_int = 1 WHERE id_int < 50{suffix}")
    df = mssql.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    updated_rows = table_df[table_df.id_int < 50]
    updated_rows["hwm_int"] = 1
    unchanged_rows = table_df[table_df.id_int >= 50]
    updated_df = pandas.concat([updated_rows, unchanged_rows])
    processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

    assert not mssql.execute(f"DELETE FROM {temp_table} WHERE id_int > 80{suffix}")

    df = mssql.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    left_df = updated_df[updated_df.id_int <= 80]
    processing.assert_equal_df(df=df, other_frame=left_df, order_by="id_int")

    assert not mssql.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not mssql.fetch(f"SELECT * FROM {temp_table}{suffix}").count()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_procedure(request, spark, processing, load_table_data, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table = load_table_data.full_name
    proc = f"{table}_proc"
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    assert not mssql.execute(
        f"""
        CREATE PROCEDURE {proc}
        AS
        SELECT * FROM {table}
    """,
    )

    def proc_finalizer():
        mssql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)
    df = mssql.execute(proc + suffix)
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mssql.execute(f"EXEC {proc}{suffix}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")
    mssql.close()

    df = mssql.execute(f"EXECUTE {proc}{suffix}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mssql.execute(f"{{call {proc}{suffix}}}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mssql.execute(f"{{call {proc}()}}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    # not allowed
    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}();}}")

    # MSSQL does not allow such procedure usage
    with pytest.raises(Exception):
        mssql.fetch(f"SELECT * FROM {proc}{suffix}")

    with pytest.raises(Exception):
        mssql.fetch(f"SELECT {proc}{suffix}")

    # CALL is not supported by MSSQL
    with pytest.raises(Exception):
        mssql.execute(f"CALL {proc}{suffix}")

    # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
    # not supported by MySQL
    with pytest.raises(Exception):
        mssql.execute(f"{{?= call {proc}{suffix}}}")

    # missing
    with pytest.raises(Exception):
        mssql.execute(f"MissingProcedure{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"EXEC MissingProcedure{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"EXECUTE MissingProcedure{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"{{call MissingProcedure{suffix}}}")

    with pytest.raises(Exception):
        mssql.execute(f"{{call MissingProcedure(){suffix}}}")

    with pytest.raises(Exception):
        mssql.execute(f"DROP PROCEDURE MissingProcedure{suffix}")

    # missing "AS" clause
    with pytest.raises(Exception):
        mssql.execute(
            f"""
            CREATE PROCEDURE {proc}_wrong
            SELECT * FROM {table}{suffix}
        """,
        )


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_procedure_arguments(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table = load_table_data.full_name
    proc = f"{table}_proc"
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not mssql.execute(
        f"""
        CREATE OR ALTER PROCEDURE {proc}
            @IDD INT
        AS
        SELECT * FROM {table} WHERE id_int < @IDD
    """,
    )

    def proc_finalizer():
        mssql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    with mssql:
        df = mssql.execute(proc + " 10" + suffix)
        result_df = table_df[table_df.id_int < 10]
        processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

        df = mssql.execute(f"EXEC {proc} 20{suffix}")
        result_df = table_df[table_df.id_int < 20]
        processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    df = mssql.execute(f"EXECUTE {proc} 30{suffix}")
    result_df = table_df[table_df.id_int < 30]
    processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    df = mssql.execute(f"{{call {proc}(40)}}")
    result_df = table_df[table_df.id_int < 40]
    processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    # not allowed
    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}(1);}}")

    # not enough arguments
    with pytest.raises(Exception):
        mssql.xml(proc + suffix)

    with pytest.raises(Exception):
        mssql.execute(f"EXEC {proc}{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"EXECUTE {proc}{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}}}")

    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}()}}")

    # too many arguments
    with pytest.raises(Exception):
        mssql.execute(proc + " 1, 2" + suffix)

    with pytest.raises(Exception):
        mssql.execute(f"EXEC {proc} 1, 2{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"EXECUTE {proc} 1, 2{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}(1, 2)}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_procedure_out(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table = load_table_data.full_name
    proc = f"{table}_proc_out"
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not mssql.execute(
        f"""
        CREATE PROCEDURE {proc}
            @IDD INT,
            @RES INT OUT
        AS
            SELECT * FROM {table} WHERE id_int < @IDD;
            SET @RES = (SELECT COUNT(*) FROM {table} WHERE id_int < @IDD){suffix}
    """,
    )

    def proc_finalizer():
        mssql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)
    #  Cannot use the OUTPUT option when passing a constant to a stored procedure
    with pytest.raises(Exception):
        mssql.execute(f"EXEC {proc} @IDD=1, @RES='abc' OUT{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"EXECUTE {proc} 2, 'cde' OUTPUT{suffix}")

    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}(3, 'def'){suffix}}}")
    df = mssql.execute(f"DECLARE @abc INT; EXEC {proc} @IDD=10, @RES=@abc OUT{suffix}")

    result_df = table_df[table_df.id_int < 10]
    processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    df = mssql.execute(f"DECLARE @cde INT; EXECUTE {proc} 20, @cde OUTPUT{suffix}")
    result_df = table_df[table_df.id_int < 20]
    processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    # output parameter is not registered
    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}(3, ?){suffix}}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_procedure_ddl(request, spark, processing, get_schema_table, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table = get_schema_table.full_name
    proc = f"{table}_proc_ddl"

    assert not mssql.execute(
        f"""
        CREATE PROCEDURE {proc}
        AS
        CREATE TABLE {table} (idd INT, text VARCHAR(400)){suffix}
    """,
    )

    def proc_finalizer():
        mssql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)
    assert not mssql.execute(f"EXECUTE {proc}{suffix}")

    def table_finalizer():
        mssql.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)
    table_finalizer()
    assert not mssql.execute(f"{{call {proc}{suffix}}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mssql_connection_execute_procedure_dml(request, spark, processing, get_schema_table, suffix):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    table = get_schema_table.full_name
    proc = f"{table}_proc_dml"
    assert not mssql.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR(400)){suffix}")

    def table_finalizer():
        mssql.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)
    assert not mssql.execute(
        f"""
        CREATE PROCEDURE {proc}
            @IDD INT,
            @TEXT VARCHAR(400)
        AS
        INSERT INTO {table} VALUES (@IDD, @TEXT){suffix}
    """,
    )

    def proc_finalizer():
        mssql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not mssql.execute(f"EXEC {proc} @IDD=1, @TEXT='abc'{suffix}")
    assert not mssql.execute(f"EXECUTE {proc} 2, 'cde'{suffix}")
    assert not mssql.execute(f"{{call {proc}(3, 'def')}}")

    # wrong syntax
    with pytest.raises(Exception):
        mssql.execute(f"{{call {proc}(3, 'def');}}")
