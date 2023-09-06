import logging

import pytest

try:
    import pandas
except ImportError:
    # pandas can be missing if someone runs tests for file connections only
    pass

from onetl.connection import Greenplum

pytestmark = pytest.mark.greenplum


def test_greenplum_connection_check(spark, processing, caplog):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    with caplog.at_level(logging.INFO):
        assert greenplum.check() == greenplum

    assert "|Greenplum|" in caplog.text
    assert f"host = '{processing.host}'" in caplog.text
    assert f"port = {processing.port}" in caplog.text
    assert f"user = '{processing.user}'" in caplog.text
    assert f"database = '{processing.database}'" in caplog.text

    if processing.password:
        assert processing.password not in caplog.text

    assert "package = " not in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_greenplum_connection_check_fail(spark):
    greenplum = Greenplum(host="host", database="db", user="some_user", password="pwd", spark=spark)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        greenplum.check()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_fetch(spark, processing, load_table_data, suffix):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table = load_table_data.full_name

    df = greenplum.fetch(f"SELECT * FROM {table}{suffix}", Greenplum.JDBCOptions(fetchsize=2))
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = greenplum.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

    # wrong syntax
    with pytest.raises(Exception):
        greenplum.fetch(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_ddl(spark, processing, get_schema_table, suffix):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table_name, schema, table = get_schema_table
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}

    assert not greenplum.execute(f"SET search_path TO {schema}, public{suffix}", Greenplum.JDBCOptions(queryTimeout=1))

    assert not greenplum.execute(processing.create_schema_ddl(schema) + suffix)
    assert not greenplum.execute(processing.create_table_ddl(table, fields, schema) + suffix)

    assert not greenplum.execute(f"CREATE INDEX {table}_id_int_idx ON {table_name} (id_int){suffix}")
    assert not greenplum.execute(f"DROP INDEX onetl.{table}_id_int_idx{suffix}")
    greenplum.close()

    assert not greenplum.execute(f"ALTER TABLE {table_name} ADD COLUMN new_column INT{suffix}")
    assert not greenplum.execute(f"ALTER TABLE {table_name} ALTER COLUMN new_column TYPE FLOAT{suffix}")
    assert not greenplum.execute(f"ALTER TABLE {table_name} DROP COLUMN new_column{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"ALTER TABLE {table_name} ADD COLUMN non_existing TYPE WRONG_TYPE{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"ALTER TABLE {table_name} ALTER COLUMN non_existing TYPE FLOAT{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"ALTER TABLE {table_name} DROP COLUMN non_existing{suffix}")

    assert not greenplum.execute(processing.drop_table_ddl(table, schema) + suffix)

    with pytest.raises(Exception):
        greenplum.execute(
            processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        greenplum.execute(
            processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        greenplum.execute(f"DROP INDEX rand_index{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"DROP TABLE {schema}.missing_table{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"DROP DATABASE rand_db{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"DROP DATABASE {schema}{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_dml(request, spark, processing, load_table_data, suffix):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
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

    assert not greenplum.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

    def table_finalizer():
        greenplum.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)

    assert not greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}").count()

    assert not greenplum.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name} WHERE id_int < 50{suffix}")
    df = greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    inserted_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=inserted_df, order_by="id_int")

    insert_returning_df = greenplum.execute(
        f"""
        INSERT INTO {temp_table}
        SELECT * FROM {table_name}
        WHERE id_int >= 50
        RETURNING id_int{suffix}
    """,
    )

    df = greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    returned_df = table_df[table_df.id_int >= 50]
    processing.assert_equal_df(df=insert_returning_df, other_frame=returned_df[["id_int"]], order_by="id_int")

    assert not greenplum.execute(f"UPDATE {temp_table} SET hwm_int = 1 WHERE id_int < 50{suffix}")
    df = greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    updated_rows = table_df[table_df.id_int < 50]
    updated_rows["hwm_int"] = 1

    unchanged_rows = table_df[table_df.id_int >= 50]
    updated_df = pandas.concat([updated_rows, unchanged_rows])
    processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

    update_returned_df = greenplum.execute(
        f"UPDATE {temp_table} SET hwm_int = 2 WHERE id_int > 75 RETURNING id_int{suffix}",
    )
    df = greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    updated_rows = updated_df[updated_df.id_int > 75]
    updated_rows["hwm_int"] = 2

    unchanged_rows = updated_df[updated_df.id_int <= 75]
    updated_df = pandas.concat([updated_rows, unchanged_rows])

    processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

    processing.assert_equal_df(df=update_returned_df, other_frame=updated_rows[["id_int"]], order_by="id_int")

    assert not greenplum.execute(f"DELETE FROM {temp_table} WHERE id_int > 80{suffix}")
    df = greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    left_df = updated_df[updated_df.id_int <= 80]
    processing.assert_equal_df(df=df, other_frame=left_df, order_by="id_int")

    delete_returning_df = greenplum.execute(f"DELETE FROM {temp_table} WHERE id_int < 20 RETURNING id_int{suffix}")
    df = greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    deleted_df = left_df[left_df.id_int < 20]
    returned_df = deleted_df[["id_int"]]
    returned_df.reset_index()

    processing.assert_equal_df(df=delete_returning_df, other_frame=returned_df, order_by="id_int")

    final_left_df = left_df[left_df.id_int >= 20]
    processing.assert_equal_df(df=df, other_frame=final_left_df, order_by="id_int")

    assert not greenplum.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not greenplum.fetch(f"SELECT * FROM {temp_table}{suffix}").count()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_execute_procedure(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table = load_table_data.full_name
    proc = f"{load_table_data.table}_proc"

    # Greenplum does not support procedures
    with pytest.raises(Exception):
        greenplum.execute(
            f"""
            CREATE PROCEDURE {proc} ()
            LANGUAGE SQL
            AS $$
                SELECT COUNT(*) FROM {table};
            $${suffix}
        """,
        )


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_execute_function(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    func = f"{load_table_data.table}_func"

    assert not greenplum.execute(
        f"""
        CREATE FUNCTION {func}()
        RETURNS INT
        IMMUTABLE
        AS $$
            BEGIN
                RETURN 100;
            END
        $$ LANGUAGE PLPGSQL{suffix}
    """,
    )

    def function_finalizer():
        greenplum.execute(f"DROP FUNCTION {func}()")

    request.addfinalizer(function_finalizer)

    with greenplum:
        df = greenplum.fetch(f"SELECT {func}() AS id_int{suffix}")
        result_df = pandas.DataFrame([[100]], columns=["id_int"])
        processing.assert_equal_df(df=df, other_frame=result_df)

        df = greenplum.execute(f"{{call {func}}}")
        result_df = pandas.DataFrame([[100]], columns=["result"])
        processing.assert_equal_df(df=df, other_frame=result_df)

    df = greenplum.execute(f"{{call {func}()}}")
    result_df = pandas.DataFrame([[100]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    # wrong syntax
    with pytest.raises(Exception):
        greenplum.fetch(f"SELECT {func}{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"{{call {func};}}")

    with pytest.raises(Exception):
        greenplum.execute(f"{{call {func}();}}")

    # CALL can be used only for procedures
    with pytest.raises(Exception):
        greenplum.execute(f"CALL {func}()")

    # EXECUTE is supported only for prepared statements
    with pytest.raises(Exception):
        greenplum.execute(f"EXECUTE {func}")

    with pytest.raises(Exception):
        greenplum.execute(f"EXECUTE {func}()")

    # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
    # not supported by Greenplum
    with pytest.raises(Exception):
        greenplum.execute(f"{{?= call {func}}}")

    with pytest.raises(Exception):
        greenplum.execute(f"{{?= call {func}()}}")

    # already exists
    with pytest.raises(Exception):
        greenplum.execute(
            f"""
            CREATE FUNCTION {func}()
            RETURNS INT
            IMMUTABLE
            AS $$
                BEGIN
                    RETURN 100;
                END
            $$ LANGUAGE PLPGSQL{suffix}
        """,
        )

    # replace
    assert not greenplum.execute(
        f"""
        CREATE OR REPLACE FUNCTION {func}()
        RETURNS INT
        IMMUTABLE
        AS $$
            BEGIN
                RETURN 100;
            END
        $$ LANGUAGE PLPGSQL{suffix}
    """,
    )

    # missing
    with pytest.raises(Exception):
        greenplum.execute(f"CALL MissingFunction{suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"CALL MissingFunction(){suffix}")

    with pytest.raises(Exception):
        greenplum.execute(f"DROP FUNCTION MissingFunction{suffix}")

    # missing semicolon in the body
    with pytest.raises(Exception):
        greenplum.execute(
            f"""
            CREATE FUNCTION {func}()
            RETURNS INT
            IMMUTABLE
            AS $$
            BEGIN
                RETURN 100
            END
            $$ LANGUAGE PLPGSQL{suffix}
        """,
        )


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_execute_function_arguments(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table = load_table_data.full_name
    func = f"{load_table_data.table}_func"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not greenplum.execute(
        f"""
        CREATE FUNCTION {func}(i INT)
        RETURNS INT
        IMMUTABLE
        AS $$
            BEGIN
                RETURN i*100;
            END
        $$ LANGUAGE PLPGSQL{suffix}
    """,
    )

    def function_finalizer():
        greenplum.execute(f"DROP FUNCTION {func}(INT)")

    request.addfinalizer(function_finalizer)

    df = greenplum.fetch(f"SELECT {func}(10) AS id_int{suffix}")
    result_df = pandas.DataFrame([[1000]], columns=["id_int"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    df = greenplum.fetch(f"SELECT {func}(id_int) AS id_int FROM {table}{suffix}")
    table_df["id_int"] = table_df["id_int"] * 100
    processing.assert_equal_df(df=df, other_frame=table_df[["id_int"]], order_by="id_int")

    df = greenplum.execute(f"{{call {func}(10)}}")
    result_df = pandas.DataFrame([[1000]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    with pytest.raises(Exception):
        greenplum.execute(f"{{call {func}(10);}}")

    # not enough options
    with pytest.raises(Exception):
        greenplum.fetch(f"SELECT {func}")

    with pytest.raises(Exception):
        greenplum.fetch(f"SELECT {func}()")

    with pytest.raises(Exception):
        greenplum.execute(f"{{call {func}}}")

    with pytest.raises(Exception):
        greenplum.execute(f"{{call {func}()}}")

    # too many options
    with pytest.raises(Exception):
        greenplum.fetch(f"SELECT {func}(1, 10)")

    with pytest.raises(Exception):
        greenplum.execute(f"{{call {func}(1, 10)}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_execute_function_table(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table = load_table_data.full_name
    func = f"{table}_func_table"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not greenplum.execute(
        f"""
        CREATE FUNCTION {func}(i INT)
        RETURNS TABLE(id_int INT, text_string VARCHAR(400))
        AS $$
            SELECT id_int, text_string
            FROM {table}
            WHERE id_int < i;
        $$ LANGUAGE SQL{suffix}
    """,
    )

    def function_finalizer():
        greenplum.execute(f"DROP FUNCTION {func}(INT)")

    request.addfinalizer(function_finalizer)

    df = greenplum.fetch(f"SELECT * FROM {func}(10){suffix}")
    result_df = table_df[table_df.id_int < 10]
    processing.assert_equal_df(df=df, other_frame=result_df[["id_int", "text_string"]], order_by="id_int")

    # Greenplum allows to do this
    df = greenplum.fetch(f"SELECT {func}(10){suffix}")
    # but result looks like a garbage, so this is not a real result check
    assert df.count()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_execute_function_ddl(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table = get_schema_table.full_name
    func = f"{get_schema_table.table}_func_ddl"

    assert not greenplum.execute(
        f"""
        CREATE FUNCTION {func}()
        RETURNS INT
        AS $$
        BEGIN
            CREATE TABLE {table} (idd INT, text VARCHAR(400));
            RETURN 1;
        END;
        $$ LANGUAGE PLPGSQL{suffix}
    """,
    )

    def function_finalizer():
        greenplum.execute(f"DROP FUNCTION {func}()")

    request.addfinalizer(function_finalizer)

    df = greenplum.execute(f"{{call {func}}}")
    result_df = pandas.DataFrame([[1]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    def table_finalizer():
        greenplum.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)
    table_finalizer()

    df = greenplum.execute(f"{{call {func}()}}")
    processing.assert_equal_df(df=df, other_frame=result_df)

    # fetch is read-only
    with pytest.raises(Exception):
        greenplum.fetch(f"SELECT {func}() AS result")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_greenplum_connection_execute_function_dml(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    table = get_schema_table.full_name
    func = f"{get_schema_table.table}_func_dml"

    assert not greenplum.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR(400)){suffix}")

    def table_finalizer():
        greenplum.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)

    assert not greenplum.execute(
        f"""
        CREATE FUNCTION {func}(idd INT, text VARCHAR)
        RETURNS INT
        AS $$
        BEGIN
            INSERT INTO {table} VALUES(idd, text);
            RETURN idd;
        END;
        $$ LANGUAGE PLPGSQL{suffix}
    """,
    )

    def function_finalizer():
        greenplum.execute(f"DROP FUNCTION {func}(INT, VARCHAR)")

    request.addfinalizer(function_finalizer)

    df = greenplum.execute(f"{{call {func}(1, 'abc')}}")
    result_df = pandas.DataFrame([[1]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    # fetch is read-only
    with pytest.raises(Exception):
        greenplum.fetch(f"SELECT {func}(1, 'abc') AS result")
