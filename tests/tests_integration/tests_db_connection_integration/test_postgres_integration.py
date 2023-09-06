import logging

import pytest

try:
    import pandas
except ImportError:
    # pandas can be missing if someone runs tests for file connections only
    pass

from onetl.connection import Postgres

pytestmark = pytest.mark.postgres


def test_postgres_connection_check(spark, processing, caplog):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert postgres.check() == postgres

    assert "|Postgres|" in caplog.text
    assert f"host = '{processing.host}'" in caplog.text
    assert f"port = {processing.port}" in caplog.text
    assert f"user = '{processing.user}'" in caplog.text
    assert f"database = '{processing.database}'" in caplog.text

    if processing.password:
        assert processing.password not in caplog.text

    assert "package = " not in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_postgres_connection_check_fail(spark):
    postgres = Postgres(host="host", database="db", user="some_user", password="pwd", spark=spark)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        postgres.check()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_sql(spark, processing, load_table_data, suffix):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name

    df = postgres.sql(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    processing.assert_equal_df(df=df, other_frame=table_df)

    df = postgres.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df)

    # wrong syntax
    with pytest.raises(Exception):
        postgres.sql(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_fetch(spark, processing, load_table_data, suffix):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name

    df = postgres.fetch(f"SELECT * FROM {table}{suffix}", Postgres.JDBCOptions(fetchsize=2))
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df)

    df = postgres.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df)

    # wrong syntax
    with pytest.raises(Exception):
        postgres.fetch(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_ddl(spark, processing, get_schema_table, suffix):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table_name, schema, table = get_schema_table
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}

    assert not postgres.execute(f"SET search_path TO {schema}, public{suffix}", Postgres.JDBCOptions(queryTimeout=1))

    assert not postgres.execute(processing.create_schema_ddl(schema) + suffix)
    assert not postgres.execute(processing.create_table_ddl(table, fields, schema) + suffix)

    assert not postgres.execute(f"CREATE INDEX {table}_id_int_idx ON {table_name} (id_int){suffix}")
    assert not postgres.execute(f"DROP INDEX {table}_id_int_idx{suffix}")
    postgres.close()

    assert not postgres.execute(f"ALTER TABLE {table_name} ADD COLUMN new_column INT{suffix}")
    assert not postgres.execute(f"ALTER TABLE {table_name} ALTER COLUMN new_column TYPE FLOAT{suffix}")
    assert not postgres.execute(f"ALTER TABLE {table_name} DROP COLUMN new_column{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"ALTER TABLE {table_name} ADD COLUMN non_existing TYPE WRONG_TYPE{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"ALTER TABLE {table_name} ALTER COLUMN non_existing TYPE FLOAT{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"ALTER TABLE {table_name} DROP COLUMN non_existing{suffix}")

    assert not postgres.execute(processing.drop_table_ddl(table, schema) + suffix)

    with pytest.raises(Exception):
        postgres.execute(
            processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        postgres.execute(
            processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        postgres.execute(f"DROP INDEX rand_index{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"DROP TABLE {schema}.missing_table{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"DROP DATABASE rand_db{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"DROP DATABASE {schema}{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_dml(request, spark, processing, load_table_data, suffix):
    postgres = Postgres(
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
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not postgres.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

    def table_finalizer():
        postgres.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)

    assert not postgres.fetch(f"SELECT * FROM {temp_table}{suffix}").count()

    assert not postgres.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name} WHERE id_int < 50{suffix}")
    df = postgres.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    inserted_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=inserted_df, order_by="id_int")

    insert_returning_df = postgres.execute(
        f"""
        INSERT INTO {temp_table}
        SELECT * FROM {table_name}
        WHERE id_int >= 50
        RETURNING id_int{suffix}
    """,
    )

    df = postgres.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    returned_df = table_df[table_df.id_int >= 50]
    processing.assert_equal_df(df=insert_returning_df, other_frame=returned_df[["id_int"]], order_by="id_int")

    assert not postgres.execute(f"UPDATE {temp_table} SET hwm_int = 1 WHERE id_int < 50{suffix}")
    df = postgres.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    updated_rows = table_df[table_df.id_int < 50]
    updated_rows["hwm_int"] = 1

    unchanged_rows = table_df[table_df.id_int >= 50]
    updated_df = pandas.concat([updated_rows, unchanged_rows])
    processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

    update_returned_df = postgres.execute(
        f"UPDATE {temp_table} SET hwm_int = 2 WHERE id_int > 75 RETURNING id_int{suffix}",
    )
    df = postgres.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    updated_rows = updated_df[updated_df.id_int > 75]
    updated_rows["hwm_int"] = 2

    unchanged_rows = updated_df[updated_df.id_int <= 75]
    updated_df = pandas.concat([updated_rows, unchanged_rows])

    processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

    processing.assert_equal_df(df=update_returned_df, other_frame=updated_rows[["id_int"]], order_by="id_int")

    assert not postgres.execute(f"DELETE FROM {temp_table} WHERE id_int > 80{suffix}")
    df = postgres.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    left_df = updated_df[updated_df.id_int <= 80]
    processing.assert_equal_df(df=df, other_frame=left_df, order_by="id_int")

    delete_returning_df = postgres.execute(f"DELETE FROM {temp_table} WHERE id_int < 20 RETURNING id_int{suffix}")
    df = postgres.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    deleted_df = left_df[left_df.id_int < 20]
    returned_df = deleted_df[["id_int"]]
    returned_df.reset_index()

    processing.assert_equal_df(df=delete_returning_df, other_frame=returned_df, order_by="id_int")

    final_left_df = left_df[left_df.id_int >= 20]
    processing.assert_equal_df(df=df, other_frame=final_left_df, order_by="id_int")

    assert not postgres.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not postgres.fetch(f"SELECT * FROM {temp_table}{suffix}").count()


@pytest.mark.xfail(reason="Postgres prior to v11 does not support procedures")
@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_procedure(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{load_table_data.table}_proc"

    assert not postgres.execute(
        f"""
        CREATE PROCEDURE {proc} ()
        LANGUAGE SQL
        AS $$
            SELECT COUNT(*) FROM {table};
        $${suffix}
    """,
    )

    def proc_finalizer():
        postgres.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not postgres.execute(f"CALL {proc}(){suffix}")

    # wrong syntax
    with pytest.raises(Exception):
        postgres.execute(f"CALL {proc}{suffix}")

    # EXECUTE is supported only for prepared statements
    with pytest.raises(Exception):
        postgres.execute(f"EXECUTE {proc}{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"EXECUTE {proc}(){suffix}")

    # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
    # supported only for functions
    with pytest.raises(Exception):
        postgres.execute(f"{{call {proc}}}")

    with pytest.raises(Exception):
        postgres.execute(f"{{call {proc}()}}")

    # not supported by Postgres
    with pytest.raises(Exception):
        postgres.execute(f"{{?= call {proc}}}")

    with pytest.raises(Exception):
        postgres.execute(f"{{?= call {proc}()}}")

    # already exists
    with pytest.raises(Exception):
        postgres.execute(
            f"""
            CREATE PROCEDURE {proc} ()
            LANGUAGE SQL
            AS $$
                SELECT COUNT(*) FROM {table};
            $${suffix}
        """,
        )

    # recreate
    assert not postgres.execute(
        f"""
        CREATE OR REPLACE PROCEDURE {proc} ()
        LANGUAGE SQL
        AS $$
            SELECT COUNT(*) FROM {table};
        $${suffix}
    """,
    )

    with pytest.raises(Exception):
        postgres.execute("CALL MissingProcedure")

    with pytest.raises(Exception):
        postgres.execute("CALL MissingProcedure()")

    with pytest.raises(Exception):
        postgres.execute("DROP PROCEDURE MissingProcedure")

    # missing semicolon in body
    with pytest.raises(Exception):
        postgres.execute(
            f"""
            CREATE PROCEDURE {proc} ()
            LANGUAGE SQL
            AS $$
                SELECT COUNT(*) FROM {table}
            $$
        """,
        )


@pytest.mark.xfail(reason="Postgres prior to v11 does not support procedures")
@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_procedure_arguments(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{load_table_data.table}_proc"

    assert not postgres.execute(
        f"""
        CREATE PROCEDURE {proc} (idd int)
        LANGUAGE SQL
        AS $$
            SELECT COUNT(*) FROM {table}
            WHERE id_int = idd;
        $${suffix}
    """,
    )

    def proc_finalizer():
        postgres.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not postgres.execute(f"CALL {proc}(10){suffix}")

    # not enough options
    with pytest.raises(Exception):
        postgres.execute(f"CALL {proc}{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"CALL {proc}(){suffix}")

    # too many options
    with pytest.raises(Exception):
        postgres.execute(f"CALL {proc}(10, 1){suffix}")


@pytest.mark.xfail(reason="Postgres prior to v11 does not support procedures")
@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_procedure_inout(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{load_table_data.table}_proc_inout"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not postgres.execute(
        f"""
        CREATE PROCEDURE {proc} (IN idd int, INOUT result int)
        LANGUAGE PLPGSQL
        AS $$
            BEGIN
                SELECT COUNT(*) INTO result FROM {table}
                WHERE id_int < idd;
            END
        $${suffix}
    """,
    )

    def proc_finalizer():
        postgres.execute(f"DROP PROCEDURE {proc}{suffix}")

    request.addfinalizer(proc_finalizer)

    df = postgres.execute(f"CALL {proc}(10, 1){suffix}")
    matching_df = table_df[table_df.id_int < 10]
    result_df = pandas.DataFrame([[len(matching_df)]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    # option 1 value is missing
    # Postgres does not support OUT arguments
    with pytest.raises(Exception):
        postgres.execute(f"CALL {proc}(10, ?){suffix}")


@pytest.mark.xfail(reason="Postgres prior to v11 does not support procedures")
@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_procedure_ddl(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    proc = f"{table}_ddl"

    assert not postgres.execute(
        f"""
        CREATE PROCEDURE {proc} ()
        LANGUAGE SQL
        AS $$
            CREATE TABLE {table} (iid INT, text VARCHAR(400));
        $${suffix}
    """,
    )

    def proc_finalizer():
        postgres.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not postgres.execute(f"CALL {proc}()")
    assert not postgres.execute(f"DROP TABLE {table}")


@pytest.mark.xfail(reason="Postgres prior to v11 does not support procedures")
@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_procedure_dml(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    proc = f"{table}_dml"

    assert not postgres.execute(f"CREATE TABLE {table} (iid INT, text VARCHAR(400)){suffix}")

    def table_finalizer():
        postgres.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)

    assert not postgres.execute(
        f"""
        CREATE PROCEDURE {proc} (idd int, text VARCHAR)
        LANGUAGE SQL
        AS $$
            INSERT INTO {table} VALUES(idd, text);
        $${suffix}
    """,
    )

    def proc_finalizer():
        postgres.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not postgres.execute(f"CALL {proc}(1, 'abc')")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_function(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    func = f"{load_table_data.table}_func"

    assert not postgres.execute(
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
        postgres.execute(f"DROP FUNCTION {func}()")

    request.addfinalizer(function_finalizer)

    with postgres:
        df = postgres.fetch(f"SELECT {func}() AS id_int{suffix}")
        result_df = pandas.DataFrame([[100]], columns=["id_int"])
        processing.assert_equal_df(df=df, other_frame=result_df)

        df = postgres.execute(f"{{call {func}}}")
        result_df = pandas.DataFrame([[100]], columns=["result"])
        processing.assert_equal_df(df=df, other_frame=result_df)

    df = postgres.execute(f"{{call {func}()}}")
    result_df = pandas.DataFrame([[100]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    # wrong syntax
    with pytest.raises(Exception):
        postgres.fetch(f"SELECT {func}{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"{{call {func};}}")

    with pytest.raises(Exception):
        postgres.execute(f"{{call {func}();}}")

    # CALL can be used only for procedures
    with pytest.raises(Exception):
        postgres.execute(f"CALL {func}()")

    # EXECUTE is supported only for prepared statements
    with pytest.raises(Exception):
        postgres.execute(f"EXECUTE {func}")

    with pytest.raises(Exception):
        postgres.execute(f"EXECUTE {func}()")

    # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
    # not supported by Postgres
    with pytest.raises(Exception):
        postgres.execute(f"{{?= call {func}}}")

    with pytest.raises(Exception):
        postgres.execute(f"{{?= call {func}()}}")

    # already exists
    with pytest.raises(Exception):
        postgres.execute(
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
    assert not postgres.execute(
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
        postgres.execute(f"CALL MissingFunction{suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"CALL MissingFunction(){suffix}")

    with pytest.raises(Exception):
        postgres.execute(f"DROP FUNCTION MissingFunction{suffix}")

    # missing semicolon in the body
    with pytest.raises(Exception):
        postgres.execute(
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
def test_postgres_connection_execute_function_arguments(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    postgres = Postgres(
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

    assert not postgres.execute(
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
        postgres.execute(f"DROP FUNCTION {func}(INT)")

    request.addfinalizer(function_finalizer)

    df = postgres.fetch(f"SELECT {func}(10) AS id_int{suffix}")
    result_df = pandas.DataFrame([[1000]], columns=["id_int"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    df = postgres.fetch(f"SELECT {func}(id_int) AS id_int FROM {table}{suffix}")
    table_df["id_int"] = table_df["id_int"] * 100
    processing.assert_equal_df(df=df, other_frame=table_df[["id_int"]], order_by="id_int")

    df = postgres.execute(f"{{call {func}(10)}}")
    result_df = pandas.DataFrame([[1000]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    with pytest.raises(Exception):
        postgres.execute(f"{{call {func}(10);}}")

    # not enough options
    with pytest.raises(Exception):
        postgres.fetch(f"SELECT {func}")

    with pytest.raises(Exception):
        postgres.fetch(f"SELECT {func}()")

    with pytest.raises(Exception):
        postgres.execute(f"{{call {func}}}")

    with pytest.raises(Exception):
        postgres.execute(f"{{call {func}()}}")

    # too many options
    with pytest.raises(Exception):
        postgres.fetch(f"SELECT {func}(1, 10)")

    with pytest.raises(Exception):
        postgres.execute(f"{{call {func}(1, 10)}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_function_table(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    func = f"{table}_func_table"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not postgres.execute(
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
        postgres.execute(f"DROP FUNCTION {func}(INT)")

    request.addfinalizer(function_finalizer)

    df = postgres.fetch(f"SELECT * FROM {func}(10){suffix}")
    result_df = table_df[table_df.id_int < 10]
    processing.assert_equal_df(df=df, other_frame=result_df[["id_int", "text_string"]], order_by="id_int")

    # Postgres allows to do this
    df = postgres.fetch(f"SELECT {func}(10){suffix}")
    # but result looks like a garbage, so this is not a real result check
    assert df.count()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_function_ddl(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    func = f"{get_schema_table.table}_func_ddl"

    assert not postgres.execute(
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
        postgres.execute(f"DROP FUNCTION {func}()")

    request.addfinalizer(function_finalizer)

    df = postgres.execute(f"{{call {func}}}")
    result_df = pandas.DataFrame([[1]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    def table_finalizer():
        postgres.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)
    table_finalizer()

    df = postgres.execute(f"{{call {func}()}}")
    processing.assert_equal_df(df=df, other_frame=result_df)
    table_finalizer()

    # fetch is read-only
    with pytest.raises(Exception):
        postgres.fetch(f"SELECT {func}() AS result")

    # unfortunately, we cannot pass read-only flag to spark.read.jdbc
    df = postgres.sql(f"SELECT {func}() AS result")
    processing.assert_equal_df(df=df, other_frame=result_df)


@pytest.mark.parametrize("suffix", ["", ";"])
def test_postgres_connection_execute_function_dml(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    func = f"{get_schema_table.table}_func_dml"

    assert not postgres.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR(400)){suffix}")

    def table_finalizer():
        postgres.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)

    assert not postgres.execute(
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
        postgres.execute(f"DROP FUNCTION {func}(INT, VARCHAR)")

    request.addfinalizer(function_finalizer)

    df = postgres.execute(f"{{call {func}(1, 'abc')}}")
    result_df = pandas.DataFrame([[1]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    # fetch is read-only
    with pytest.raises(Exception):
        postgres.fetch(f"SELECT {func}(1, 'abc') AS result")

    # unfortunately, we cannot pass read-only flag to spark.read.jdbc
    df = postgres.sql(f"SELECT {func}(2, 'cde') AS result")
    result_df = pandas.DataFrame([[2]], columns=["result"])
    processing.assert_equal_df(df=df, other_frame=result_df)
