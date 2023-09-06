import logging

import pytest

try:
    import pandas
except ImportError:
    # pandas can be missing if someone runs tests for file connections only
    pass

from onetl.connection import MySQL

pytestmark = pytest.mark.mysql


def test_mysql_connection_check(spark, processing, caplog):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert mysql.check() == mysql

    assert "|MySQL|" in caplog.text
    assert f"host = '{processing.host}'" in caplog.text
    assert f"port = {processing.port}" in caplog.text
    assert f"database = '{processing.database}'" in caplog.text
    assert f"user = '{processing.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert processing.password not in caplog.text

    assert "package = " not in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_mysql_connection_check_fail(spark):
    mysql = MySQL(host="host", user="some_user", password="pwd", database="abc", spark=spark)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        mysql.check()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_sql(spark, processing, load_table_data, suffix):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name

    df = mysql.sql(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mysql.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

    #  not supported by JDBC, use SELECT * FROM v$tables
    with pytest.raises(Exception):
        mysql.sql(f"SHOW TABLES{suffix}")

    # wrong syntax
    with pytest.raises(Exception):
        mysql.sql(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_fetch(spark, processing, load_table_data, suffix):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    schema = load_table_data.schema
    table = load_table_data.full_name

    df = mysql.fetch(f"SELECT * FROM {table}{suffix}")
    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mysql.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
    filtered_df = table_df[table_df.id_int < 50]
    processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

    df = mysql.fetch(f"SHOW TABLES{suffix}")
    result_df = pandas.DataFrame([[load_table_data.table]], columns=[f"Tables_in_{schema}"])
    processing.assert_equal_df(df=df, other_frame=result_df)

    # wrong syntax
    with pytest.raises(Exception):
        mysql.fetch(f"SELEC 1{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_ddl(spark, processing, get_schema_table, suffix):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table_name, schema, table = get_schema_table
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}

    assert not mysql.execute(processing.create_schema_ddl(schema) + suffix)
    assert not mysql.execute(processing.create_table_ddl(table, fields, schema) + suffix)

    assert not mysql.execute(f"CREATE INDEX {table}_id_int_idx ON {table_name} (id_int){suffix}")
    assert not mysql.execute(f"DROP INDEX {table}_id_int_idx ON {table_name}{suffix}")

    assert not mysql.execute(f"ALTER TABLE {table_name} ADD COLUMN new_column INT, ALGORITHM=INPLACE{suffix}")
    assert not mysql.execute(
        f"ALTER TABLE {table_name} CHANGE COLUMN new_column new_column BIGINT, ALGORITHM=COPY{suffix}",
    )
    assert not mysql.execute(f"ALTER TABLE {table_name} ADD INDEX {table}__idx (new_column){suffix}")
    assert not mysql.execute(f"ALTER TABLE {table_name} DROP INDEX {table}__idx{suffix}")
    assert not mysql.execute(f"ALTER TABLE {table_name} DROP COLUMN new_column{suffix}")

    assert not mysql.execute(processing.drop_table_ddl(table, schema) + suffix)

    with pytest.raises(Exception):
        mysql.execute(
            processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        mysql.execute(
            processing.create_schema_ddl(schema) + ";" + processing.create_table_ddl(table, schema) + suffix,
        )

    with pytest.raises(Exception):
        mysql.execute(f"DROP INDEX rand_index{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"DROP INDEX rand_index ON {table_name}{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"DROP TABLE {schema}.missing_table{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"DROP DATABASE rand_db{suffix}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_dml(request, spark, processing, load_table_data, suffix):
    mysql = MySQL(
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

    assert not mysql.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

    def table_finalizer():
        mysql.execute(processing.drop_table_ddl(temp_name, schema))

    request.addfinalizer(table_finalizer)

    assert not mysql.fetch(f"SELECT * FROM {temp_table}{suffix}").count()

    assert not mysql.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}{suffix}")
    df = mysql.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    assert not mysql.execute(f"UPDATE {temp_table} SET hwm_int = 1 WHERE id_int < 50{suffix}")
    df = mysql.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    updated_rows = table_df[table_df.id_int < 50]
    updated_rows["hwm_int"] = 1

    unchanged_rows = table_df[table_df.id_int >= 50]
    updated_df = pandas.concat([updated_rows, unchanged_rows])

    processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

    assert not mysql.execute(f"DELETE FROM {temp_table} WHERE id_int > 80{suffix}")
    df = mysql.fetch(f"SELECT * FROM {temp_table}{suffix}")
    assert df.count()

    left_df = updated_df[updated_df.id_int <= 80]
    processing.assert_equal_df(df=df, other_frame=left_df, order_by="id_int")

    assert not mysql.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
    assert not mysql.fetch(f"SELECT * FROM {temp_table}{suffix}").count()


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_procedure(request, spark, processing, load_table_data, suffix):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{table}_proc"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not mysql.execute(
        f"""
        CREATE PROCEDURE {proc} ()
        BEGIN
            SELECT * FROM {table};
        END{suffix}
    """,
    )

    def proc_finalizer():
        mysql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    df = mysql.execute(f"CALL {proc}{suffix}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mysql.execute(f"CALL {proc}(){suffix}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mysql.execute(f"{{call {proc}}}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    df = mysql.execute(f"{{call {proc}()}}")
    processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

    # wrong syntax
    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc};}}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}();}}")

    # wrong usage
    with pytest.raises(Exception):
        mysql.fetch(f"SELECT * FROM {proc}{suffix}")

    with pytest.raises(Exception):
        mysql.fetch(f"SELECT * FROM {proc}(){suffix}")

    # not a function
    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {proc}{suffix}")

    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {proc}(){suffix}")

    # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
    # can be used only for functions
    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {proc}}}")

    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {proc}()}}")

    # EXECUTE is not supported by MySQL
    with pytest.raises(Exception):
        mysql.execute(f"EXECUTE {proc}{suffix}")

    # EXECUTE is not supported by MySQL
    with pytest.raises(Exception):
        mysql.execute(f"EXECUTE {proc}(){suffix}")

    # missing
    with pytest.raises(Exception):
        mysql.execute(f"CALL MissingProcedure{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"CALL MissingProcedure(){suffix}")

    with pytest.raises(Exception):
        mysql.execute("{call MissingProcedure}")

    with pytest.raises(Exception):
        mysql.execute("{call MissingProcedure()}")

    with pytest.raises(Exception):
        mysql.execute(f"DROP PROCEDURE MissingProcedure{suffix}")

    # missing semicolon
    with pytest.raises(Exception):
        mysql.execute(
            f"""
            CREATE PROCEDURE {proc}_wrong (IN idd INT, OUT result INT)
            BEGIN
                SELECT COUNT(*) INTO result FROM {table}
                WHERE id_int = idd
            END{suffix}
        """,
        )


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_procedure_arguments(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{table}_proc"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not mysql.execute(
        f"""
        CREATE PROCEDURE {proc} (IN idd INT)
        BEGIN
            SELECT * FROM {table}
            WHERE id_int < idd;
        END{suffix}
    """,
    )

    def proc_finalizer():
        mysql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    df = mysql.execute(f"CALL {proc}(10){suffix}")
    result_df = table_df[table_df.id_int < 10]
    processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    df = mysql.execute(f"{{call {proc}(10)}}")
    processing.assert_equal_df(df=df, other_frame=result_df, order_by="id_int")

    # wrong syntax
    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}(10);}}")

    # not enough options
    with pytest.raises(Exception):
        mysql.execute(f"CALL {proc}{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"CALL {proc}(){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}}}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}()}}")

    # too many options
    with pytest.raises(Exception):
        mysql.execute(f"CALL {proc}(10, 1){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}(10, 1)}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_procedure_out(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{table}_proc_out"

    assert not mysql.execute(
        f"""
        CREATE PROCEDURE {proc} (IN idd INT, OUT result INT)
        BEGIN
            SELECT COUNT(*) INTO result FROM {table}
            WHERE id_int = idd;
        END{suffix}
    """,
    )

    def proc_finalizer():
        mysql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not mysql.execute(f"CALL {proc}(10, ?){suffix}")
    assert not mysql.execute(f"{{call {proc}(10, ?)}}")

    # passing value for out parameter is not allowed
    with pytest.raises(Exception):
        mysql.execute(f"CALL {proc}(10, 1){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}(10, 1)}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_procedure_inout(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    proc = f"{table}_proc_inout"

    assert not mysql.execute(
        f"""
        CREATE PROCEDURE {proc} (IN idd INT, INOUT result INT)
        BEGIN
            SELECT COUNT(*) INTO result FROM {table}
            WHERE id_int = idd;
        END{suffix}
    """,
    )

    def proc_finalizer():
        mysql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    # inout argument is not properly registered
    # that's not okay, but at the moment we cannot parse all possible SQL dialects to handle this
    with pytest.raises(Exception):
        mysql.execute(f"CALL {proc}(10, 1){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}(10, 1)}}")

    # missing value for inout argument
    with pytest.raises(Exception):
        mysql.execute(f"CALL {proc}(10, ?){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {proc}(10, ?)}}")


@pytest.mark.flaky
@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_procedure_ddl(
    request,
    spark,
    processing,
    get_schema_table,
    suffix,
):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    proc = f"{table}_proc_ddl"

    assert not mysql.execute(
        f"""
        CREATE PROCEDURE {proc} ()
        BEGIN
            CREATE TABLE {table} (iid INT, text VARCHAR(400));
        END{suffix}
    """,
    )

    def proc_finalizer():
        mysql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not mysql.execute(f"CALL {proc}(){suffix}")

    def table_finalizer():
        mysql.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)
    table_finalizer()

    assert not mysql.execute(f"CALL {proc}{suffix}")
    table_finalizer()

    assert not mysql.execute(f"{{call {proc}}}")
    table_finalizer()

    assert not mysql.execute(f"{{call {proc}()}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_procedure_dml(request, spark, processing, get_schema_table, suffix):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    proc = f"{table}_proc_dml"

    assert not mysql.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR(400))")

    def table_finalizer():
        mysql.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)

    assert not mysql.execute(
        f"""
        CREATE PROCEDURE {proc} (idd INT, text VARCHAR(400))
        BEGIN
            INSERT INTO {table} VALUES(idd, text);
        END{suffix}
    """,
    )

    def proc_finalizer():
        mysql.execute(f"DROP PROCEDURE {proc}")

    request.addfinalizer(proc_finalizer)

    assert not mysql.execute(f"CALL {proc}(1, 'abc'){suffix}")
    assert not mysql.execute(f"{{call {proc}(2, 'cde')}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_function(request, spark, processing, load_table_data, suffix):
    mysql_root = MySQL(
        host=processing.host,
        port=processing.port,
        user="root",
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    # without this option non-root users cannot create functions
    mysql_root.execute("SET GLOBAL log_bin_trust_function_creators = 1")

    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    func = f"{table}_func"

    assert not mysql.execute(
        f"""
        CREATE FUNCTION {func}()
        RETURNS INT DETERMINISTIC
        RETURN 100
    """,
    )

    def func_finalizer():
        mysql.execute(f"DROP FUNCTION {func}")

    request.addfinalizer(func_finalizer)

    df = mysql.fetch(f"SELECT {func}() AS query_result{suffix}")
    result_df = pandas.DataFrame([[100]], columns=["query_result"])
    processing.assert_equal_df(df=df, other_frame=result_df)
    mysql.close()

    # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
    # can be used only for functions
    assert not mysql.execute(f"{{?= call {func}}}")
    assert not mysql.execute(f"{{?= call {func}()}}")

    # wrong syntax
    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {func}{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {func};}}")

    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {func}();}}")

    # not a procedure
    with pytest.raises(Exception):
        mysql.execute(f"CALL {func}{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"CALL {func}(){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {func}}}")

    with pytest.raises(Exception):
        mysql.execute(f"{{call {func}()}}")

    # EXECUTE is not supported by MySQL
    with pytest.raises(Exception):
        mysql.execute(f"EXECUTE {func}{suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"EXECUTE {func}(){suffix}")

    # missing
    with pytest.raises(Exception):
        mysql.execute("{{?= call MissingFunction}}")

    with pytest.raises(Exception):
        mysql.execute("DROP FUNCTION MissingFunction")

    # missing RETURNS statement
    with pytest.raises(Exception):
        mysql.execute(
            f"""
            CREATE FUNCTION {func}_wrong()
            RETURN 123
        """,
        )


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_function_arguments(
    request,
    spark,
    processing,
    load_table_data,
    suffix,
):
    mysql_root = MySQL(
        host=processing.host,
        port=processing.port,
        user="root",
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    # without this option non-root users cannot create functions
    mysql_root.execute("SET GLOBAL log_bin_trust_function_creators = 1")

    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = load_table_data.full_name
    func = f"{table}_func"

    table_df = processing.get_expected_dataframe(
        schema=load_table_data.schema,
        table=load_table_data.table,
        order_by="id_int",
    )

    assert not mysql.execute(
        f"""
        CREATE FUNCTION {func}(i INT)
        RETURNS INT DETERMINISTIC
        RETURN i * 100
    """,
    )

    def func_finalizer():
        mysql.execute(f"DROP FUNCTION {func}")

    request.addfinalizer(func_finalizer)

    with mysql:
        df = mysql.fetch(f"SELECT {func}(10) AS query_result{suffix}")
        result_df = pandas.DataFrame([[1000]], columns=["query_result"])
        processing.assert_equal_df(df=df, other_frame=result_df)

        df = mysql.fetch(f"SELECT {func}(id_int) AS id_int FROM {table}{suffix}")
        table_df["id_int"] = table_df.id_int * 100
        processing.assert_equal_df(df=df, other_frame=table_df[["id_int"]], order_by="id_int")

    assert not mysql.execute(f"{{?= call {func}(10)}}")

    # wrong syntax
    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {func}(10);}}")

    # not enough options
    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {func}{suffix}")

    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {func}(){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {func}}}")

    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {func}()}}")

    # too many options
    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {func}(1, 10){suffix}")

    with pytest.raises(Exception):
        mysql.execute(f"{{?= call {func}(1, 10)}}")


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_function_ddl(spark, processing, get_schema_table, suffix):
    mysql_root = MySQL(
        host=processing.host,
        port=processing.port,
        user="root",
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    # without this option non-root users cannot create functions
    mysql_root.execute("SET GLOBAL log_bin_trust_function_creators = 1")

    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    func = f"{get_schema_table.full_name}_func"

    # Explicit or implicit commit is not allowed in stored function or trigger
    with pytest.raises(Exception):
        mysql.execute(
            f"""
            CREATE FUNCTION {func}_ddl()
            RETURNS INT
            BEGIN
                CREATE TABLE {table} (idd INT, text VARCHAR(400));
                RETURN 1;
            END{suffix}
        """,
        )


@pytest.mark.parametrize("suffix", ["", ";"])
def test_mysql_connection_execute_function_dml(request, spark, processing, get_schema_table, suffix):
    mysql_root = MySQL(
        host=processing.host,
        port=processing.port,
        user="root",
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    # without this option non-root users cannot create functions
    mysql_root.execute("SET GLOBAL log_bin_trust_function_creators = 1")

    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table = get_schema_table.full_name
    func = f"{table}_func_dml"

    assert not mysql.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR(400))")

    def table_finalizer():
        mysql.execute(f"DROP TABLE {table}")

    request.addfinalizer(table_finalizer)

    assert not mysql.execute(
        f"""
        CREATE FUNCTION {func}(idd INT, text VARCHAR(400))
        RETURNS INT
        BEGIN
            INSERT INTO {table} VALUES (idd, text);
            RETURN idd;
        END{suffix}
    """,
    )

    def func_finalizer():
        mysql.execute(f"DROP FUNCTION {func}")

    request.addfinalizer(func_finalizer)

    assert not mysql.execute(f"{{?= call {func}(20, 'cde')}}")

    # fetch is always read-only
    with pytest.raises(Exception):
        mysql.fetch(f"SELECT {func}(10, 'abc') AS query_result{suffix}")

    # unfortunately, we cannot pass read-only flag to spark.read.jdbc
    df = mysql.sql(f"SELECT {func}(10, 'abc') AS query_result{suffix}")
    result_df = pandas.DataFrame([[10]], columns=["query_result"])
    processing.assert_equal_df(df=df, other_frame=result_df)
