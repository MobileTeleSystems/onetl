import logging

import pandas
import pytest

from onetl.connection import Oracle
from onetl.core import DBReader, DBWriter


class TestIntegrationONETLOracle:
    """
    The test name affects how the test works: the second and third words define the behavior of the test.
    For example: test_<storage_name>_<reader/writer>_...
    <storage_name> - the name of the database in which the table will be pre-created.
    <reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
    if writer is specified then only preliminary table creation will be performed.
    The name of the test will be given to the test table.
    """

    def test_oracle_connection_check(self, spark, processing, caplog):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        with caplog.at_level(logging.INFO):
            oracle.check()

        assert "Connection is available" in caplog.text

    def test_oracle_wrong_connection_check(self, spark):
        oracle = Oracle(host="host", user="some_user", password="pwd", database="abc", sid="cde", spark=spark)

        with pytest.raises(RuntimeError):
            oracle.check()

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_sql(self, spark, processing, prepare_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name

        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )

        df = oracle.sql(f"SELECT * FROM {table}{suffix}")
        assert df.count()
        processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

        df = oracle.sql(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
        assert df.count()

        filtered_df = table_df[table_df.ID_INT < 50]
        processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

        with pytest.raises(Exception):
            oracle.sql(f"SELECT 1{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_fetch(self, spark, processing, prepare_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name

        df = oracle.fetch(f"SELECT * FROM {table}{suffix}")
        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )
        processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

        df = oracle.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
        filtered_df = table_df[table_df.ID_INT < 50]
        processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

        # not supported by JDBC, use SELECT * FROM v$tables
        with pytest.raises(Exception):
            oracle.fetch(f"SHOW TABLES{suffix}")

        # wrong syntax
        with pytest.raises(Exception):
            oracle.fetch(f"SELECT 1{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_ddl(self, spark, processing, get_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table_name, schema, table = get_schema_table
        fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}

        # SET is SQLPlus* option, not Oracle itself
        with pytest.raises(Exception):
            oracle.execute(f"SET SQLBLANKLINES ON{suffix}")

        assert not oracle.execute(processing.create_schema_ddl(schema) + suffix)
        assert not oracle.execute(processing.create_table_ddl(table, fields, schema) + suffix)

        assert not oracle.execute(f"CREATE INDEX {table}_id_int_idx ON {table_name} (id_int){suffix}")
        assert not oracle.execute(f"DROP INDEX {table}_id_int_idx{suffix}")

        assert not oracle.execute(f"ALTER TABLE {table_name} ADD new_column NUMBER{suffix}")
        assert not oracle.execute(f"ALTER TABLE {table_name} MODIFY new_column VARCHAR2(50) NOT NULL{suffix}")
        assert not oracle.execute(f"ALTER TABLE {table_name} DROP COLUMN new_column{suffix}")

        assert not oracle.execute(processing.drop_table_ddl(table, schema) + suffix)

        with pytest.raises(Exception):
            oracle.execute(
                processing.create_schema_ddl(schema) + "\n" + processing.create_table_ddl(table, schema) + suffix,
            )

        with pytest.raises(Exception):
            oracle.execute(
                processing.create_schema_ddl(schema) + ";\n" + processing.create_table_ddl(table, schema) + suffix,
            )

        with pytest.raises(Exception):
            oracle.execute(f"DROP INDEX rand_index{suffix}")

        with pytest.raises(Exception):
            oracle.execute(f"DROP TABLE {schema}.missing_table{suffix}")

        with pytest.raises(Exception):
            oracle.execute(f"DROP DATABASE rand_db{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_dml(self, request, spark, processing, prepare_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table_name, schema, table = prepare_schema_table
        temp_name = f"{table}_temp"
        temp_table = f"{schema}.{temp_name}"

        fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )

        assert not oracle.execute(processing.create_table_ddl(temp_name, fields, schema) + suffix)

        def table_finalizer():
            oracle.execute(processing.drop_table_ddl(temp_name, schema))

        request.addfinalizer(table_finalizer)

        assert not oracle.fetch(f"SELECT * FROM {temp_table}{suffix}").count()

        assert not oracle.execute(f"INSERT INTO {temp_table} SELECT * FROM {table_name}{suffix}")
        df = oracle.fetch(f"SELECT * FROM {temp_table}{suffix}")
        assert df.count()
        processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")

        assert not oracle.execute(f"UPDATE {temp_table} SET hwm_int = 1 WHERE id_int < 50{suffix}")
        df = oracle.fetch(f"SELECT * FROM {temp_table}{suffix}")
        assert df.count()

        updated_rows = table_df[table_df.ID_INT < 50]
        updated_rows["HWM_INT"] = 1

        unchanged_rows = table_df[table_df.ID_INT >= 50]
        updated_df = pandas.concat([updated_rows, unchanged_rows])

        processing.assert_equal_df(df=df, other_frame=updated_df, order_by="id_int")

        assert not oracle.execute(f"DELETE FROM {temp_table} WHERE id_int > 80{suffix}")
        df = oracle.fetch(f"SELECT * FROM {temp_table}{suffix}")
        assert df.count()

        left_df = updated_df[updated_df.ID_INT <= 80]
        processing.assert_equal_df(df=df, other_frame=left_df, order_by="id_int")

        assert not oracle.execute(f"TRUNCATE TABLE {temp_table}{suffix}")
        assert not oracle.fetch(f"SELECT * FROM {temp_table}{suffix}").count()

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_procedure(self, request, spark, processing, prepare_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name
        proc = f"{table}_proc"

        assert not oracle.execute(
            f"""
            CREATE PROCEDURE {proc}
            IS
                result NUMBER;

                CURSOR cur1 IS
                    SELECT COUNT(*) FROM {table}
                    WHERE ROWNUM = 1;
            BEGIN
                OPEN cur1;
                FETCH cur1 INTO result;
                CLOSE cur1;
            END{suffix}
        """,
        )

        def proc_finalizer():
            oracle.execute(f"DROP PROCEDURE {proc}{suffix}")

        request.addfinalizer(proc_finalizer)

        assert not oracle.execute(f"CALL {proc}(){suffix}")
        assert not oracle.execute(f"{{call {proc}}}")
        assert not oracle.execute(f"{{call {proc}()}}")

        # wrong syntax
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}{suffix}")

        with pytest.raises(Exception):
            oracle.execute(f"{{call {proc};}}")

        with pytest.raises(Exception):
            oracle.execute(f"{{call {proc}();}}")

        # EXECUTE is not not allowed here
        with pytest.raises(Exception):
            oracle.execute(f"EXECUTE {proc}")

        with pytest.raises(Exception):
            oracle.execute(f"EXECUTE {proc}()")

        # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
        # not supported by Oracle
        with pytest.raises(Exception):
            oracle.execute(f"{{?= call {proc}}}")

        with pytest.raises(Exception):
            oracle.execute(f"{{?= call {proc}()}}")

        # already exists
        with pytest.raises(Exception):
            oracle.execute(
                f"""
                CREATE PROCEDURE {proc}
                IS
                    result NUMBER;

                    CURSOR cur1 IS
                        SELECT COUNT(*) FROM {table}
                        WHERE ROWNUM = 1;
                BEGIN
                    OPEN cur1;
                    FETCH cur1 INTO result;
                    CLOSE cur1;
                END{suffix}
            """,
            )

        # replace
        assert not oracle.execute(
            f"""
            CREATE OR REPLACE PROCEDURE {proc}
            IS
                result NUMBER;

                CURSOR cur1 IS
                    SELECT COUNT(*) FROM {table}
                    WHERE ROWNUM = 1;
            BEGIN
                OPEN cur1;
                FETCH cur1 INTO result;
                CLOSE cur1;
            END{suffix}
        """,
        )

        with pytest.raises(Exception):
            oracle.execute("CALL MissingProcedure")

        with pytest.raises(Exception):
            oracle.execute("DROP PROCEDURE MissingProcedure")

        # unnecessary parentheses
        with pytest.raises(Exception):
            oracle.execute(
                f"""
                CREATE OR REPLACE PROCEDURE {proc} ()
                IS
                    result NUMBER;

                    CURSOR cur1 IS
                        SELECT COUNT(*) FROM {table}
                        WHERE ROWNUM = 1;
                BEGIN
                    OPEN cur1;
                    FETCH cur1 INTO result;
                    CLOSE cur1
                END{suffix}
            """,
            )

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_procedure_arguments(
        self,
        request,
        spark,
        processing,
        prepare_schema_table,
        suffix,
    ):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name
        proc = f"{table}_proc"

        assert not oracle.execute(
            f"""
            CREATE PROCEDURE {proc} (idd IN NUMBER)
            IS
                result NUMBER;

                CURSOR cur1 IS
                    SELECT COUNT(*) FROM {table}
                    WHERE id_int = idd;
            BEGIN
                OPEN cur1;
                FETCH cur1 INTO result;
                CLOSE cur1;
            END{suffix}
        """,
        )

        def proc_finalizer():
            oracle.execute(f"DROP PROCEDURE {proc}")

        request.addfinalizer(proc_finalizer)

        assert not oracle.execute(f"CALL {proc}(10){suffix}")
        assert not oracle.execute(f"{{call {proc}(10)}}")

        # wrong syntax
        with pytest.raises(Exception):
            oracle.execute(f"{{call {proc}(10);}}")

        # not enough options
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}()")

        # too many options
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}(10, 1)")

        # already exists
        with pytest.raises(Exception):
            oracle.execute(
                f"""
                CREATE PROCEDURE {proc} (idd IN NUMBER)
                IS
                    result NUMBER;

                    CURSOR cur1 IS
                        SELECT COUNT(*) FROM {table}
                        WHERE id_int = idd;
                BEGIN
                    OPEN cur1;
                    FETCH cur1 INTO result;
                    CLOSE cur1;
                END{suffix}
            """,
            )

        assert not oracle.execute(
            f"""
            CREATE OR REPLACE PROCEDURE {proc} (idd IN NUMBER)
            IS
                result NUMBER;

                CURSOR cur1 IS
                    SELECT COUNT(*) FROM {table}
                    WHERE id_int = idd;
            BEGIN
                OPEN cur1;
                FETCH cur1 INTO result;
                CLOSE cur1;
            END{suffix}
        """,
        )

        with pytest.raises(Exception):
            oracle.execute("CALL MissingProcedure")

        with pytest.raises(Exception):
            oracle.execute("DROP PROCEDURE MissingProcedure")

        # missing semicolon in the body
        with pytest.raises(Exception):
            oracle.execute(
                f"""
                CREATE OR REPLACE PROCEDURE {proc} (idd IN NUMBER)
                IS
                    result NUMBER;

                    CURSOR cur1 IS
                        SELECT COUNT(*) FROM {table}
                        WHERE id_int = idd;
                BEGIN
                    OPEN cur1;
                    FETCH cur1 INTO result;
                    CLOSE cur1
                END{suffix}
            """,
            )

        with pytest.raises(Exception):
            oracle.execute(f"ALTER PROCEDURE {proc} COMPILE{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_procedure_inout(
        self,
        request,
        spark,
        processing,
        prepare_schema_table,
        suffix,
    ):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name
        proc = f"{table}_proc_inout"

        assert not oracle.execute(
            f"""
            CREATE PROCEDURE {proc} (idd IN NUMBER, result IN OUT NUMBER)
            IS
                CURSOR cur1 IS
                    SELECT COUNT(*) FROM {table}
                    WHERE id_int = idd;
            BEGIN
                OPEN cur1;
                FETCH cur1 INTO result;
                CLOSE cur1;
            END{suffix}
        """,
        )

        def proc_finalizer():
            oracle.execute(f"DROP PROCEDURE {proc}")

        request.addfinalizer(proc_finalizer)

        oracle.execute(
            f"""
            DECLARE
                result NUMBER;
            BEGIN
                result := 1;
                {proc}(10, result);
            END{suffix}
        """,
        )

        # output parameter is not a variable
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}(10, 1)")

        # option 1 value is missing
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}(10, ?)")

        # not enough options
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}(10)")

        # too many options
        with pytest.raises(Exception):
            oracle.execute(f"CALL {proc}(10, 1, 2)")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_procedure_ddl(self, request, spark, processing, get_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = get_schema_table.full_name
        proc = f"{table}_proc_ddl"

        assert not oracle.execute(
            f"""
            CREATE PROCEDURE {proc}
            AUTHID CURRENT_USER
            IS
                stmt VARCHAR2(4000);
            BEGIN
                stmt := 'CREATE TABLE {table} (idd NUMBER, text VARCHAR2(400))';
                EXECUTE IMMEDIATE stmt;
            END{suffix}
        """,
        )

        assert not oracle.execute(f"CALL {proc}()")
        assert not oracle.execute(f"DROP TABLE {table}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_procedure_dml(self, request, spark, processing, get_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = get_schema_table.full_name
        proc = f"{table}_proc_dml"

        assert not oracle.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR2(400)){suffix}")

        def table_finalizer():
            oracle.execute(f"DROP TABLE {table}")

        request.addfinalizer(table_finalizer)

        assert not oracle.execute(
            f"""
            CREATE PROCEDURE {proc} (idd IN NUMBER, text IN VARCHAR2)
            AUTHID CURRENT_USER
            IS
                stmt VARCHAR2(4000);
            BEGIN
                stmt := 'INSERT INTO {table} VALUES(' || idd || ',' || '''' || text || ''')';
                EXECUTE IMMEDIATE stmt;
            END{suffix}
        """,
        )

        def proc_finalizer():
            oracle.execute(f"DROP PROCEDURE {proc}")

        request.addfinalizer(proc_finalizer)

        assert not oracle.execute(f"CALL {proc}(1, 'abc')")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_function(self, request, spark, processing, prepare_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name
        func = f"{table}_func"

        assert not oracle.execute(
            f"""
            CREATE FUNCTION {func}
            RETURN NUMBER
            DETERMINISTIC
            AS
            BEGIN
                RETURN 100;
            END{suffix}
        """,
        )

        def func_finalizer():
            oracle.execute(f"DROP FUNCTION {func}{suffix}")

        request.addfinalizer(func_finalizer)

        # PL/SQL context
        assert not oracle.execute(
            f"""
            DECLARE
                result NUMBER;
            BEGIN
                result := {func}();
            END{suffix}
        """,
        )

        # PL/SQL context
        assert not oracle.execute(
            f"""
            DECLARE
                result NUMBER;
            BEGIN
                result := {func};
            END{suffix}
        """,
        )

        result_df = pandas.DataFrame([[100]], columns=["id_int"])

        # SQL context
        df = oracle.fetch(f"SELECT {func} AS id_int FROM DUAL{suffix}")
        processing.assert_equal_df(df=df, other_frame=result_df)
        oracle.close()

        df = oracle.fetch(f"SELECT {func}() AS id_int FROM DUAL{suffix}")
        processing.assert_equal_df(df=df, other_frame=result_df)

        # supported only for procedures
        with pytest.raises(Exception):
            assert not oracle.execute(f"CALL {func}{suffix}")

        with pytest.raises(Exception):
            assert not oracle.execute(f"CALL {func}(){suffix}")

        # can be used with procedures only
        with pytest.raises(Exception):
            oracle.execute(f"{{call {func}{suffix}}}")

        with pytest.raises(Exception):
            oracle.execute(f"{{call {func}(){suffix}}}")

        # syntax proposed by https://docs.oracle.com/javase/8/docs/api/java/sql/CallableStatement.html
        # not supported by Oracle
        with pytest.raises(Exception):
            oracle.execute(f"{{?= call {func}{suffix}}}")

        with pytest.raises(Exception):
            oracle.execute(f"{{?= call {func}(){suffix}}}")

        # EXECUTE is supported only for prepared statements
        with pytest.raises(Exception):
            oracle.execute(f"EXECUTE {func}{suffix}")

        with pytest.raises(Exception):
            oracle.execute(f"EXECUTE {func}(){suffix}")

        # already exists
        with pytest.raises(Exception):
            oracle.execute(
                f"""
                CREATE FUNCTION {func}()
                RETURN NUMBER
                DETERMINISTIC
                AS
                BEGIN
                    RETURN 100;
                END{suffix}
            """,
            )

        # recreate
        assert not oracle.execute(
            f"""
            CREATE OR REPLACE FUNCTION {func}
            RETURN NUMBER
            DETERMINISTIC
            AS
            BEGIN
                RETURN 100;
            END{suffix}
        """,
        )

        # missing
        with pytest.raises(Exception):
            oracle.fetch("SELECT MissingFunction() FROM DUAL")

        with pytest.raises(Exception):
            oracle.execute("DROP FUNCTION MissingFunction")

        # unnecessary parentheses
        with pytest.raises(Exception):
            oracle.execute(
                f"""
                CREATE OR REPLACE FUNCTION {func}()
                RETURN NUMBER
                DETERMINISTIC
                AS
                BEGIN
                    RETURN 100;
                END{suffix}
            """,
            )

        with pytest.raises(Exception):
            oracle.execute(f"ALTER FUNCTION {func} COMPILE{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_function_arguments(
        self,
        request,
        spark,
        processing,
        prepare_schema_table,
        suffix,
    ):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name
        func = f"{table}_func"

        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )

        assert not oracle.execute(
            f"""
            CREATE FUNCTION {func}(i IN NUMBER)
            RETURN NUMBER
            DETERMINISTIC
            AS
            BEGIN
                RETURN i*100;
            END{suffix}
        """,
        )

        def func_finalizer():
            oracle.execute(f"DROP FUNCTION {func}")

        request.addfinalizer(func_finalizer)

        with oracle:
            # PL/SQL context
            assert not oracle.execute(
                f"""
                DECLARE
                    result NUMBER;
                BEGIN
                    result := {func}(10);
                END{suffix}
            """,
            )

            # SQL context
            df = oracle.fetch(f"SELECT {func}(10) AS id_int FROM dual{suffix}")
            result_df = pandas.DataFrame([[1000]], columns=["id_int"])
            processing.assert_equal_df(df=df, other_frame=result_df)

        df = oracle.fetch(f"SELECT {func}(id_int) AS id_int FROM {table}{suffix}")
        table_df["ID_INT"] = table_df["ID_INT"] * 100
        processing.assert_equal_df(df=df, other_frame=table_df[["ID_INT"]], order_by="id_int")

        # not enough options
        with pytest.raises(Exception):
            oracle.fetch(f"SELECT {func}() FROM DUAL")

        # too many options
        with pytest.raises(Exception):
            oracle.fetch(f"SELECT {func}(10, 1) FROM DUAL")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_function_table(
        self,
        request,
        spark,
        processing,
        prepare_schema_table,
        suffix,
    ):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = prepare_schema_table.full_name
        func = f"{prepare_schema_table.table}_func"

        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )

        assert not oracle.execute(
            f"""
            CREATE PACKAGE {func}_pkg
            AUTHID CURRENT_USER
            AS
                TYPE func_res IS TABLE OF {table}%ROWTYPE;

                FUNCTION func_pipelined(iid IN NUMBER)
                RETURN func_res
                PIPELINED;
            END{suffix}
        """,
        )

        def package_finalizer():
            oracle.execute(f"DROP PACKAGE {func}_pkg{suffix}")

        request.addfinalizer(package_finalizer)

        assert not oracle.execute(
            f"""
            CREATE PACKAGE BODY {func}_pkg
            AS
                FUNCTION func_pipelined(iid IN NUMBER)
                RETURN func_res
                PIPELINED
                AS
                BEGIN
                    FOR rec IN (SELECT * FROM {table} WHERE id_int < iid) LOOP
                        PIPE ROW(rec);
                    END LOOP;

                    RETURN;
                END;
            END{suffix}
        """,
        )

        df = oracle.fetch(f"SELECT * FROM TABLE({func}_pkg.func_pipelined(10))")
        selected_df = table_df[table_df.ID_INT < 10]
        processing.assert_equal_df(df=df, other_frame=selected_df, order_by="id_int")

        df = oracle.fetch(f"SELECT * FROM {func}_pkg.func_pipelined(10)")
        processing.assert_equal_df(df=df, other_frame=selected_df, order_by="id_int")

        with pytest.raises(Exception):
            oracle.fetch(f"SELECT {func}_pkg.func_pipelined(10) FROM DUAL")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_function_ddl(self, request, spark, processing, get_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = get_schema_table.full_name
        func = f"{get_schema_table.table}_func_ddl"

        assert not oracle.execute(
            f"""
            CREATE FUNCTION {func}
            RETURN NUMBER
            AUTHID CURRENT_USER
            AS
                stmt VARCHAR2(4000);
            BEGIN
                stmt := 'CREATE TABLE {table} (idd NUMBER, text VARCHAR2(400))';
                EXECUTE IMMEDIATE stmt;

                RETURN 1;
            END{suffix}
        """,
        )

        def func_finalizer():
            oracle.execute(f"DROP FUNCTION {func}")

        request.addfinalizer(func_finalizer)

        assert not oracle.execute(
            f"""
            DECLARE
                result NUMBER;
            BEGIN
                result := {func}();
            END;
        """,
        )

        # fetch is read-only
        with pytest.raises(Exception):
            oracle.fetch(f"SELECT {func}() FROM DUAL")

        # ORA-14552: cannot perform a DDL, commit or rollback inside a query or DML
        with pytest.raises(Exception):
            oracle.sql(f"SELECT {func}() AS result FROM DUAL").collect()

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_oracle_reader_connection_execute_function_dml(self, request, spark, processing, get_schema_table, suffix):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        table = get_schema_table.full_name
        func = f"{get_schema_table.table}_func_dml"

        assert not oracle.execute(f"CREATE TABLE {table} (idd INT, text VARCHAR2(400)){suffix}")

        def table_finalizer():
            oracle.execute(f"DROP TABLE {table}")

        request.addfinalizer(table_finalizer)

        assert not oracle.execute(
            f"""
            CREATE FUNCTION {func}(idd IN NUMBER, text IN VARCHAR2)
            RETURN NUMBER
            AUTHID CURRENT_USER
            AS
                stmt VARCHAR2(4000);
            BEGIN
                stmt := 'INSERT INTO {table} VALUES(' || idd || ',' || '''' || text || ''')';
                EXECUTE IMMEDIATE stmt;

                RETURN idd;
            END{suffix}
        """,
        )

        def func_finalizer():
            oracle.execute(f"DROP FUNCTION {func}")

        request.addfinalizer(func_finalizer)

        assert not oracle.execute(
            f"""
            DECLARE
                result NUMBER;
            BEGIN
                result := {func}(50, 'abc');
            END{suffix}
        """,
        )

        # fetch is read-only
        with pytest.raises(Exception):
            oracle.fetch(f"SELECT {func}(50, 'abc') FROM DUAL")

        # ORA-14551: cannot perform a DML operation inside a query
        with pytest.raises(Exception):
            oracle.sql(f"SELECT {func}(50, 'abc') AS result FROM DUAL").collect()

    def test_oracle_reader_snapshot(self, spark, processing, prepare_schema_table):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        reader = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
        )
        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_oracle_reader_snapshot_with_columns(self, spark, processing, prepare_schema_table):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        reader1 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_oracle_reader_snapshot_with_where(self, spark, processing, prepare_schema_table):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        reader = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
        )
        table_df = reader.run()

        reader1 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            where="id_int < 1000",
        )
        table_df1 = reader1.run()
        assert table_df1.count() == table_df.count()

        reader2 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            where="id_int < 1000 OR id_int = 1000",
        )
        table_df2 = reader2.run()
        assert table_df2.count() == table_df.count()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=table_df1,
        )

        reader3 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            where="id_int = 50",
        )
        one_df = reader3.run()

        assert one_df.count() == 1

        reader4 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            where="id_int > 1000",
        )
        empty_df = reader4.run()

        assert not empty_df.count()

    def test_oracle_reader_snapshot_with_columns_and_where(self, spark, processing, prepare_schema_table):
        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        reader1 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            where="id_int < 80 AND id_int > 10",
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=oracle,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
            where="id_int < 80 AND id_int > 10",
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_oracle_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        oracle = Oracle(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
            sid=processing.sid,
        )

        writer = DBWriter(
            connection=oracle,
            table=prepare_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )
