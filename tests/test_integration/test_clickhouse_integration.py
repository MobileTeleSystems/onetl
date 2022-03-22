import logging

import pandas
import pytest

from onetl.connection import Clickhouse
from onetl.core import DBReader, DBWriter


class TestIntegrationONETLClickhouse:
    """
    The test name affects how the test works: the second and third words define the behavior of the test.
    For example: test_<storage_name>_<reader/writer>_...
    <storage_name> - the name of the database in which the table will be pre-created.
    <reader/writer> - if reader is specified then the table will be pre-created and filled with test data,
    if writer is specified then only preliminary table creation will be performed.
    The name of the test will be given to the test table.
    """

    def test_clickhouse_connection_check(self, spark, processing, caplog):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        with caplog.at_level(logging.INFO):
            clickhouse.check()

        assert "Connection is available" in caplog.text

    def test_clickhouse_wrong_connection_check(self, spark):
        clickhouse = Clickhouse(host="host", user="some_user", password="pwd", database="abc", spark=spark)

        with pytest.raises(RuntimeError):
            clickhouse.check()

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_clickhouse_reader_connection_sql(self, spark, processing, prepare_schema_table, suffix):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        table = prepare_schema_table.full_name

        df = clickhouse.sql(f"SELECT * FROM {table}{suffix}")
        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
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
    def test_clickhouse_reader_connection_fetch(self, spark, processing, prepare_schema_table, suffix):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        schema = prepare_schema_table.schema
        table = prepare_schema_table.full_name

        df = clickhouse.fetch(f"SELECT * FROM {table}{suffix}")
        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            order_by="id_int",
        )
        processing.assert_equal_df(df=df, other_frame=table_df, order_by="id_int")
        clickhouse.close()

        df = clickhouse.fetch(f"SELECT * FROM {table} WHERE id_int < 50{suffix}")
        filtered_df = table_df[table_df.id_int < 50]
        processing.assert_equal_df(df=df, other_frame=filtered_df, order_by="id_int")

        df = clickhouse.fetch(f"SHOW TABLES IN {schema}{suffix}")
        result_df = pandas.DataFrame([[prepare_schema_table.table]], columns=["name"])
        processing.assert_equal_df(df=df, other_frame=result_df)

        # wrong syntax
        with pytest.raises(Exception):
            clickhouse.fetch(f"SELEC 1{suffix}")

    @pytest.mark.parametrize("suffix", ["", ";"])
    def test_clickhouse_reader_connection_execute_ddl(self, spark, processing, get_schema_table, suffix):
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
    def test_clickhouse_reader_connection_execute_dml(self, request, spark, processing, prepare_schema_table, suffix):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        table_name, schema, table = prepare_schema_table
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
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
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
    def test_clickhouse_reader_connection_execute_function(
        self,
        request,
        spark,
        processing,
        prepare_schema_table,
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

        table = prepare_schema_table.full_name
        func = f"{prepare_schema_table.table}_func"
        table_df = processing.get_expected_dataframe(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
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

    def test_clickhouse_reader_snapshot(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )

        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_clickhouse_reader_snapshot_without_set_database(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            spark=spark,
        )

        reader = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )

        df = reader.run()

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    def test_clickhouse_reader_snapshot_with_columns(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader1 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_clickhouse_reader_snapshot_with_where(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )
        table_df = reader.run()

        reader1 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int < 1000",
        )
        table_df1 = reader1.run()
        assert table_df1.count() == table_df.count()

        reader2 = DBReader(
            connection=clickhouse,
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
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int = 50",
        )
        one_df = reader3.run()

        assert one_df.count() == 1

        reader4 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int > 1000",
        )
        empty_df = reader4.run()

        assert not empty_df.count()

    def test_clickhouse_reader_snapshot_with_columns_and_where(self, spark, processing, prepare_schema_table):
        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        reader1 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            where="id_int < 80 AND id_int > 10",
        )
        table_df = reader1.run()

        reader2 = DBReader(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
            columns=["count(*)"],
            where="id_int < 80 AND id_int > 10",
        )
        count_df = reader2.run()

        assert count_df.collect()[0][0] == table_df.count()

    def test_clickhouse_writer_snapshot(self, spark, processing, prepare_schema_table):
        df = processing.create_spark_df(spark=spark)

        clickhouse = Clickhouse(
            host=processing.host,
            port=processing.port,
            user=processing.user,
            password=processing.password,
            database=processing.database,
            spark=spark,
        )

        writer = DBWriter(
            connection=clickhouse,
            table=prepare_schema_table.full_name,
        )

        writer.run(df)

        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )
