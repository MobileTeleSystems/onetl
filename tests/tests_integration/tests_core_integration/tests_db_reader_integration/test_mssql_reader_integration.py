from onetl.core import DBReader
from onetl.connection import MSSQL


def test_mssql_reader_snapshot(spark, processing, load_table_data):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    reader = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
    )
    df = reader.run()
    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
    )


def test_mssql_reader_snapshot_with_columns(spark, processing, load_table_data):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    reader1 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
    )
    table_df = reader1.run()
    reader2 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        columns=["count(*) AS query_result"],
    )
    count_df = reader2.run()
    assert count_df.collect()[0][0] == table_df.count()


def test_mssql_reader_snapshot_with_where(spark, processing, load_table_data):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    reader = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
    )
    table_df = reader.run()
    reader1 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        where="id_int < 1000",
    )
    table_df1 = reader1.run()
    assert table_df1.count() == table_df.count()
    reader2 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        where="id_int < 1000 OR id_int = 1000",
    )
    table_df2 = reader2.run()
    assert table_df2.count() == table_df.count()
    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df1,
    )
    reader3 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        where="id_int = 50",
    )
    one_df = reader3.run()
    assert one_df.count() == 1
    reader4 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        where="id_int > 1000",
    )
    empty_df = reader4.run()
    assert not empty_df.count()


def test_mssql_reader_snapshot_with_columns_and_where(spark, processing, load_table_data):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    reader1 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        where="id_int < 80 AND id_int > 10",
    )
    table_df = reader1.run()
    reader2 = DBReader(
        connection=mssql,
        table=load_table_data.full_name,
        columns=["count(*) AS query_result"],
        where="id_int < 80 AND id_int > 10",
    )
    count_df = reader2.run()
    assert count_df.collect()[0][0] == table_df.count()
