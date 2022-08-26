import pytest

from onetl.connection import Postgres
from onetl.core import DBReader


def test_postgres_reader_snapshot(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
    )


def test_postgres_reader_snapshot_with_columns(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader1 = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
    )
    table_df = reader1.run()

    reader2 = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        columns=["count(*)"],
    )
    count_df = reader2.run()

    assert count_df.collect()[0][0] == table_df.count()


def test_postgres_reader_snapshot_with_where(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
    )
    table_df = reader.run()

    reader1 = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        where="id_int < 1000",
    )
    table_df1 = reader1.run()
    assert table_df1.count() == table_df.count()

    reader2 = DBReader(
        connection=postgres,
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
        connection=postgres,
        table=load_table_data.full_name,
        where="id_int = 50",
    )
    one_df = reader3.run()

    assert one_df.count() == 1

    reader4 = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        where="id_int > 1000",
    )
    empty_df = reader4.run()

    assert not empty_df.count()


def test_postgres_reader_snapshot_with_columns_and_where(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader1 = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        where="id_int < 80 AND id_int > 10",
    )
    table_df = reader1.run()

    reader2 = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        columns=["count(*)"],
        where="id_int < 80 AND id_int > 10",
    )
    count_df = reader2.run()

    assert count_df.collect()[0][0] == table_df.count()


def test_postgres_reader_snapshot_with_pydantic_options(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        options=Postgres.ReadOptions(fetchsize=500),
    )

    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
    )


@pytest.mark.parametrize(
    "options",
    [
        {"numPartitions": "2", "partitionColumn": "hwm_int"},
        {"numPartitions": "2", "partitionColumn": "hwm_int", "lowerBound": "50"},
        {"numPartitions": "2", "partitionColumn": "hwm_int", "upperBound": "70"},
        {"fetchsize": "2"},
    ],
)
def test_postgres_reader_different_options(spark, processing, load_table_data, options):

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=load_table_data.full_name,
        options=options,
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
    )
