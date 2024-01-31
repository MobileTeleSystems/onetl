import pytest

try:
    import pandas
except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

from onetl.connection import MongoDB
from onetl.db import DBReader

pytestmark = pytest.mark.mongodb


@pytest.fixture()
def df_schema():
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("_id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("hwm_int", IntegerType()),
            StructField("hwm_datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )


def test_mongodb_reader_snapshot(spark, processing, load_table_data, df_schema):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=mongo,
        source=load_table_data.table,
        df_schema=df_schema,
    )

    df = reader.run()

    assert df.schema == df_schema

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
        order_by="_id",
    )


def test_mongodb_reader_snapshot_with_where(spark, processing, load_table_data, df_schema):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=mongo,
        source=load_table_data.table,
        df_schema=df_schema,
    )
    table_df = reader.run()

    reader1 = DBReader(
        connection=mongo,
        source=load_table_data.table,
        df_schema=df_schema,
        hint={"_id": 1},
        where={"_id": {"$lt": 1000}},
    )

    table_df1 = reader1.run()

    assert table_df1.count() == table_df.count()

    reader2 = DBReader(
        connection=mongo,
        source=load_table_data.table,
        df_schema=df_schema,
        where={"$or": [{"_id": {"$lt": 1000}}, {"_id": {"$eq": 1000}}]},
    )
    table_df2 = reader2.run()

    assert table_df2.count() == table_df.count()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df1,
        order_by="_id",
    )

    one_reader = DBReader(
        connection=mongo,
        source=load_table_data.table,
        df_schema=df_schema,
        where={"_id": {"$eq": 50}},
    )
    one_df = one_reader.run()

    assert one_df.count() == 1

    empty_reader = DBReader(
        connection=mongo,
        source=load_table_data.table,
        df_schema=df_schema,
        where={"_id": {"$gt": 1000}},
    )
    empty_df = empty_reader.run()

    assert not empty_df.count()


def test_mongodb_reader_snapshot_nothing_to_read(spark, processing, prepare_schema_table, df_schema):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=mongo,
        source=prepare_schema_table.table,
        df_schema=df_schema,
    )

    span_gap = 10
    span_length = 50

    # there are 2 spans with a gap between

    # 0..50
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 60..110
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    with pytest.raises(Exception, match="No data in the source:"):
        reader.raise_if_no_data()

    assert not reader.has_data()

    # no data yet, nothing to read
    df = reader.run()
    assert not df.count()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # .run() is not called, but dataframes are lazy, so it now contains all data from the source
    processing.assert_equal_df(df=df, other_frame=first_span, order_by="_id")

    # read data explicitly
    df = reader.run()

    # check that read df has data
    assert reader.has_data()
    processing.assert_equal_df(df=df, other_frame=first_span, order_by="_id")

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )
    total_span = pandas.concat([first_span, second_span], ignore_index=True)

    # .run() is not called, but dataframes are lazy, so it now contains all data from the source
    processing.assert_equal_df(df=df, other_frame=total_span, order_by="_id")

    # read data explicitly
    df = reader.run()
    processing.assert_equal_df(df=df, other_frame=total_span, order_by="_id")
