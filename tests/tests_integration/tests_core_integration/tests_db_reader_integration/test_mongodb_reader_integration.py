import pytest

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
