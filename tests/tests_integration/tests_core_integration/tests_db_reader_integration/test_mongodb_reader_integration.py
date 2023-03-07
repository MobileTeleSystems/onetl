import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from onetl.connection import MongoDB
from onetl.core import DBReader

df_schema = StructType(
    [
        StructField("_id", IntegerType()),
        StructField("text_string", StringType()),
        StructField("hwm_int", IntegerType()),
        StructField("hwm_datetime", TimestampType()),
        StructField("float_value", DoubleType()),
    ],
)


def test_mongodb_reader_snapshot_without_df_schema(spark, processing, load_table_data):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with pytest.raises(ValueError, match="'df_schema' parameter should be passed."):
        DBReader(
            connection=mongo,
            table=load_table_data.table,
        )


def test_mongodb_reader_snapshot_with_df_schema(spark, processing, load_table_data):
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
        table=load_table_data.table,
        df_schema=df_schema,
    )

    df = reader.run()

    assert df.schema == df_schema

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
    )


def test_mongodb_reader_snapshot_error_pass_columns(spark, processing, load_table_data):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with pytest.raises(
        ValueError,
        match="Invalid 'columns' parameter passed. MongoDB connector does not support this option.",
    ):
        DBReader(connection=mongo, table=load_table_data.table, columns="_id,test", df_schema=df_schema)


def test_mongodb_reader_hwm_wrong_columns(spark, processing, load_table_data):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with pytest.raises(
        ValueError,
        match="the 'hwm_column' parameter must be specified among the fields in 'df_schema'",
    ):
        DBReader(connection=mongo, table=load_table_data.table, hwm_column="_id2", df_schema=df_schema)
