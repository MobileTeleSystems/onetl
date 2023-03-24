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

pytestmark = pytest.mark.mongodb

df_schema = StructType(
    [
        StructField("_id", IntegerType()),
        StructField("text_string", StringType()),
        StructField("hwm_int", IntegerType()),
        StructField("hwm_datetime", TimestampType()),
        StructField("float_value", DoubleType()),
    ],
)


def test_mongodb_reader_snapshot(spark, processing, load_table_data):
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
