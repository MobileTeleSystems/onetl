import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from onetl.connection import Oracle
from onetl.core import DBReader


def test_oracle_reader_error_df_schema(spark_mock):
    df_schema = StructType(
        [
            StructField("_id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("hwm_int", IntegerType()),
            StructField("hwm_datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )

    oracle = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="'df_schema' parameter is not supported by Oracle"):
        DBReader(
            connection=oracle,
            table="schema.table",
            df_schema=df_schema,
        )
