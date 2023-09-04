import pytest

from onetl.connection import Oracle
from onetl.db import DBReader

pytestmark = pytest.mark.oracle


def test_oracle_reader_error_df_schema(spark_mock):
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

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


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_oracle_reader_wrong_table_name(spark_mock, table):
    oracle = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)
    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBReader(
            connection=oracle,
            table=table,  # Required format: table="shema.table"
        )


def test_oracle_reader_wrong_hint_type(spark_mock):
    oracle = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Oracle requires 'hint' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=oracle,
            hint={"col1": 1},
            table="schema.table",
        )


def test_oracle_reader_wrong_where_type(spark_mock):
    oracle = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Oracle requires 'where' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=oracle,
            where={"col1": 1},
            table="schema.table",
        )
