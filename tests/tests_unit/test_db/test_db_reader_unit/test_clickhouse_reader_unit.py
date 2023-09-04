import pytest

from onetl.connection import Clickhouse
from onetl.db import DBReader

pytestmark = pytest.mark.clickhouse


def test_clickhouse_reader_snapshot_error_pass_df_schema(spark_mock):
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
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="'df_schema' parameter is not supported by Clickhouse"):
        DBReader(
            connection=clickhouse,
            table="schema.table",
            df_schema=df_schema,
        )


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_clickhouse_reader_wrong_table_name(spark_mock, table):
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBReader(
            connection=clickhouse,
            table=table,  # Required format: table="shema.table"
        )


def test_clickhouse_reader_wrong_hint_type(spark_mock):
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Clickhouse requires 'hint' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=clickhouse,
            hint={"col1": 1},
            table="schema.table",
        )


def test_clickhouse_reader_wrong_where_type(spark_mock):
    clickhouse = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Clickhouse requires 'where' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=clickhouse,
            where={"col1": 1},
            table="schema.table",
        )
