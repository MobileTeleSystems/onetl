import pytest

from onetl.connection import Greenplum
from onetl.db import DBReader

pytestmark = pytest.mark.greenplum


def test_greenplum_reader_snapshot_error_pass_df_schema(spark_mock):
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

    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="'df_schema' parameter is not supported by Greenplum"):
        DBReader(
            connection=greenplum,
            table="schema.table",
            df_schema=df_schema,
        )


@pytest.mark.parametrize("table", ["table", "table.table.table"])
def test_greenplum_reader_wrong_table_name(spark_mock, table):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBReader(
            connection=greenplum,
            table=table,  # Required format: table="shema.table"
        )


def test_postgres_reader_hint_unsupported(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="'hint' parameter is not supported by Greenplum",
    ):
        DBReader(
            connection=greenplum,
            hint="col1",
            table="schema.table",
        )


def test_greenplum_reader_wrong_where_type(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Greenplum requires 'where' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=greenplum,
            where={"col1": 1},
            table="schema.table",
        )
