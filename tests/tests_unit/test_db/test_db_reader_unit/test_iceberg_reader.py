import pytest

from onetl.connection import Iceberg
from onetl.db import DBReader

pytestmark = pytest.mark.iceberg


def test_iceberg_reader_snapshot_error_pass_df_schema(spark_mock):
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

    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)

    with pytest.raises(ValueError, match="'df_schema' parameter is not supported by Iceberg"):
        DBReader(
            connection=iceberg,
            source="schema.table",
            df_schema=df_schema,
        )


def test_iceberg_reader_wrong_table_name(spark_mock):
    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)

    with pytest.raises(ValueError):
        DBReader(
            connection=iceberg,
            source="table",  # Required format: source="schema.table"
        )


def test_iceberg_reader_wrong_hint_type(spark_mock):
    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Iceberg requires 'hint' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=iceberg,
            hint={"col1": 1},
            source="schema.table",
        )


def test_iceberg_reader_wrong_where_type(spark_mock):
    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Iceberg requires 'where' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=iceberg,
            where={"col1": 1},
            source="schema.table",
        )
