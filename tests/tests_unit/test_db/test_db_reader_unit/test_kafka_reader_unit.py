import pytest
from etl_entities import Column

from onetl.connection import Kafka
from onetl.db import DBReader

pytestmark = pytest.mark.kafka


@pytest.fixture(scope="function")
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
            StructField("id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("int_value", IntegerType()),
            StructField("datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )


def test_kafka_reader_invalid_table(spark_mock):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )
    with pytest.raises(
        ValueError,
        match="Table name should be passed in `mytable` format",
    ):
        DBReader(
            connection=kafka,
            table="schema.table",  # Includes schema. Required format: table="table"
        )
    with pytest.raises(
        ValueError,
        match="Table name should be passed in `schema.name` format",
    ):
        DBReader(
            connection=kafka,
            table="schema.table.subtable",  # Includes subtable. Required format: table="table"
        )


def test_kafka_reader_unsupported_parameters(spark_mock, df_schema):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="'where' parameter is not supported by Kafka ",
    ):
        DBReader(
            connection=kafka,
            where={"col1": 1},
            table="table",
        )
    with pytest.raises(
        ValueError,
        match="'hint' parameter is not supported by Kafka ",
    ):
        DBReader(
            connection=kafka,
            hint={"col1": 1},
            table="table",
        )
    with pytest.raises(
        ValueError,
        match="'df_schema' parameter is not supported by Kafka ",
    ):
        DBReader(
            connection=kafka,
            table="table",
            df_schema=df_schema,
        )


def test_kafka_reader_valid_hwm_column(spark_mock):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    try:
        DBReader(
            connection=kafka,
            table="table",
            hwm_column="offset",
        )

        DBReader(
            connection=kafka,
            table="table",
            hwm_column=Column(name="offset"),
        )
    except ValueError:
        pytest.fail("ValueError for hwm_column raised unexpectedly!")

    if spark_mock.version.startswith("3."):
        try:
            DBReader(
                connection=kafka,
                table="table",
                hwm_column="timestamp",
            )
        except ValueError:
            pytest.fail("ValueError for hwm_column raised unexpectedly!")
    else:
        with pytest.raises(ValueError, match="Spark version must be 3.x"):
            DBReader(
                connection=kafka,
                table="table",
                hwm_column="timestamp",
            )


def test_kafka_reader_invalid_hwm_column(spark_mock):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="is not a valid hwm column",
    ):
        DBReader(
            connection=kafka,
            table="table",
            hwm_column="unknown",
        )

    with pytest.raises(
        ValueError,
        match="is not a valid hwm column",
    ):
        DBReader(
            connection=kafka,
            table="table",
            hwm_column=("some", "thing"),
        )
