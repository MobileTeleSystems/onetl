import pytest
from pydantic import ValidationError

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
        deploy_keytab=False,
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
        ValidationError,
    ):
        DBReader(
            connection=kafka,
            table="schema.table.subtable",  # Includes subtable. Required format: table="table"
        )
    with pytest.raises(
        ValidationError,
    ):
        DBReader(
            connection=kafka,
            table="",  # Empty table name. Required format: table="table"
        )


def test_kafka_reader_unsupported_parameters(spark_mock, df_schema):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        deploy_keytab=False,
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
    with pytest.raises(
        ValueError,
        match="parameter is not supported by Kafka ",
    ):
        DBReader(
            connection=kafka,
            where={"col1": 1},
            hint={"col1": 1},
            table="table",
            df_schema=df_schema,
        )
