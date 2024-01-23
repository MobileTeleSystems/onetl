import secrets

import pytest

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


def test_kafka_reader_hwm_offset_is_valid(spark_mock):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    DBReader(
        connection=kafka,
        table="table",
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="offset"),
    )


@pytest.mark.parametrize(
    "hwm_expression",
    ["unknown", "timestamp"],
)
def test_kafka_reader_invalid_hwm_column(spark_mock, hwm_expression):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match=f"hwm.expression='{hwm_expression}' is not supported by Kafka",
    ):
        DBReader(
            connection=kafka,
            table="table",
            hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_expression),
        )


@pytest.mark.parametrize(
    "topic, should_raise, error_message",
    [
        ("valid_topic_name", False, None),  # valid case
        ("*", True, r"source/target=\* is not supported by Kafka. Provide a singular topic."),
        ("topic1, topic2", True, "source/target=topic1, topic2 is not supported by Kafka. Provide a singular topic."),
    ],
)
def test_kafka_reader_valid_source(spark_mock, topic, should_raise, error_message):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    if should_raise:
        with pytest.raises(
            ValueError,
            match=error_message,
        ):
            DBReader(
                connection=kafka,
                table=topic,
                hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="offset"),
            )
    else:
        DBReader(
            connection=kafka,
            table=topic,
            hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="offset"),
        )
