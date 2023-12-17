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


def test_kafka_reader_hwm_timestamp_depends_on_spark_version(spark_mock, mocker):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )
    mocker.patch.object(spark_mock, "version", new="3.2.0")
    DBReader(
        connection=kafka,
        table="table",
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="timestamp"),
    )

    mocker.patch.object(spark_mock, "version", new="2.4.0")
    with pytest.raises(ValueError, match="Spark version must be 3.x"):
        DBReader(
            connection=kafka,
            table="table",
            hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="timestamp"),
        )


def test_kafka_reader_invalid_hwm_column(spark_mock):
    kafka = Kafka(
        addresses=["localhost:9092"],
        cluster="my_cluster",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="hwm.expression='unknown' is not supported by Kafka",
    ):
        DBReader(
            connection=kafka,
            table="table",
            hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="unknown"),
        )
