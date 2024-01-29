import secrets

import pytest

try:
    import pandas
except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

from onetl._util.spark import get_spark_version
from onetl.connection import Kafka
from onetl.db import DBReader

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


@pytest.fixture(name="schema")
def dataframe_schema():
    from pyspark.sql.types import (
        FloatType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("id_int", LongType(), nullable=True),
            StructField("text_string", StringType(), nullable=True),
            StructField("hwm_int", LongType(), nullable=True),
            StructField("float_value", FloatType(), nullable=True),
        ],
    )


@pytest.fixture
def kafka_schema():
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("key", BinaryType(), nullable=True),
            StructField("value", BinaryType(), nullable=True),
            StructField("topic", StringType(), nullable=True),
            StructField("partition", IntegerType(), nullable=True),
            StructField("offset", LongType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("timestampType", IntegerType(), nullable=True),
        ],
    )
    return schema  # noqa:  WPS331


@pytest.fixture
def kafka_schema_with_headers():
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("key", BinaryType(), nullable=True),
            StructField("value", BinaryType(), nullable=True),
            StructField("topic", StringType(), nullable=True),
            StructField("partition", IntegerType(), nullable=True),
            StructField("offset", LongType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("timestampType", IntegerType(), nullable=True),
            StructField(
                "headers",
                ArrayType(
                    StructType(
                        [
                            StructField("key", StringType(), nullable=True),
                            StructField("value", BinaryType(), nullable=True),
                        ],
                    ),
                ),
                nullable=True,
            ),
        ],
    )
    return schema  # noqa:  WPS331


def test_kafka_reader(spark, processing, schema):
    topic = secrets.token_hex(6)

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source=topic,
    )

    first_span = processing.create_pandas_df(min_id=0, max_id=100)
    processing.insert_pandas_df_into_topic(first_span, topic)
    df = reader.run()

    processing.assert_equal_df(processing.json_deserialize(df, df_schema=schema), other_frame=first_span)

    processing.delete_topic([topic])


def test_kafka_reader_columns_and_types_without_headers(spark, processing, kafka_schema):
    topic = secrets.token_hex(6)

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source=topic,
    )

    first_span = processing.create_pandas_df(min_id=0, max_id=100)
    processing.insert_pandas_df_into_topic(first_span, topic)

    df = reader.run()

    assert df.schema == kafka_schema  # headers aren't included in schema if includeHeaders=False
    processing.delete_topic([topic])


def test_kafka_reader_columns_and_types_with_headers(spark, processing, kafka_schema_with_headers):
    if get_spark_version(spark).major < 3:
        pytest.skip("Spark 3.x or later is required to write/read 'headers' from Kafka messages")

    topic = secrets.token_hex(6)

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    # Check that the DataFrame also has a "headers" column when includeHeaders=True
    reader = DBReader(
        connection=kafka,
        source=topic,
        options=Kafka.ReadOptions(includeHeaders=True),
    )

    first_span = processing.create_pandas_df(min_id=0, max_id=100)
    processing.insert_pandas_df_into_topic(first_span, topic)

    df = reader.run()

    assert df.schema == kafka_schema_with_headers

    processing.delete_topic([topic])


def test_kafka_reader_topic_does_not_exist(spark, processing):
    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source="missing",
    )

    with pytest.raises(ValueError, match="Topic 'missing' doesn't exist"):
        reader.run()


@pytest.mark.parametrize("group_id_option", ["group.id", "groupIdPrefix"])
def test_kafka_reader_with_group_id(group_id_option, spark, processing, schema):
    if get_spark_version(spark).major < 3:
        pytest.skip("Spark 3.x or later is required to pas group.id")
    topic = secrets.token_hex(6)

    first_span = processing.create_pandas_df(min_id=0, max_id=100)
    processing.insert_pandas_df_into_topic(first_span, topic)

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        extra={group_id_option: "test"},
    )

    reader = DBReader(
        connection=kafka,
        source=topic,
    )
    df = reader.run()
    processing.assert_equal_df(processing.json_deserialize(df, df_schema=schema), other_frame=first_span)

    # Spark does not report to Kafka which messages were read, so Kafka does not remember latest offsets for groupId
    # https://stackoverflow.com/a/64003569
    df = reader.run()
    processing.assert_equal_df(processing.json_deserialize(df, df_schema=schema), other_frame=first_span)

    processing.delete_topic([topic])


def test_kafka_reader_snapshot_nothing_to_read(spark, processing, schema):
    topic = secrets.token_hex(6)
    processing.create_topic(topic, num_partitions=1)

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source=topic,
    )

    # 0..100
    first_span_begin = 0
    first_span_end = 100

    # 110..210
    second_span_begin = 110
    second_span_end = 210

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    with pytest.raises(Exception, match="No data in the source:"):
        reader.raise_error_if_no_data()

    assert not reader.has_data()

    # no data yet, nothing to read
    df = reader.run()
    assert not df.count()

    # insert first span
    processing.insert_pandas_df_into_topic(first_span, topic)

    # .run() is not called, but dataframes are lazy, so it now contains all data from the source
    deserialized_df = processing.json_deserialize(df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=first_span, order_by="id_int")

    # read data explicitly
    df = reader.run()

    # check that read df has data
    assert reader.has_data()

    deserialized_df = processing.json_deserialize(df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_pandas_df_into_topic(second_span, topic)
    total_span = pandas.concat([first_span, second_span], ignore_index=True)

    # .run() is not called, but dataframes are lazy, so it now contains all data from the source
    deserialized_df = processing.json_deserialize(df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=total_span, order_by="id_int")

    # read data explicitly
    df = reader.run()

    deserialized_df = processing.json_deserialize(df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=total_span, order_by="id_int")

    processing.delete_topic([topic])
