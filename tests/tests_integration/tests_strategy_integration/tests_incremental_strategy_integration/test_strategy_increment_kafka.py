import secrets

import pytest
from etl_entities.hwm import KeyValueIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import Kafka
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

pytestmark = pytest.mark.kafka


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


@pytest.mark.parametrize(
    "span_gap, span_length",
    [
        (10, 100),
        (10, 50),
    ],
)
@pytest.mark.parametrize(
    "num_partitions",
    [
        None,  # default number of partitions is 1
        5,
        10,
    ],
)
def test_kafka_strategy_incremental(
    spark,
    processing,
    schema,
    span_gap,
    span_length,
    num_partitions,
):
    hwm_type = KeyValueIntHWM
    topic = secrets.token_hex(6)
    hwm_name = secrets.token_hex(5)
    store = HWMStoreStackManager.get_current()

    kafka = Kafka(
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        spark=spark,
    )

    # change the number of partitions for the Kafka topic to test work for different partitioning cases
    if num_partitions is not None:
        processing.change_topic_partitions(topic, num_partitions)

    reader = DBReader(
        connection=kafka,
        source=topic,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression="offset"),
    )

    # there are 2 spans with a gap between

    # 0..100
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 110..210
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    first_span_max = len(first_span)
    second_span_max = len(second_span) + first_span_max

    # insert first span
    processing.insert_pandas_df_into_topic(first_span, topic)

    # hwm is not in the store
    assert store.get_hwm(hwm_name) is None

    # incremental run
    with IncrementalStrategy():
        first_df = reader.run()

    hwm = store.get_hwm(hwm_name)
    assert hwm is not None
    assert isinstance(hwm, hwm_type)
    assert sum(value for value in hwm.value.values()) == first_span_max

    # all the data has been read
    deserialized_first_df = processing.json_deserialize(first_df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_first_df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_pandas_df_into_topic(second_span, topic)

    with IncrementalStrategy():
        second_df = reader.run()

    assert sum(value for value in store.get_hwm(hwm_name).value.values()) == second_span_max

    deserialized_second_df = processing.json_deserialize(second_df, df_schema=schema)
    processing.assert_subset_df(df=deserialized_second_df, other_frame=second_span)


@pytest.mark.parametrize(
    "num_partitions",
    [
        None,  # default number of partitions is 1
        5,
        10,
    ],
)
def test_kafka_strategy_incremental_nothing_to_read(spark, processing, schema, num_partitions):
    topic = secrets.token_hex(6)
    hwm_name = secrets.token_hex(5)
    store = HWMStoreStackManager.get_current()

    kafka = Kafka(
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        spark=spark,
    )

    # change the number of partitions for the Kafka topic to test work for different partitioning cases
    if num_partitions is not None:
        processing.change_topic_partitions(topic, num_partitions)

    reader = DBReader(
        connection=kafka,
        source=topic,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression="offset"),
    )

    span_gap = 10
    span_length = 50

    # there are 2 spans with a gap between

    # 0..50
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 60..110
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    first_span_max = len(first_span)
    second_span_max = len(second_span) + first_span_max

    # no data yet, nothing to read
    with IncrementalStrategy():
        df = reader.run()

    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert sum(value for value in hwm.value.values()) == 0

    # insert first span
    processing.insert_pandas_df_into_topic(first_span, topic)

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert sum(value for value in hwm.value.values()) == 0

    # set hwm value to 50
    with IncrementalStrategy():
        df = reader.run()

    deserialized_df = processing.json_deserialize(df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=first_span, order_by="id_int")
    hwm = store.get_hwm(name=hwm_name)
    assert sum(value for value in hwm.value.values()) == first_span_max

    # no new data yet, nothing to read
    with IncrementalStrategy():
        df = reader.run()

    assert not df.count()
    # HWM value is unchanged
    hwm = store.get_hwm(name=hwm_name)
    assert sum(value for value in hwm.value.values()) == first_span_max

    # insert second span
    processing.insert_pandas_df_into_topic(second_span, topic)

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert sum(value for value in hwm.value.values()) == first_span_max

    # read data
    with IncrementalStrategy():
        df = reader.run()

    deserialized_df = processing.json_deserialize(df, df_schema=schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=second_span, order_by="id_int")
    hwm = store.get_hwm(name=hwm_name)
    assert sum(value for value in hwm.value.values()) == second_span_max


@pytest.mark.parametrize(
    "hwm_column",
    [
        "float_value",
        "unknown_column",
    ],
)
def test_kafka_strategy_incremental_wrong_hwm(
    spark,
    processing,
    schema,
    hwm_column,
):
    topic = secrets.token_hex(6)
    hwm_name = secrets.token_hex(5)

    kafka = Kafka(
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        spark=spark,
    )

    with pytest.raises(
        ValueError,
        match=r"hwm.expression=.* is not supported by Kafka. Valid values are: \{\'offset\'\}",
    ):
        DBReader(
            connection=kafka,
            source=topic,
            hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
        )
