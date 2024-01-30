import secrets

import pytest
from etl_entities.hwm import KeyValueIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import Kafka
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

pytestmark = pytest.mark.kafka


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
    kafka_dataframe_schema,
    kafka_topic,
    num_partitions,
):
    from pyspark.sql.functions import count as spark_count

    hwm_type = KeyValueIntHWM
    hwm_name = secrets.token_hex(5)
    store = HWMStoreStackManager.get_current()

    kafka = Kafka(
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        spark=spark,
    )

    # change the number of partitions for the Kafka topic to test work for different partitioning cases
    if num_partitions is not None:
        processing.change_topic_partitions(kafka_topic, num_partitions)

    reader = DBReader(
        connection=kafka,
        source=kafka_topic,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression="offset"),
    )

    # there are 2 spans with a gap between

    # 0..100
    first_span_begin = 0
    first_span_end = 100

    # 110..210
    second_span_begin = 110
    second_span_end = 210

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # insert first span
    processing.insert_pandas_df_into_topic(first_span, kafka_topic)

    # hwm is not in the store
    assert store.get_hwm(hwm_name) is None

    # incremental run
    with IncrementalStrategy():
        first_df = reader.run()

    hwm = store.get_hwm(hwm_name)
    assert hwm is not None
    assert isinstance(hwm, hwm_type)

    # check that HWM distribution of messages in partitions matches the distribution in sparkDF
    partition_counts = first_df.groupBy("partition").agg(spark_count("*").alias("count"))
    partition_count_dict_first_df = {row["partition"]: row["count"] for row in partition_counts.collect()}
    assert hwm.value == partition_count_dict_first_df

    # all the data has been read
    deserialized_first_df = processing.json_deserialize(first_df, df_schema=kafka_dataframe_schema)
    processing.assert_equal_df(df=deserialized_first_df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_pandas_df_into_topic(second_span, kafka_topic)

    with IncrementalStrategy():
        second_df = reader.run()

    hwm = store.get_hwm(hwm_name)

    # check that HWM distribution of messages in partitions matches the distribution in sparkDF
    combined_df = first_df.union(second_df)
    partition_counts_combined = combined_df.groupBy("partition").agg(spark_count("*").alias("count"))
    partition_count_dict_combined = {row["partition"]: row["count"] for row in partition_counts_combined.collect()}
    assert hwm.value == partition_count_dict_combined

    deserialized_second_df = processing.json_deserialize(second_df, df_schema=kafka_dataframe_schema)
    processing.assert_equal_df(df=deserialized_second_df, other_frame=second_span, order_by="id_int")


@pytest.mark.parametrize(
    "num_partitions",
    [
        None,  # default number of partitions is 1
        5,
        10,
    ],
)
def test_kafka_strategy_incremental_nothing_to_read(
    spark,
    processing,
    kafka_dataframe_schema,
    num_partitions,
    kafka_topic,
):
    from pyspark.sql.functions import count as spark_count

    hwm_name = secrets.token_hex(5)
    store = HWMStoreStackManager.get_current()

    kafka = Kafka(
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        spark=spark,
    )

    # change the number of partitions for the Kafka topic to test work for different partitioning cases
    if num_partitions is not None:
        processing.change_topic_partitions(kafka_topic, num_partitions)

    reader = DBReader(
        connection=kafka,
        source=kafka_topic,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression="offset"),
    )

    # 0..50
    first_span_begin = 0
    first_span_end = 50
    # 60..110
    second_span_begin = 60
    second_span_end = 110

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # no data yet, nothing to read
    with IncrementalStrategy():
        df = reader.run()

    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert all(value == 0 for value in hwm.value.values())

    # insert first span
    processing.insert_pandas_df_into_topic(first_span, kafka_topic)

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert all(value == 0 for value in hwm.value.values())

    # set hwm value to 50
    with IncrementalStrategy():
        first_df = reader.run()

    hwm = store.get_hwm(name=hwm_name)
    # check that HWM distribution of messages in partitions matches the distribution in sparkDF
    partition_counts = first_df.groupBy("partition").agg(spark_count("*").alias("count"))
    partition_count_dict_first_df = {row["partition"]: row["count"] for row in partition_counts.collect()}
    assert hwm.value == partition_count_dict_first_df

    deserialized_df = processing.json_deserialize(first_df, df_schema=kafka_dataframe_schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=first_span, order_by="id_int")

    # no new data yet, nothing to read
    with IncrementalStrategy():
        df = reader.run()

    assert not df.count()
    # HWM value is unchanged
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == partition_count_dict_first_df

    # insert second span
    processing.insert_pandas_df_into_topic(second_span, kafka_topic)

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    # HWM value is unchanged
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == partition_count_dict_first_df

    # read data
    with IncrementalStrategy():
        df = reader.run()

    hwm = store.get_hwm(name=hwm_name)
    # check that HWM distribution of messages in partitions matches the distribution in sparkDF
    combined_df = df.union(first_df)
    partition_counts_combined = combined_df.groupBy("partition").agg(spark_count("*").alias("count"))
    partition_count_dict_combined = {row["partition"]: row["count"] for row in partition_counts_combined.collect()}
    assert hwm.value == partition_count_dict_combined

    deserialized_df = processing.json_deserialize(df, df_schema=kafka_dataframe_schema)
    processing.assert_equal_df(df=deserialized_df, other_frame=second_span, order_by="id_int")


@pytest.mark.parametrize(
    "initial_partitions, additional_partitions",
    [
        (3, 2),  # starting with 3 partitions, adding 2 more
        (5, 1),  # starting with 5 partitions, adding 1 more
    ],
)
def test_kafka_strategy_incremental_with_new_partition(
    spark,
    processing,
    initial_partitions,
    additional_partitions,
    kafka_topic,
):
    from pyspark.sql.functions import count as spark_count

    hwm_name = secrets.token_hex(5)
    store = HWMStoreStackManager.get_current()

    kafka = Kafka(
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
        spark=spark,
    )

    reader = DBReader(
        connection=kafka,
        source=kafka_topic,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression="offset"),
    )

    # Initial setup with `initial_partitions` partitions
    processing.change_topic_partitions(kafka_topic, initial_partitions)

    # 0..50
    first_span_begin = 0
    first_span_end = 100

    # 60..110
    second_span_begin = 60
    second_span_end = 110

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    processing.insert_pandas_df_into_topic(first_span, kafka_topic)
    with IncrementalStrategy():
        first_df = reader.run()

    # it is crucial to save dataframe after reading as if number of partitions is altered before executing any subsequent operations, Spark fails to run them due to
    # Caused by: java.lang.AssertionError: assertion failed: If startingOffsets contains specific offsets, you must specify all TopicPartitions.
    # Use -1 for latest, -2 for earliest.
    # Specified: Set(topic1, topic2) Assigned: Set(topic1, topic2, additional_topic3, additional_topic4)
    first_df.cache()

    hwm = store.get_hwm(name=hwm_name)
    first_run_hwm_keys_num = len(hwm.value.keys())

    processing.change_topic_partitions(kafka_topic, initial_partitions + additional_partitions)
    processing.insert_pandas_df_into_topic(second_span, kafka_topic)

    with IncrementalStrategy():
        second_df = reader.run()

    hwm = store.get_hwm(name=hwm_name)
    second_run_hwm_keys_num = len(hwm.value)
    assert first_run_hwm_keys_num + additional_partitions == second_run_hwm_keys_num

    # check that HWM distribution of messages in partitions matches the distribution in sparkDF
    combined_df = second_df.union(first_df)
    partition_counts_combined = combined_df.groupBy("partition").agg(spark_count("*").alias("count"))
    partition_count_dict_combined = {row["partition"]: row["count"] for row in partition_counts_combined.collect()}
    assert hwm.value == partition_count_dict_combined
