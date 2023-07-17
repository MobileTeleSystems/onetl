from datetime import timedelta

import pytest
from etl_entities import DateTimeHWM, IntHWM

from onetl.connection import MongoDB
from onetl.db import DBReader
from onetl.hwm.store import HWMStoreManager
from onetl.strategy import IncrementalBatchStrategy

pytestmark = pytest.mark.mongodb


@pytest.fixture()
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
            StructField("_id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("hwm_int", IntegerType()),
            StructField("hwm_datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_type, hwm_column, step, per_iter",
    [
        (IntHWM, "hwm_int", 20, 30),  # step <  per_iter
        (IntHWM, "hwm_int", 30, 30),  # step == per_iter
        (DateTimeHWM, "hwm_datetime", timedelta(weeks=2), 20),  # same
    ],
)
@pytest.mark.parametrize(
    "span_gap, span_length",
    [
        (50, 100),  # step < gap < span_length
        (50, 40),  # step < gap > span_length
        (5, 20),  # gap < step < span_length
        (20, 5),  # span_length < step < gap
        (5, 2),  # gap < span_length < step
        (2, 5),  # span_length < gap < step
        (1, 1),  # minimal gap possible
    ],
)
def test_mongodb_strategy_incremental_batch(
    spark,
    processing,
    prepare_schema_table,
    df_schema,
    hwm_type,
    hwm_column,
    step,
    per_iter,
    span_gap,
    span_length,
):
    store = HWMStoreManager.get_current()

    mongodb = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=mongodb, table=prepare_schema_table.table, hwm_column=hwm_column, df_schema=df_schema)

    hwm = hwm_type(source=reader.source, column=reader.hwm_column)

    # there are 2 spans with a gap between
    # 0..100
    first_span_begin = 0
    first_span_end = span_length
    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)

    # 150..250
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    first_span_max = first_span[hwm_column].max()
    second_span_max = second_span[hwm_column].max()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # hwm is not in the store
    assert store.get(hwm.qualified_name) is None

    # fill up hwm storage with last value, e.g. 100
    first_df = None
    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if first_df is None:
                first_df = next_df
            else:
                first_df = first_df.union(next_df)

    # same behavior as SnapshotBatchStrategy, no rows skipped
    if "int" in hwm_column:
        # only changed data has been read
        processing.assert_equal_df(df=first_df, other_frame=first_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=first_df, other_frame=first_span)

    # hwm is set
    hwm = store.get(hwm.qualified_name)
    assert hwm is not None
    assert isinstance(hwm, hwm_type)
    assert hwm.value == first_span_max

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # incremental run with 10 rows per iter
    # but only hwm_column > 100 and hwm_column <= 250
    second_df = None
    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            hwm = store.get(hwm.qualified_name)
            assert hwm is not None
            assert isinstance(hwm, hwm_type)
            assert first_span_max <= hwm.value <= second_span_max

            next_df = reader.run()
            assert next_df.count() <= per_iter

            if second_df is None:
                second_df = next_df
            else:
                second_df = second_df.union(next_df)

            hwm = store.get(hwm.qualified_name)
            assert hwm is not None
            assert isinstance(hwm, hwm_type)
            assert first_span_max <= hwm.value <= second_span_max

    hwm = store.get(hwm.qualified_name)
    assert hwm is not None
    assert isinstance(hwm, hwm_type)
    assert hwm.value == second_span_max

    if "int" in hwm_column:
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)


def test_mongodb_strategy_incremental_batch_where(spark, processing, prepare_schema_table, df_schema):
    mongodb = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=mongodb,
        table=prepare_schema_table.table,
        where={"$or": [{"float_value": {"$lt": 51}}, {"float_value": {"$gt": 101, "$lt": 120}}]},
        hwm_column="hwm_int",
        df_schema=df_schema,
    )

    # there are 2 spans
    # 0..100
    first_span_begin = 0
    first_span_end = 100
    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)

    # 101..250
    second_span_begin = 101
    second_span_end = 200
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    first_df = None
    with IncrementalBatchStrategy(step=10) as batches:
        for _ in batches:
            next_df = reader.run()

            if first_df is None:
                first_df = next_df
            else:
                first_df = first_df.union(next_df)

    processing.assert_equal_df(df=first_df, other_frame=first_span[:51])

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    second_df = None
    with IncrementalBatchStrategy(step=10) as batches:
        for _ in batches:
            next_df = reader.run()

            if second_df is None:
                second_df = next_df
            else:
                second_df = second_df.union(next_df)

    processing.assert_equal_df(df=second_df, other_frame=second_span[:19])
