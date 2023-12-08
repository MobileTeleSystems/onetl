import secrets

import pytest
from etl_entities.hwm import ColumnDateTimeHWM, ColumnIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import MongoDB
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

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
    "hwm_type, hwm_column",
    [
        (ColumnIntHWM, "hwm_int"),
        (ColumnDateTimeHWM, "hwm_datetime"),
    ],
)
@pytest.mark.parametrize(
    "span_gap, span_length",
    [
        (10, 100),
        (10, 50),
    ],
)
def test_mongodb_strategy_incremental(
    spark,
    processing,
    prepare_schema_table,
    df_schema,
    hwm_type,
    hwm_column,
    span_gap,
    span_length,
):
    store = HWMStoreStackManager.get_current()

    mongodb = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_name = secrets.token_hex(5)

    reader = DBReader(
        connection=mongodb,
        table=prepare_schema_table.table,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
        df_schema=df_schema,
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

    first_span_max = first_span[hwm_column].max()
    second_span_max = second_span[hwm_column].max()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # incremental run
    with IncrementalStrategy():
        first_df = reader.run()

    hwm = store.get_hwm(hwm_name)
    assert hwm is not None
    assert isinstance(hwm, hwm_type)
    assert hwm.value == first_span_max

    # all the data has been read
    processing.assert_equal_df(df=first_df, other_frame=first_span)

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    with IncrementalStrategy():
        second_df = reader.run()

    assert store.get_hwm(hwm_name).value == second_span_max

    if "int" in hwm_column:
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)


# Fail if HWM is Numeric, or Decimal with fractional part, or string
@pytest.mark.parametrize(
    "hwm_column, exception_type, error_message",
    [
        ("float_value", ValueError, "Expression 'float_value' returned values"),
        ("text_string", RuntimeError, "Cannot detect HWM type for"),
        ("unknown_column", ValueError, "not found in dataframe schema"),
    ],
)
def test_mongodb_strategy_incremental_wrong_hwm(
    spark,
    processing,
    prepare_schema_table,
    df_schema,
    hwm_column,
    exception_type,
    error_message,
):
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
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
        df_schema=df_schema,
    )

    data = processing.create_pandas_df()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=data,
    )

    with pytest.raises(exception_type, match=error_message):
        # incremental run
        with IncrementalStrategy():
            reader.run()


def test_mongodb_strategy_incremental_explicit_hwm_type(
    spark,
    processing,
    df_schema,
    prepare_schema_table,
):
    store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

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
        source=prepare_schema_table.table,
        df_schema=df_schema,
        # tell DBReader that text_string column contains integer values, and can be used for HWM
        hwm=ColumnIntHWM(name=hwm_name, expression="text_string"),
    )

    data = processing.create_pandas_df()
    data["text_string"] = data["hwm_int"].apply(str)

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=data,
    )

    # incremental run
    with IncrementalStrategy():
        df = reader.run()

    hwm = store.get_hwm(name=hwm_name)
    # type is exactly as set by user
    assert isinstance(hwm, ColumnIntHWM)

    # MongoDB does not support comparison str < int
    assert not df.count()

    # but HWM is updated to max value from the source. yes, that's really weird case.
    # garbage in (wrong HWM type for specific expression) - garbage out (wrong dataframe content)
    assert hwm.value == 99
