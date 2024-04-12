import secrets

import pytest
from etl_entities.hwm import ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import Clickhouse
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

pytestmark = pytest.mark.clickhouse


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_type, hwm_column",
    [
        (ColumnIntHWM, "hwm_int"),
        (ColumnDateHWM, "hwm_date"),
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
def test_clickhouse_strategy_incremental(
    spark,
    processing,
    prepare_schema_table,
    hwm_type,
    hwm_column,
    span_gap,
    span_length,
):
    store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
    )
    reader = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
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

    assert store.get_hwm(hwm_name).value == first_span_max

    # all the data has been read
    processing.assert_equal_df(df=first_df, other_frame=first_span, order_by="id_int")

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
        processing.assert_equal_df(df=second_df, other_frame=second_span, order_by="id_int")
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)


def test_clickhouse_strategy_incremental_nothing_to_read(spark, processing, prepare_schema_table):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
    )

    store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)
    hwm_column = "hwm_int"

    reader = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
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

    first_span_max = first_span[hwm_column].max()
    second_span_max = second_span[hwm_column].max()

    # no data yet, nothing to read
    with IncrementalStrategy():
        assert not reader.has_data()
        df = reader.run()

    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value is None

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value is None

    # set hwm value to 50
    with IncrementalStrategy():
        assert reader.has_data()
        df = reader.run()

    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == first_span_max

    # no new data yet, nothing to read
    with IncrementalStrategy():
        df = reader.run()

    assert not df.count()
    # HWM value is unchanged
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == first_span_max

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == first_span_max

    # read data
    with IncrementalStrategy():
        df = reader.run()

    processing.assert_equal_df(df=df, other_frame=second_span, order_by="id_int")
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == second_span_max


@pytest.mark.flaky(
    # sometimes test fails with vague error on Spark side, e.g. `An error occurred while calling o58.version`
    only_rerun="py4j.protocol.Py4JError",
)
@pytest.mark.parametrize(
    "hwm_column, exception_type, error_message",
    [
        ("float_value", ValueError, "Expression 'float_value' returned values"),
        ("text_string", RuntimeError, "Cannot detect HWM type for"),
        ("unknown_column", Exception, "Missing columns"),
    ],
)
def test_clickhouse_strategy_incremental_wrong_hwm(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    exception_type,
    error_message,
):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
    )
    reader = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
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


def test_clickhouse_strategy_incremental_explicit_hwm_type(
    spark,
    processing,
    prepare_schema_table,
):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
        # tell DBReader that text_string column contains integer values, and can be used for HWM
        hwm=ColumnIntHWM(name=secrets.token_hex(5), expression="text_string"),
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
    with pytest.raises(Exception, match="There is no supertype for types String, UInt8 because"):
        with IncrementalStrategy():
            reader.run()


@pytest.mark.parametrize(
    "hwm_source, hwm_expr, hwm_type, func",
    [
        (
            "hwm_int",
            "CAST(text_string AS Integer)",
            ColumnIntHWM,
            str,
        ),
        (
            "hwm_date",
            "CAST(text_string AS Date)",
            ColumnDateHWM,
            lambda x: x.isoformat(),
        ),
        (
            "hwm_datetime",
            "CAST(text_string AS DateTime)",
            ColumnDateTimeHWM,
            lambda x: x.isoformat(),
        ),
        (
            "hwm_datetime",
            "CAST(text_string AS DateTime32)",
            ColumnDateTimeHWM,
            lambda x: x.isoformat(),
        ),
        (
            "hwm_datetime",
            "CAST(text_string AS DateTime64)",
            ColumnDateTimeHWM,
            lambda x: x.isoformat(),
        ),
        (
            "hwm_datetime",
            "CAST(text_string AS DateTime64(6))",
            ColumnDateTimeHWM,
            lambda x: x.isoformat(),
        ),
    ],
)
def test_clickhouse_strategy_incremental_with_hwm_expr(
    spark,
    processing,
    prepare_schema_table,
    hwm_source,
    hwm_expr,
    hwm_type,
    func,
):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_expr),
    )

    # there are 2 spans with a gap between
    span_gap = 10
    span_length = 50

    # 0..100
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 110..210
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    first_span["text_string"] = first_span[hwm_source].apply(func)
    second_span["text_string"] = second_span[hwm_source].apply(func)

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # incremental run
    with IncrementalStrategy():
        first_df = reader.run()

    # all the data has been read
    processing.assert_equal_df(df=first_df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    with IncrementalStrategy():
        second_df = reader.run()

    if issubclass(hwm_type, ColumnIntHWM):
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span, order_by="id_int")
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)
