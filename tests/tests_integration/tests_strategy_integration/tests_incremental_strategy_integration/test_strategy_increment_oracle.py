import secrets
from datetime import datetime

import pytest
from etl_entities.hwm import ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import Oracle
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

pytestmark = pytest.mark.oracle


# There is no INTEGER column in Oracle, only NUMERIC
# Do not fail in such the case
@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_type, hwm_column",
    [
        (ColumnIntHWM, "HWM_INT"),
        # there is no Date type in Oracle
        (ColumnDateTimeHWM, "HWM_DATE"),
        (ColumnDateTimeHWM, "HWM_DATETIME"),
    ],
)
@pytest.mark.parametrize(
    "span_gap, span_length",
    [
        (10, 100),
        (10, 50),
    ],
)
def test_oracle_strategy_incremental(
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

    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        sid=processing.sid,
        service_name=processing.service_name,
        spark=spark,
    )
    reader = DBReader(
        connection=oracle,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, column=hwm_column),
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

    first_span_max = first_span[hwm_column.lower()].max()
    second_span_max = second_span[hwm_column.lower()].max()

    if "datetime" in hwm_column.lower():
        # Oracle store datetime only with second precision
        first_span_max = first_span_max.replace(microsecond=0, nanosecond=0)
        second_span_max = second_span_max.replace(microsecond=0, nanosecond=0)
    elif "date" in hwm_column.lower():
        # Oracle does not support date type, convert to datetime
        first_span_max = datetime.fromisoformat(first_span_max.isoformat())
        second_span_max = datetime.fromisoformat(second_span_max.isoformat())

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
        ("FLOAT_VALUE", ValueError, "Expression 'FLOAT_VALUE' returned values"),
        ("TEXT_STRING", RuntimeError, "Cannot detect HWM type for"),
        ("UNKNOWN_COLUMN", Exception, "java.sql.SQLSyntaxErrorException"),
    ],
)
def test_oracle_strategy_incremental_wrong_hwm(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    exception_type,
    error_message,
):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        sid=processing.sid,
        service_name=processing.service_name,
        spark=spark,
    )
    reader = DBReader(
        connection=oracle,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), column=hwm_column),
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


@pytest.mark.parametrize(
    "hwm_source, hwm_column, hwm_expr, hwm_type, func",
    [
        (
            "hwm_int",
            "HWM1_INT",
            "TO_NUMBER(TEXT_STRING)",
            ColumnIntHWM,
            str,
        ),
        (
            "hwm_date",
            "HWM1_DATE",
            "TO_DATE(TEXT_STRING, 'YYYY-MM-DD')",
            ColumnDateHWM,
            lambda x: x.isoformat(),
        ),
        (
            "hwm_datetime",
            "hwm1_datetime",
            "TO_DATE(TEXT_STRING, 'YYYY-MM-DD HH24:MI:SS')",
            ColumnDateTimeHWM,
            lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    ],
)
def test_oracle_strategy_incremental_with_hwm_expr(
    spark,
    processing,
    prepare_schema_table,
    hwm_source,
    hwm_column,
    hwm_expr,
    hwm_type,
    func,
):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        sid=processing.sid,
        service_name=processing.service_name,
        spark=spark,
    )

    reader = DBReader(
        connection=oracle,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), column=hwm_column, expression=hwm_expr),
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
    processing.assert_equal_df(df=first_df, other_frame=first_span)

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
        processing.assert_equal_df(df=second_df, other_frame=second_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)
