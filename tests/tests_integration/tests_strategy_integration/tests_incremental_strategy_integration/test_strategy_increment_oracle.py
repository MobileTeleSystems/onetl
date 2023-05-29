import pytest
from etl_entities import DateHWM, DateTimeHWM, IntHWM

from onetl.connection import Oracle
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

pytestmark = pytest.mark.oracle


# There is no INTEGER column in Oracle, only NUMERIC
# Do not fail in such the case
@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_column",
    [
        "HWM_INT",
        "HWM_DATE",
        "HWM_DATETIME",
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
    hwm_column,
    span_gap,
    span_length,
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
    reader = DBReader(connection=oracle, source=prepare_schema_table.full_name, hwm_column=hwm_column)

    # there are 2 spans with a gap between

    # 0..100
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 110..210
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

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

    if "int" in hwm_column:
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)


# Fail if HWM is Numeric, or Decimal with fractional part, or string
@pytest.mark.parametrize(
    "hwm_column",
    [
        "FLOAT_VALUE",
        "TEXT_STRING",
    ],
)
def test_oracle_strategy_incremental_wrong_hwm_type(spark, processing, prepare_schema_table, hwm_column):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        sid=processing.sid,
        service_name=processing.service_name,
        spark=spark,
    )
    reader = DBReader(connection=oracle, source=prepare_schema_table.full_name, hwm_column=hwm_column)

    data = processing.create_pandas_df()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=data,
    )

    with pytest.raises((KeyError, ValueError)):
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
            IntHWM,
            str,
        ),
        (
            "hwm_date",
            "HWM1_DATE",
            "TO_DATE(TEXT_STRING, 'YYYY-MM-DD')",
            DateHWM,
            lambda x: x.isoformat(),
        ),
        (
            "hwm_datetime",
            "hwm1_datetime",
            "TO_DATE(TEXT_STRING, 'YYYY-MM-DD HH24:MI:SS')",
            DateTimeHWM,
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
        hwm_column=(hwm_column, hwm_expr),
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
    first_span_with_hwm = first_span.copy()
    first_span_with_hwm[hwm_column.upper()] = first_span[hwm_source]

    second_span["text_string"] = second_span[hwm_source].apply(func)
    second_span_with_hwm = second_span.copy()
    second_span_with_hwm[hwm_column.upper()] = second_span[hwm_source]

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
    processing.assert_equal_df(df=first_df, other_frame=first_span_with_hwm)

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    with IncrementalStrategy():
        second_df = reader.run()

    if issubclass(hwm_type, IntHWM):
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span_with_hwm)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span_with_hwm)
