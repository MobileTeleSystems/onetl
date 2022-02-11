from contextlib import suppress
from datetime import date, timedelta, datetime
import secrets
import pytest

import pandas as pd

from onetl.connection import Postgres
from onetl.reader.db_reader import DBReader
from onetl.strategy import IncrementalStrategy, IncrementalBatchStrategy
from onetl.strategy.hwm_store import HWMClassRegistry, HWMStoreManager


@pytest.mark.parametrize(
    "hwm_column",
    [
        "unknown_column",
        "HWM_INT",  # wrong case
    ],
)
def test_postgres_strategy_incremental_unknown_hwm_column(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=prepare_schema_table.full_name,
        hwm_column=hwm_column,
    )

    with pytest.raises(KeyError):
        with IncrementalStrategy():
            reader.run()


def test_postgres_reader_strategy_incremental_hwm_set_twice(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    table1 = prepare_schema_table.full_name
    table2 = f"{secrets.token_hex()}.{secrets.token_hex()}"

    hwm_column1 = "hwm_int"
    hwm_column2 = "hwm_datetime"

    reader1 = DBReader(connection=postgres, table=table1, hwm_column=hwm_column1)
    reader2 = DBReader(connection=postgres, table=table2, hwm_column=hwm_column1)
    reader3 = DBReader(connection=postgres, table=table1, hwm_column=hwm_column2)

    with IncrementalStrategy():
        reader1.run()

        with pytest.raises(ValueError):
            reader2.run()

        with pytest.raises(ValueError):
            reader3.run()


# Fail if HWM is Numeric, or Decimal with fractional part, or string
@pytest.mark.parametrize(
    "hwm_column",
    [
        "float_value",
        "text_string",
    ],
)
def test_postgres_strategy_incremental_wrong_hwm_type(spark, processing, prepare_schema_table, hwm_column):
    postgres = Postgres(
        host=processing.host,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

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
    "hwm_type_name, hwm_column",
    [
        ("integer", "hwm_int"),
        ("date", "hwm_date"),
        ("timestamp", "hwm_datetime"),
    ],
)
@pytest.mark.parametrize(
    "span_gap, span_length",
    [
        (10, 100),
        (10, 50),
    ],
)
def test_postgres_strategy_incremental(
    spark,
    processing,
    prepare_schema_table,
    hwm_type_name,
    hwm_column,
    span_gap,
    span_length,
):
    store = HWMStoreManager.get_current()

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    hwm_type = HWMClassRegistry.get(hwm_type_name)
    hwm = hwm_type(source=reader.table, column=reader.hwm_column)

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

    # hwm is not in the store
    assert store.get(hwm.qualified_name) is None

    # incremental run
    with IncrementalStrategy():
        first_df = reader.run()

    hwm = store.get(hwm.qualified_name)
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

    assert store.get(hwm.qualified_name).value == second_span_max

    if "int" in hwm_column:
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=second_df, other_frame=second_span)


def test_postgres_strategy_incremental_where(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    # resulting WHERE clause should be "(id < 1000 OR id = 1000) AND hwm_int > 100"
    # not like this: "id < 1000 OR id = 1000 AND hwm_int > 100"
    reader = DBReader(
        connection=postgres,
        table=prepare_schema_table.full_name,
        where="id_int < 1000 OR id_int = 1000",
        hwm_column="hwm_int",
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

    # only changed data has been read
    processing.assert_equal_df(df=second_df, other_frame=second_span)


@pytest.mark.parametrize(
    "span_gap, span_length, hwm_column, offset",
    [
        (10, 50, "hwm_int", 50 + 10 + 50 + 1),  # offset >  span_length + gap
        (50, 10, "hwm_int", 10 + 50 + 10 + 1),  # offset <  span_length + gap
        (10, 50, "hwm_date", timedelta(weeks=20)),  # this offset covers span_length + gap
        (10, 50, "hwm_datetime", timedelta(days=140)),  # this offset covers span_length + gap
    ],
)
def test_postgres_strategy_incremental_offset(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    offset,
    span_gap,
    span_length,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    # there are 2 spans with a gap between

    # 0..50
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 60..110
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # set hwm value to second offset max value, e.g. 110
    with IncrementalStrategy():
        next_df = reader.run()

    # first span was late
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # but offset=111 allows to read old values (hwm_column > (hwm - first_offset - gap - second_offset - 1))
    with IncrementalStrategy(offset=offset):
        next_df = reader.run()

    total_span = pd.concat([second_span, first_span], ignore_index=True)
    processing.assert_equal_df(df=next_df, other_frame=total_span)


def test_postgres_strategy_incremental_handle_exception(spark, processing, prepare_schema_table):  # noqa: C812
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_column = "hwm_int"
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

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

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # set hwm value to 50
    with IncrementalStrategy():
        reader.run()

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # process is failed
    with suppress(ValueError):
        with IncrementalStrategy():
            reader.run()
            raise ValueError("some error")

    # and then process is retried
    with IncrementalStrategy():
        reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

        second_df = reader.run()

    # all the data from the second span has been read
    # like there was no exception
    second_df = second_df.sort(second_df.id_int.asc())
    processing.assert_equal_df(df=second_df, other_frame=second_span)


def test_postgres_reader_strategy_incremental_batch_outside_loop(  # noqa: WPS118
    spark,
    processing,
    prepare_schema_table,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=prepare_schema_table.full_name,
        hwm_column="hwm_int",
    )

    with pytest.raises(RuntimeError):
        with IncrementalBatchStrategy(step=1):
            reader.run()


@pytest.mark.parametrize(
    "hwm_column",
    [
        "unknown_column",
        "HWM_INT",  # wrong case
    ],
)
def test_postgres_strategy_incremental_batch_unknown_hwm_column(  # noqa: WPS118
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        table=prepare_schema_table.full_name,
        hwm_column=hwm_column,
    )

    with pytest.raises(KeyError):
        with IncrementalBatchStrategy(step=1) as batches:
            for _ in batches:
                reader.run()


def test_postgres_reader_strategy_incremental_batch_hwm_set_twice(  # noqa: WPS118
    spark,
    processing,
    prepare_schema_table,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    step = 1

    table1 = prepare_schema_table.full_name
    table2 = f"{secrets.token_hex()}.{secrets.token_hex()}"

    hwm_column1 = "hwm_int"
    hwm_column2 = "hwm_datetime"

    reader1 = DBReader(connection=postgres, table=table1, hwm_column=hwm_column1)
    reader2 = DBReader(connection=postgres, table=table2, hwm_column=hwm_column1)
    reader3 = DBReader(connection=postgres, table=table1, hwm_column=hwm_column2)

    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            reader1.run()

            with pytest.raises(ValueError):
                reader2.run()

            with pytest.raises(ValueError):
                reader3.run()

            break


# Fail if HWM is Numeric, or Decimal with fractional part, or string
@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("float_value", 1.0),
        ("text_string", "abc"),
    ],
)
def test_postgres_strategy_incremental_batch_wrong_hwm_type(spark, processing, prepare_schema_table, hwm_column, step):
    postgres = Postgres(
        host=processing.host,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    data = processing.create_pandas_df()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=data,
    )

    with pytest.raises((KeyError, ValueError)):
        # incremental run
        with IncrementalBatchStrategy(step=step) as batches:
            for _ in batches:
                reader.run()


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", -10),
        ("hwm_int", 0),
        ("hwm_int", 0.5),
        ("hwm_int", "abc"),
        ("hwm_int", timedelta(hours=10)),
        ("hwm_date", timedelta(hours=-10)),
        ("hwm_date", timedelta(hours=0)),
        ("hwm_date", timedelta(hours=10)),
        ("hwm_date", 10),
        ("hwm_date", 0.5),
        ("hwm_date", "abc"),
        ("hwm_datetime", timedelta(minutes=-60)),
        ("hwm_datetime", timedelta(minutes=0)),
        ("hwm_datetime", 10),
        ("hwm_datetime", 0.5),
        ("hwm_datetime", "abc"),
    ],
)
def test_postgres_reader_strategy_incremental_batch_wrong_step(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    step,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    with pytest.raises((TypeError, ValueError)):
        with IncrementalBatchStrategy(step=step) as part:
            for _ in part:
                reader.run()


@pytest.mark.parametrize(
    "hwm_type_name, hwm_column, step, per_iter",
    [
        ("integer", "hwm_int", 20, 30),  # step <  per_iter
        ("integer", "hwm_int", 30, 30),  # step == per_iter
        ("date", "hwm_date", timedelta(days=20), 20),  # per_iter value is calculated to cover the step value
        ("timestamp", "hwm_datetime", timedelta(weeks=2), 20),  # same
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
    ],
)
def test_postgres_strategy_incremental_batch(
    spark,
    processing,
    prepare_schema_table,
    hwm_type_name,
    hwm_column,
    step,
    per_iter,
    span_gap,
    span_length,
):
    store = HWMStoreManager.get_current()

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    hwm_type = HWMClassRegistry.get(hwm_type_name)
    hwm = hwm_type(source=reader.table, column=reader.hwm_column)

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


@pytest.mark.parametrize(
    "hwm_column, step, stop",
    [
        ("hwm_int", 10, 50),  # step <  stop
        ("hwm_int", 50, 10),  # step >  stop
        ("hwm_int", 50, 50),  # step == stop
        ("hwm_date", timedelta(days=10), date.today() + timedelta(days=40)),
        ("hwm_datetime", timedelta(hours=100), datetime.now() + timedelta(days=10)),
    ],
)
@pytest.mark.parametrize("span_length", [100, 40, 5])
def test_postgres_strategy_incremental_batch_stop(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    step,
    stop,
    span_length,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    # there is a span 0..100
    span_begin = 0
    span_end = span_length
    span = processing.create_pandas_df(min_id=span_begin, max_id=span_end)

    # insert span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=span,
    )

    # incremental run until row with hwm_column == stop will met
    total_df = None
    with IncrementalBatchStrategy(step=step, stop=stop) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    # only a small part of input data has been read
    # so instead of checking the whole dataframe a partial comparison should be performed
    processing.assert_subset_df(df=total_df, other_frame=span)

    # check that stop clause working as expected
    total_pandas_df = total_df.toPandas()
    assert (total_pandas_df[hwm_column] <= stop).all()


@pytest.mark.parametrize(
    "span_gap, span_length, hwm_column, step, offset, full",
    [
        (10, 60, "hwm_int", 100, 40 + 10 + 40 + 1, False),  # step >  offset, step <  span_length + gap
        (10, 60, "hwm_int", 100, 60 + 10 + 60 + 1, True),  # step <  offset, step <  span_length + gap
        (10, 60, "hwm_int", 100, 100, False),  # step == offset, step <  span_length + gap
        (10, 40, "hwm_int", 100, 40 + 10 + 40 + 1, True),  # step >  offset, step >  span_length + gap
        (10, 40, "hwm_int", 100, 60 + 10 + 60 + 1, True),  # step <  offset, step >  span_length + gap
        (10, 40, "hwm_int", 100, 100, False),  # step == offset, step >  span_length + gap
        (10, 45, "hwm_int", 100, 40 + 10 + 40 + 1, False),  # step >  offset, step == span_length + gap
        (10, 45, "hwm_int", 100, 60 + 10 + 60 + 1, True),  # step <  offset, step == span_length + gap
        (10, 45, "hwm_int", 100, 100, False),  # step == offset, step == span_length + gap
        (10, 40, "hwm_date", timedelta(days=10), timedelta(weeks=17), False),  # date values have a random part
        (10, 40, "hwm_datetime", timedelta(hours=100), timedelta(days=116), False),  # same thing
    ],
)
def test_postgres_strategy_incremental_batch_offset(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    offset,
    span_gap,
    span_length,
    step,
    full,
):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    # there are 2 spans with a gap between

    # 0..40
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 50..90
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length

    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # set hwm value to second offset max value, e.g. 90
    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            reader.run()

    # first span was late
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # but offset=91 allows to read old values (hwm_column > (hwm - first_offset - gap - second_offset - 1))
    total_df = None
    with IncrementalBatchStrategy(step=step, offset=offset) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    total_span = pd.concat([first_span, second_span], ignore_index=True)

    if full:
        total_df = total_df.sort(total_df.id_int.asc())
        # all the data has been read
        processing.assert_equal_df(df=total_df, other_frame=total_span)
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=total_df, other_frame=total_span)
