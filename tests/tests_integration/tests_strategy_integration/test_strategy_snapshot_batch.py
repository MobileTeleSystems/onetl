import re
import secrets
from contextlib import suppress
from datetime import date, datetime, timedelta

import pytest

try:
    import pandas

except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

from etl_entities.hwm import ColumnIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import SnapshotBatchStrategy, SnapshotStrategy

pytestmark = pytest.mark.postgres


def test_postgres_strategy_snapshot_hwm_column_present(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    column = secrets.token_hex(5)
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=column),
    )

    error_message = "DBReader(hwm=...) cannot be used with SnapshotStrategy"
    with SnapshotStrategy():
        with pytest.raises(RuntimeError, match=re.escape(error_message)):
            reader.run()


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", "abc"),
        ("hwm_int", timedelta(hours=10)),
        ("hwm_date", 10),
        ("hwm_date", 1.5),
        ("hwm_date", "abc"),
        ("hwm_datetime", 10),
        ("hwm_datetime", 1.5),
        ("hwm_datetime", "abc"),
    ],
)
def test_postgres_strategy_snapshot_batch_wrong_step_type(
    spark,
    processing,
    prepare_schema_table,
    load_table_data,
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    with pytest.raises((TypeError, ValueError)):
        with SnapshotBatchStrategy(step=step) as part:
            for _ in part:
                reader.run()


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", -10),
        ("hwm_date", timedelta(days=-10)),
        ("hwm_datetime", timedelta(minutes=-60)),
    ],
)
def test_postgres_strategy_snapshot_batch_step_negative(
    spark,
    processing,
    prepare_schema_table,
    load_table_data,
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

    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    error_msg = "HWM value is not increasing, please check options passed to SnapshotBatchStrategy"
    with pytest.raises(ValueError, match=error_msg):
        with SnapshotBatchStrategy(step=step) as batches:
            for _ in batches:
                reader.run()


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", 0.5),
        ("hwm_date", timedelta(days=1)),
        ("hwm_datetime", timedelta(hours=1)),
    ],
)
def test_postgres_strategy_snapshot_batch_step_too_small(
    spark,
    processing,
    prepare_schema_table,
    load_table_data,
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    error_msg = f"step={step!r} parameter of SnapshotBatchStrategy leads to generating too many iterations"
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        with SnapshotBatchStrategy(step=step) as batches:
            for _ in batches:
                reader.run()


def test_postgres_strategy_snapshot_batch_outside_loop(
    spark,
    processing,
    load_table_data,
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
        source=load_table_data.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
    )

    error_message = "Invalid SnapshotBatchStrategy usage!"
    with pytest.raises(RuntimeError, match=re.escape(error_message)):
        with SnapshotBatchStrategy(step=1):
            reader.run()


def test_postgres_strategy_snapshot_batch_hwm_set_twice(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    step = 20

    table1 = load_table_data.full_name
    table2 = f"{secrets.token_hex(5)}.{secrets.token_hex(5)}"

    reader1 = DBReader(
        connection=postgres,
        table=table1,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
    )
    reader2 = DBReader(
        connection=postgres,
        table=table1,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
    )
    reader3 = DBReader(
        connection=postgres,
        table=table2,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
    )

    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            reader1.run()

            with pytest.raises(
                ValueError,
                match="Detected wrong SnapshotBatchStrategy usage.",
            ):
                reader2.run()

            with pytest.raises(
                ValueError,
                match="Detected wrong SnapshotBatchStrategy usage.",
            ):
                reader3.run()

            break


def test_postgres_strategy_snapshot_batch_where(spark, processing, prepare_schema_table):
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
        source=prepare_schema_table.full_name,
        where="float_value < 50 OR float_value = 50.50",
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
    )

    # there is a span 0..100
    span_begin = 0
    span_end = 100
    span = processing.create_pandas_df(min_id=span_begin, max_id=span_end)

    # insert span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=span,
    )

    # snapshot run with only 10 rows per run
    df = None
    with SnapshotBatchStrategy(step=10) as batches:
        for _ in batches:
            next_df = reader.run()
            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    processing.assert_equal_df(df=df, other_frame=span[:51], order_by="id_int")


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_column, step, per_iter",
    [
        (
            "hwm_int",
            10,
            11,
        ),  # yes, 11, ids are 0..10, and the first row is included in snapshot strategy
        (
            "hwm_date",
            timedelta(days=4),
            30,
        ),  # per_iter value is calculated to cover the step value
        ("hwm_datetime", timedelta(hours=100), 30),  # same
    ],
)
@pytest.mark.parametrize(
    "span_gap, span_length",
    [
        (50, 60),  # step < gap < span_length
        (50, 40),  # step < gap > span_length
        (5, 20),  # gap < step < span_length
        (20, 5),  # span_length < step < gap
        (5, 2),  # gap < span_length < step
        (2, 5),  # span_length < gap < step
        (1, 1),  # minimal gap possible
    ],
)
def test_postgres_strategy_snapshot_batch(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    step,
    per_iter,
    span_gap,
    span_length,
):
    store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

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
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
    )

    # hwm is not in the store
    assert store.get_hwm(hwm_name) is None

    # there are 2 spans with a gap between
    # 0..100
    first_span_begin = 0
    first_span_end = span_length
    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)

    # 150..200
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # snapshot run with only 10 rows per run
    total_df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            # no hwm saves on each iteration
            assert store.get_hwm(hwm_name) is None

            next_df = reader.run()
            assert next_df.count() <= per_iter

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

            assert store.get_hwm(hwm_name) is None

    # no hwm saves after exiting the context
    assert store.get_hwm(hwm_name) is None

    # all the rows will be read
    total_span = pandas.concat([first_span, second_span], ignore_index=True)

    processing.assert_equal_df(df=total_df, other_frame=total_span, order_by="id_int")


def test_postgres_strategy_snapshot_batch_ignores_hwm_value(
    spark,
    processing,
    prepare_schema_table,
):
    store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)
    hwm_column = "id_int"
    hwm = ColumnIntHWM(name=hwm_name, expression=hwm_column)
    step = 10

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
        source=prepare_schema_table.full_name,
        hwm=hwm,
    )

    span = processing.create_pandas_df()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=span,
    )

    # set HWM value in HWM store
    first_span_max = span[hwm_column].max()
    fake_hwm = hwm.copy().set_value(first_span_max)
    store.set_hwm(fake_hwm)

    # snapshot run
    total_df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    # all the rows are be read, HWM store is completely ignored
    processing.assert_equal_df(df=total_df, other_frame=span, order_by="id_int")

    # HWM in hwm store is left intact
    assert store.get_hwm(hwm_name) == fake_hwm


@pytest.mark.parametrize(
    "hwm_column, step, stop",
    [
        ("hwm_int", 10, 50),  # step <  stop
        ("hwm_int", 50, 10),  # step >  stop
        ("hwm_int", 50, 50),  # step == stop
        ("hwm_date", timedelta(days=1), date.today() + timedelta(days=10)),  # this step is covering span_length + gap
        ("hwm_datetime", timedelta(hours=100), datetime.now() + timedelta(days=10)),  # same
    ],
)
@pytest.mark.parametrize("span_length", [100, 40, 5])
def test_postgres_strategy_snapshot_batch_stop(
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

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

    # snapshot run until row with hwm_column == stop will
    total_df = None
    with SnapshotBatchStrategy(step=step, stop=stop) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    # check that stop clause working as expected
    total_span = span[span[hwm_column] <= stop]
    processing.assert_equal_df(df=total_df, other_frame=total_span, order_by="id_int")


def test_postgres_strategy_snapshot_batch_handle_exception(spark, processing, prepare_schema_table):
    hwm_column = "hwm_int"
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
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    step = 10

    span_gap = 50
    span_length = 100

    # there are 2 spans with a gap between
    # 0..100
    first_span_begin = 0
    first_span_end = span_length
    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)

    # 150..200
    second_span_begin = first_span_end + span_gap
    second_span_end = second_span_begin + span_length
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    first_df = None
    raise_counter = 0
    with suppress(ValueError):
        with SnapshotBatchStrategy(step=step) as batches:
            for _ in batches:
                if first_df is None:
                    first_df = reader.run()
                else:
                    first_df = first_df.union(reader.run())

                raise_counter += step
                # raise exception somewhere in the middle of the read process
                if raise_counter >= span_gap + (span_length // 2):
                    raise ValueError("some error")

    # and then process is retried
    total_df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    # all the rows are be read
    total_span = pandas.concat([first_span, second_span], ignore_index=True)
    processing.assert_equal_df(df=total_df, other_frame=total_span, order_by="id_int")


def test_postgres_strategy_snapshot_batch_nothing_to_read(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_name = secrets.token_hex(5)
    step = 10

    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression="hwm_int"),
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

    # no data yet, nothing to read
    df = None
    counter = 0
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()
            counter += 1

            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    # exactly 1 batch with empty result
    assert counter == 1
    assert not df.count()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # .run() is not called - dataframe still empty (unlike SnapshotStrategy)
    assert not df.count()

    # read data
    df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")

    # read data again - same output, HWM is not saved to HWM store
    df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # .run() is not called - dataframe contains only old data (unlike SnapshotStrategy)
    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")

    # read data
    df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()
            counter += 1

            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    total_span = pandas.concat([first_span, second_span], ignore_index=True)
    processing.assert_equal_df(df=df, other_frame=total_span, order_by="id_int")


def test_postgres_has_data_outside_snapshot_batch_strategy(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=ColumnIntHWM(name=secrets.token_hex(5), expression="text_string"),
    )

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Check documentation DBReader.has_data(): ",
        ),
    ):
        reader.has_data()
