import secrets
import pytest

from contextlib import suppress
from datetime import timedelta, datetime, date

import pandas as pd

from onetl.connection import Postgres
from onetl.reader.db_reader import DBReader
from onetl.strategy import SnapshotStrategy, SnapshotBatchStrategy, IncrementalStrategy
from onetl.strategy.hwm_store import HWMClassRegistry, HWMStoreManager


def test_postgres_strategy_snapshot_hwm_column_present(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=secrets.token_hex())

    with SnapshotStrategy():
        with pytest.raises(ValueError):
            reader.run()


def test_postgres_strategy_snapshot(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name)

    # there is a span 0..50
    span_begin = 0
    span_end = 100
    span = processing.create_pandas_df(min_id=span_begin, max_id=span_end)

    # insert span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=span,
    )

    # snapshot run
    with SnapshotStrategy():
        total_df = reader.run()

    processing.assert_equal_df(df=total_df, other_frame=span)


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", -10),
        ("hwm_date", timedelta(hours=-10)),
        ("hwm_date", timedelta(hours=0)),
        ("hwm_date", timedelta(hours=10)),
        ("hwm_datetime", timedelta(minutes=-60)),
        ("hwm_datetime", timedelta(minutes=0)),
    ],
)
def test_postgres_strategy_snapshot_batch_step_too_small(
    spark, processing, prepare_schema_table, hwm_column, step  # noqa: C812
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

    with pytest.raises(ValueError):
        with SnapshotBatchStrategy(step=step) as part:
            for _ in part:
                reader.run()


def test_postgres_reader_strategy_snapshot_batch_hwm_set_twice(spark, processing, prepare_schema_table):
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

    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            reader1.run()

            with pytest.raises(ValueError):
                reader2.run()

            with pytest.raises(ValueError):
                reader3.run()

            break


@pytest.mark.parametrize(
    "hwm_type_name, hwm_column, step, per_iter",
    [
        ("integer", "hwm_int", 10, 11),  # yes, 11, ids are 0..10, and the first row is included in snapshot strategy
        ("date", "hwm_date", timedelta(days=1), 20),  # this offset is covering span_length + gap
        ("timestamp", "hwm_datetime", timedelta(hours=100), 30),  # same
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
def test_postgres_strategy_snapshot_batch(
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

    # hwm is not in the store
    assert store.get(hwm.qualified_name) is None

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
            assert store.get(hwm.qualified_name) is None

            next_df = reader.run()
            assert next_df.count() <= per_iter

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

            assert store.get(hwm.qualified_name) is None

    # no hwm saves after exiting the context
    assert store.get(hwm.qualified_name) is None

    # all the rows will be read
    total_span = pd.concat([first_span, second_span], ignore_index=True)

    total_df = total_df.sort(total_df.id_int.asc())
    processing.assert_equal_df(df=total_df, other_frame=total_span)


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", 10),  # yes, 11, ids are 0..10, and the first row is included in snapshot strategy
        ("hwm_date", timedelta(days=1)),  # this offset is covering span_length + gap
        ("hwm_datetime", timedelta(hours=100)),  # same
    ],
)
def test_postgres_strategy_snapshot_batch_ignores_hwm_value(
    spark,
    processing,
    prepare_schema_table,
    hwm_column,
    step,
):
    span_length = 100
    span_gap = 50

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

    # init hwm with 100 value
    with IncrementalStrategy():
        reader.run()

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    # snapshot run
    total_df = None
    with SnapshotBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    # init hwm value will be ignored
    # all the rows will be read
    total_span = pd.concat([first_span, second_span], ignore_index=True)

    total_df = total_df.sort(total_df.id_int.asc())
    processing.assert_equal_df(df=total_df, other_frame=total_span)


@pytest.mark.parametrize(
    "hwm_column, step, stop",
    [
        ("hwm_int", 10, 50),  # step <  stop
        ("hwm_int", 50, 10),  # step >  stop
        ("hwm_int", 50, 50),  # step == stop
        ("hwm_date", timedelta(days=1), date.today() + timedelta(days=10)),  # this stop is covering span_length + gap
        ("hwm_datetime", timedelta(hours=100), datetime.now() + timedelta(days=10)),  # same
    ],
)
@pytest.mark.parametrize("span_length", [100, 40, 5])
def test_postgres_strategy_snapshot_batch_stop(
    spark, processing, prepare_schema_table, hwm_column, step, stop, span_length  # noqa: C812
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

    # snapshot run until row with hwm_column == stop will
    total_df = None
    with SnapshotBatchStrategy(step=step, stop=stop) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    total_pandas_df = total_df.toPandas()

    # only a small part of input data has been read
    # so instead of checking the whole dataframe a partial comparison should be performed
    for column in total_pandas_df.columns:
        total_pandas_df[column].isin(span[column]).all()

    # check that stop clause working as expected
    assert (total_pandas_df[hwm_column] <= stop).all()


def test_postgres_strategy_snapshot_batch_handle_exception(spark, processing, prepare_schema_table):  # noqa: C812
    hwm_column = "hwm_int"
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

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

    # init hwm with 100 value
    with IncrementalStrategy():
        reader.run()

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
                # raise exception somethere in the middle of the read process
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

    # all the rows will be read
    total_span = pd.concat([first_span, second_span], ignore_index=True)
    total_df = total_df.sort(total_df.id_int.asc())

    processing.assert_equal_df(df=total_df, other_frame=total_span)
