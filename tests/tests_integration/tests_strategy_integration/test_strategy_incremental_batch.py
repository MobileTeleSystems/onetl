import re
import secrets
from datetime import date, datetime, timedelta

import pytest
from etl_entities.hwm import ColumnDateHWM, ColumnDateTimeHWM, ColumnIntHWM

from tests.util.rand import rand_str

try:
    import pandas

    from tests.util.to_pandas import to_pandas
except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

from etl_entities.hwm_store import HWMStoreStackManager

from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import IncrementalBatchStrategy, IncrementalStrategy

pytestmark = pytest.mark.postgres


def test_postgres_strategy_incremental_batch_outside_loop(
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

    error_msg = "Invalid IncrementalBatchStrategy usage!"
    with pytest.raises(RuntimeError, match=re.escape(error_msg)):
        with IncrementalBatchStrategy(step=1):
            reader.run()


def test_postgres_strategy_incremental_batch_where(spark, processing, prepare_schema_table):
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
        where="float_value < 51 OR float_value BETWEEN 101 AND 120",
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
    )

    # there are 2 spans
    first_span_begin = 0
    first_span_end = 100
    first_span = processing.create_pandas_df(min_id=first_span_begin, max_id=first_span_end)

    second_span_begin = 101
    second_span_end = 200
    second_span = processing.create_pandas_df(min_id=second_span_begin, max_id=second_span_end)

    # insert first span
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

    # read only rows 0..50 (according to where)
    processing.assert_equal_df(df=first_df, other_frame=first_span[:51], order_by="id_int")

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

    # read only rows 101..119 (according to where)
    processing.assert_equal_df(df=second_df, other_frame=second_span[:19], order_by="id_int")


def test_postgres_strategy_incremental_batch_hwm_set_twice(
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

    step = 1

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

    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            reader1.run()

            with pytest.raises(
                ValueError,
                match="Detected wrong IncrementalBatchStrategy usage.",
            ):
                reader2.run()

            with pytest.raises(
                ValueError,
                match="Detected wrong IncrementalBatchStrategy usage.",
            ):
                reader3.run()


@pytest.mark.parametrize(
    "hwm_column, new_type, step",
    [
        ("hwm_int", "date", 200),
        ("hwm_date", "integer", timedelta(days=20)),
        ("hwm_datetime", "integer", timedelta(weeks=2)),
    ],
)
def test_postgres_strategy_incremental_batch_different_hwm_type_in_store(
    spark,
    processing,
    load_table_data,
    hwm_column,
    new_type,
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
        source=load_table_data.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            reader.run()

    # change table schema
    new_fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    new_fields[hwm_column] = new_type

    processing.drop_table(schema=load_table_data.schema, table=load_table_data.table)
    processing.create_table(schema=load_table_data.schema, table=load_table_data.table, fields=new_fields)

    with pytest.raises(TypeError, match="Cannot cast HWM of type .* as .*"):
        with IncrementalBatchStrategy(step=step) as batches:
            for _ in batches:
                reader.run()


def test_postgres_strategy_incremental_batch_different_hwm_source_in_store(
    spark,
    processing,
    load_table_data,
):
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    old_hwm = ColumnIntHWM(name=hwm_name, source=load_table_data.full_name, expression="hwm_int", description="abc")
    # change HWM entity in HWM store
    fake_hwm = old_hwm.copy(update={"entity": rand_str()})
    hwm_store.set_hwm(fake_hwm)

    reader = DBReader(
        connection=postgres,
        source=load_table_data.full_name,
        hwm=old_hwm,
    )
    with pytest.raises(ValueError, match="Detected HWM with different `entity` attribute"):
        with IncrementalBatchStrategy(step=50) as batches:
            for _ in batches:
                reader.run()


@pytest.mark.parametrize("attribute", ["expression", "description"])
def test_postgres_strategy_incremental_batch_different_hwm_optional_attribute_in_store(
    spark,
    processing,
    load_table_data,
    attribute,
):
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    old_hwm = ColumnIntHWM(name=hwm_name, source=load_table_data.full_name, expression="hwm_int", description="abc")

    # change attribute value in HWM store
    fake_hwm = old_hwm.copy(update={attribute: rand_str()})
    hwm_store.set_hwm(fake_hwm)

    reader = DBReader(
        connection=postgres,
        source=load_table_data.full_name,
        hwm=old_hwm,
    )
    with pytest.warns(UserWarning, match=f"Detected HWM with different `{attribute}` attribute"):
        with IncrementalBatchStrategy(step=50) as batches:
            for _ in batches:
                reader.run()

    # attributes from DBReader have higher priority, except value
    new_hwm = hwm_store.get_hwm(name=hwm_name)
    assert new_hwm.dict(exclude={"value", "modified_time"}) == old_hwm.dict(exclude={"value", "modified_time"})


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", 1.5),
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
def test_postgres_strategy_incremental_batch_wrong_step_type(
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    # there are 2 spans with a gap between

    span_length = 100
    span_gap = 50

    # 0..40
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 50..90
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

    # set hwm value to first span max value, e.g. 100
    with IncrementalStrategy():
        reader.run()

    # data is added
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    with pytest.raises((TypeError, ValueError)):
        with IncrementalBatchStrategy(step=step) as part:
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
def test_postgres_strategy_incremental_batch_step_negative(
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    # there are 2 spans with a gap between

    span_length = 100
    span_gap = 50

    # 0..40
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 50..90
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

    # set hwm value to first span max value, e.g. 100
    with IncrementalStrategy():
        reader.run()

    # data is added
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    error_msg = "HWM value is not increasing, please check options passed to IncrementalBatchStrategy"
    with pytest.raises(ValueError, match=error_msg):
        with IncrementalBatchStrategy(step=step) as part:
            for _ in part:
                reader.run()


@pytest.mark.parametrize(
    "hwm_column, step",
    [
        ("hwm_int", 0.01),
        ("hwm_date", timedelta(days=1)),
        ("hwm_datetime", timedelta(hours=1)),
    ],
)
def test_postgres_strategy_incremental_batch_step_too_small(
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

    # there are 2 spans with a gap between

    span_length = 100
    span_gap = 50

    # 0..40
    first_span_begin = 0
    first_span_end = first_span_begin + span_length

    # 50..90
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

    # set hwm value to first span max value, e.g. 100
    with IncrementalStrategy():
        reader.run()

    # data is added
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    error_msg = f"step={step!r} parameter of IncrementalBatchStrategy leads to generating too many iterations"
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        with IncrementalBatchStrategy(step=step) as batches:
            for _ in batches:
                reader.run()


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "hwm_type, hwm_column, step, per_iter",
    [
        (ColumnIntHWM, "hwm_int", 20, 30),  # step <  per_iter
        (ColumnIntHWM, "hwm_int", 30, 30),  # step == per_iter
        (ColumnDateHWM, "hwm_date", timedelta(days=20), 20),  # per_iter value is calculated to cover the step value
        (ColumnDateTimeHWM, "hwm_datetime", timedelta(weeks=2), 20),  # same
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
def test_postgres_strategy_incremental_batch(
    spark,
    processing,
    prepare_schema_table,
    hwm_type,
    hwm_column,
    step,
    per_iter,
    span_gap,
    span_length,
):
    store = HWMStoreStackManager.get_current()

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_name = secrets.token_hex(5)
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
    )

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
    assert store.get_hwm(hwm_name) is None

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
        processing.assert_equal_df(df=first_df, other_frame=first_span, order_by="id_int")
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=first_df, other_frame=first_span)

    # hwm is set
    hwm = store.get_hwm(hwm_name)
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
            hwm = store.get_hwm(hwm_name)
            assert hwm is not None
            assert isinstance(hwm, hwm_type)
            assert first_span_max <= hwm.value <= second_span_max

            next_df = reader.run()
            assert next_df.count() <= per_iter

            if second_df is None:
                second_df = next_df
            else:
                second_df = second_df.union(next_df)

            hwm = store.get_hwm(hwm_name)
            assert hwm is not None
            assert isinstance(hwm, hwm_type)
            assert first_span_max <= hwm.value <= second_span_max

    hwm = store.get_hwm(hwm_name)
    assert hwm is not None
    assert isinstance(hwm, hwm_type)
    assert hwm.value == second_span_max

    if "int" in hwm_column:
        # only changed data has been read
        processing.assert_equal_df(df=second_df, other_frame=second_span, order_by="id_int")
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
        ("hwm_int", 1, 1),  # step == stop
        ("hwm_date", timedelta(days=10), date.today() + timedelta(days=40)),
        ("hwm_datetime", timedelta(hours=100), datetime.now() + timedelta(days=10)),
    ],
)
@pytest.mark.parametrize("span_length", [100, 40, 5, 1])
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

    # incremental run until row with hwm_column == stop will met
    total_df = None
    with IncrementalBatchStrategy(step=step, stop=stop) as batches:
        for _ in batches:
            next_df = reader.run()

            if total_df is None:
                total_df = next_df
            else:
                total_df = total_df.union(next_df)

    total_pandas_df = processing.fix_pandas_df(to_pandas(total_df))

    # only a small part of input data has been read
    # so instead of checking the whole dataframe a partial comparison should be performed
    processing.assert_subset_df(df=total_pandas_df, other_frame=span)

    # check that stop clause working as expected
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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        # the error is raised if hwm_expr is set, and hwm_column in the columns list
        # but if columns list is not passed, this is not an error
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

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

    # set hwm value to second span max value minus offset, e.g. 90
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

    total_span = pandas.concat([first_span, second_span], ignore_index=True)

    if full:
        # all the data has been read
        processing.assert_equal_df(df=total_df, other_frame=total_span, order_by="id_int")
    else:
        # date and datetime values have a random part
        # so instead of checking the whole dataframe a partial comparison should be performed
        processing.assert_subset_df(df=total_df, other_frame=total_span)


def test_postgres_strategy_incremental_batch_nothing_to_read(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)
    hwm_column = "hwm_int"
    step = 10

    reader = DBReader(
        connection=postgres,
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
    df = None
    counter = 0
    with IncrementalBatchStrategy(step=step) as batches:
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
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value is None

    assert not reader.has_data()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    assert reader.has_data()

    # .run() is not called - dataframe still empty - HWM not updated
    assert not df.count()
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value is None

    df = None
    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()

            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == first_span_max

    # no new data yet, nothing to read
    df = None
    counter = 0
    with IncrementalBatchStrategy(step=step) as batches:
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
    df = None
    with IncrementalBatchStrategy(step=step) as batches:
        for _ in batches:
            next_df = reader.run()
            counter += 1

            if df is None:
                df = next_df
            else:
                df = df.union(next_df)

    processing.assert_equal_df(df=df, other_frame=second_span, order_by="id_int")
    hwm = store.get_hwm(name=hwm_name)
    assert hwm.value == second_span_max
