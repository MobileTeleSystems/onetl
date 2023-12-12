import secrets
from contextlib import suppress
from datetime import timedelta

import pytest
from etl_entities.hwm import ColumnIntHWM
from etl_entities.hwm_store import HWMStoreStackManager

from tests.util.rand import rand_str

try:
    import pandas
except ImportError:
    # pandas can be missing if someone runs tests for file connections only
    pass

from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize(
    "hwm_column, new_type",
    [
        ("hwm_int", "date"),
        ("hwm_date", "integer"),
        ("hwm_datetime", "integer"),
    ],
)
def test_postgres_strategy_incremental_different_hwm_type_in_store(
    spark,
    processing,
    load_table_data,
    hwm_column,
    new_type,
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

    with IncrementalStrategy():
        reader.run()

    # change table schema
    new_fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    new_fields[hwm_column] = new_type

    processing.drop_table(schema=load_table_data.schema, table=load_table_data.table)
    processing.create_table(schema=load_table_data.schema, table=load_table_data.table, fields=new_fields)

    with pytest.raises(TypeError, match="Cannot cast HWM of type .* as .*"):
        with IncrementalStrategy():
            reader.run()


def test_postgres_strategy_incremental_different_hwm_source_in_store(
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
        with IncrementalStrategy():
            reader.run()


@pytest.mark.parametrize("attribute", ["expression", "description"])
def test_postgres_strategy_incremental_different_hwm_optional_attribute_in_store(
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
        with IncrementalStrategy():
            reader.run()

    # attributes from DBReader have higher priority, except value
    new_hwm = hwm_store.get_hwm(name=hwm_name)
    assert new_hwm.dict(exclude={"value", "modified_time"}) == old_hwm.dict(exclude={"value", "modified_time"})


def test_postgres_strategy_incremental_hwm_set_twice(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

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

    with IncrementalStrategy():
        reader1.run()

        with pytest.raises(
            ValueError,
            match="Detected wrong IncrementalStrategy usage.",
        ):
            reader2.run()

        with pytest.raises(
            ValueError,
            match="Detected wrong IncrementalStrategy usage.",
        ):
            reader3.run()


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
        source=prepare_schema_table.full_name,
        where="id_int < 1000 OR id_int = 1000",
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression="hwm_int"),
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
    processing.assert_equal_df(df=first_df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )

    with IncrementalStrategy():
        second_df = reader.run()

    # only changed data has been read
    processing.assert_equal_df(df=second_df, other_frame=second_span, order_by="id_int")


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
    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
        hwm=DBReader.AutoDetectHWM(name=secrets.token_hex(5), expression=hwm_column),
    )

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

    total_span = pandas.concat([second_span, first_span], ignore_index=True)
    processing.assert_equal_df(df=next_df, other_frame=total_span, order_by="id_int")


def test_postgres_strategy_incremental_handle_exception(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_name = secrets.token_hex(5)

    hwm_column = "hwm_int"
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
        reader = DBReader(
            connection=postgres,
            source=prepare_schema_table.full_name,
            hwm=DBReader.AutoDetectHWM(name=hwm_name, expression=hwm_column),
        )
        second_df = reader.run()

    # all the data from the second span has been read
    # like there was no exception
    processing.assert_equal_df(df=second_df, other_frame=second_span, order_by="id_int")
