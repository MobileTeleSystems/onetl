from string import ascii_letters

import pytest

try:
    import pandas
except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

from onetl.connection import Clickhouse
from onetl.db import DBReader
from tests.util.rand import rand_str

pytestmark = pytest.mark.clickhouse


def test_clickhouse_reader_snapshot(spark, processing, load_table_data):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
    )
    df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
        order_by="id_int",
    )


def test_clickhouse_reader_snapshot_with_partitioning_mode_range_int(spark, processing, load_table_data):
    from pyspark.sql.functions import spark_partition_id

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "range",
            "partitionColumn": "hwm_int",
            "numPartitions": 3,
        },
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 3
    # So just check that any partition has at least 0 rows
    assert table_df.groupBy(spark_partition_id()).count().count() == 3

    # 100 rows per 3 partitions -> each partition should contain about ~33 rows with very low variance.
    average_count_per_partition = table_df.count() // table_df.rdd.getNumPartitions()
    min_count_per_partition = average_count_per_partition - 1
    max_count_per_partition = average_count_per_partition + 1

    count_per_partition = table_df.groupBy(spark_partition_id()).count().collect()
    for partition in count_per_partition:
        assert min_count_per_partition <= partition["count"] <= max_count_per_partition


@pytest.mark.parametrize(
    "bounds",
    [
        pytest.param({"lowerBound": "50"}, id="lower_bound"),
        pytest.param({"upperBound": "70"}, id="upper_bound"),
        pytest.param({"lowerBound": "50", "upperBound": "70"}, id="both_bounds"),
    ],
)
def test_clickhouse_reader_snapshot_with_partitioning_mode_range_int_explicit_bounds(
    spark,
    processing,
    load_table_data,
    bounds,
):
    from pyspark.sql.functions import spark_partition_id

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "range",
            "partitionColumn": "hwm_int",
            "numPartitions": 3,
            **bounds,
        },
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 3
    # So just check that any partition has at least 0 rows
    assert table_df.groupBy(spark_partition_id()).count().count() == 3


@pytest.mark.parametrize(
    "column",
    [
        "hwm_date",
        "hwm_datetime",
    ],
)
def test_clickhouse_reader_snapshot_with_partitioning_mode_range_date_datetime(
    spark,
    processing,
    load_table_data,
    column,
):
    from pyspark.sql.functions import spark_partition_id

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "range",
            "partitionColumn": column,
            "numPartitions": 3,
        },
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 3
    # So just check that any partition has at least 0 rows
    assert table_df.groupBy(spark_partition_id()).count().count() == 3


@pytest.mark.parametrize(
    "column",
    [
        "float_value",
        "text_string",
    ],
)
def test_clickhouse_reader_snapshot_with_partitioning_mode_range_unsupported_column_type(
    spark,
    processing,
    load_table_data,
    column,
):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "range",
            "partitionColumn": column,
            "numPartitions": 3,
        },
    )

    with pytest.raises(Exception):
        reader.run()


@pytest.mark.parametrize(
    "column",
    [
        # all column types are supported
        "hwm_int",
        "hwm_date",
        "hwm_datetime",
        "float_value",
        "text_string",
    ],
)
def test_clickhouse_reader_snapshot_with_partitioning_mode_hash(spark, processing, load_table_data, column):
    from pyspark.sql.functions import spark_partition_id

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "hash",
            "partitionColumn": column,
            "numPartitions": 3,
        },
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 3
    # So just check that any partition has at least 0 rows
    assert table_df.groupBy(spark_partition_id()).count().count() == 3


@pytest.mark.parametrize(
    "column",
    [
        "hwm_int",
        "float_value",
    ],
)
def test_clickhouse_reader_snapshot_with_partitioning_mode_mod_number(spark, processing, load_table_data, column):
    from pyspark.sql.functions import spark_partition_id

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "mod",
            "partitionColumn": column,
            "numPartitions": 3,
        },
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 3
    # So just check that any partition has at least 0 rows
    assert table_df.groupBy(spark_partition_id()).count().count() == 3

    # 100 rows per 3 partitions -> each partition should contain about ~33 rows with very low variance.
    average_count_per_partition = table_df.count() // table_df.rdd.getNumPartitions()
    min_count_per_partition = average_count_per_partition - 1
    max_count_per_partition = average_count_per_partition + 1

    count_per_partition = table_df.groupBy(spark_partition_id()).count().collect()
    for partition in count_per_partition:
        assert min_count_per_partition <= partition["count"] <= max_count_per_partition


# Apparently, Clickhouse supports `date % number` operation
def test_clickhouse_reader_snapshot_with_partitioning_mode_mod_date(spark, processing, load_table_data):
    from pyspark.sql.functions import spark_partition_id

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "mod",
            "partitionColumn": "hwm_date",
            "numPartitions": 3,
        },
    )
    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 3
    # So just check that any partition has at least 0 rows
    assert table_df.groupBy(spark_partition_id()).count().count() == 3


@pytest.mark.parametrize(
    "column",
    [
        "hwm_datetime",
        "text_string",
    ],
)
def test_clickhouse_reader_snapshot_with_partitioning_mode_mod_unsupported_column_type(
    spark,
    processing,
    load_table_data,
    column,
):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        options={
            "partitioning_mode": "mod",
            "partitionColumn": column,
            "numPartitions": 3,
        },
    )

    with pytest.raises(Exception, match=r"Illegal types .* of arguments of function modulo"):
        reader.run()


def test_clickhouse_reader_snapshot_without_set_database(spark, processing, load_table_data):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
    )
    df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
        order_by="id_int",
    )


def test_clickhouse_reader_snapshot_with_columns(spark, processing, load_table_data):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader1 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
    )
    table_df = reader1.run()

    columns = [
        "text_string",
        "hwm_int",
        "float_value",
        "id_int",
        "hwm_date",
        "hwm_datetime",
    ]

    reader2 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        columns=columns,
    )
    table_df_with_columns = reader2.run()

    # columns order is same as expected
    assert table_df.columns != table_df_with_columns.columns
    assert table_df_with_columns.columns == columns
    # dataframe content is unchanged
    processing.assert_equal_df(
        table_df_with_columns,
        other_frame=table_df,
        order_by="id_int",
    )

    reader3 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        columns=["count(*) as abc"],
    )
    count_df = reader3.run()

    # expressions are allowed
    assert count_df.columns == ["abc"]
    assert count_df.collect()[0][0] == table_df.count()


@pytest.mark.xfail(reason="Clickhouse <24 deduplicated column names, but 24+ does not")
def test_clickhouse_reader_snapshot_with_columns_duplicated(spark, processing, prepare_schema_table):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader1 = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
    )
    df1 = reader1.run()

    reader2 = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
        columns=[
            "*",
            "id_int",
        ],
    )

    with pytest.raises(Exception, match="The column `id_int` already exists"):
        df2 = reader2.run()
        assert df1.columns == df2.columns


def test_clickhouse_reader_snapshot_with_columns_mixed_naming(spark, processing, get_schema_table):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    # create table with mixed column names, e.g. IdInt
    full_name, schema, table = get_schema_table
    column_names = []
    table_fields = {}
    for original_name in processing.column_names:
        column_type = processing.get_column_type(original_name)
        new_name = rand_str(alphabet=ascii_letters + " _").strip()
        # wrap column names in DDL with quotes to preserve case
        table_fields[f'"{new_name}"'] = column_type
        column_names.append(new_name)

    processing.create_table(schema=schema, table=table, fields=table_fields)

    # before 0.10 this caused errors because * in column names was replaced with real column names,
    # but they were not escaped
    reader = DBReader(
        connection=clickhouse,
        source=full_name,
        columns=["*"],
    )

    df = reader.run()
    assert df.columns == column_names


def test_clickhouse_reader_snapshot_with_where(spark, processing, load_table_data):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
    )
    table_df = reader.run()

    reader1 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        where="id_int < 1000",
    )
    table_df1 = reader1.run()

    assert table_df1.count() == table_df.count()

    reader2 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        where="id_int < 1000 OR id_int = 1000",
    )
    table_df2 = reader2.run()

    assert table_df2.count() == table_df.count()
    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df1,
        order_by="id_int",
    )

    reader3 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        where="id_int = 50",
    )
    one_df = reader3.run()

    assert one_df.count() == 1

    reader4 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        where="id_int > 1000",
    )
    empty_df = reader4.run()

    assert not empty_df.count()


def test_clickhouse_reader_snapshot_with_columns_and_where(spark, processing, load_table_data):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader1 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        where="id_int < 80 AND id_int > 10",
    )
    table_df = reader1.run()

    reader2 = DBReader(
        connection=clickhouse,
        source=load_table_data.full_name,
        columns=["count(*)"],
        where="id_int < 80 AND id_int > 10",
    )
    count_df = reader2.run()

    assert count_df.collect()[0][0] == table_df.count()


def test_clickhouse_reader_snapshot_nothing_to_read(spark, processing, prepare_schema_table):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=clickhouse,
        source=prepare_schema_table.full_name,
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

    with pytest.raises(Exception, match="No data in the source:"):
        reader.raise_if_no_data()

    assert not reader.has_data()

    # no data yet, nothing to read
    df = reader.run()
    assert not df.count()

    # insert first span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=first_span,
    )

    # .run() is not called, but dataframes are lazy, so it now contains all data from the source
    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")

    # check that read df has data
    assert reader.has_data()

    # read data explicitly
    df = reader.run()

    processing.assert_equal_df(df=df, other_frame=first_span, order_by="id_int")

    # insert second span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=second_span,
    )
    total_span = pandas.concat([first_span, second_span], ignore_index=True)

    # .run() is not called, but dataframes are lazy, so it now contains all data from the source
    processing.assert_equal_df(df=df, other_frame=total_span, order_by="id_int")

    # read data explicitly
    df = reader.run()
    processing.assert_equal_df(df=df, other_frame=total_span, order_by="id_int")
