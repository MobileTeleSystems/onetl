import pytest

from onetl.connection import Oracle
from onetl.db import DBReader

pytestmark = pytest.mark.oracle


def test_oracle_reader_snapshot(spark, processing, load_table_data):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
        sid=processing.sid,
        service_name=processing.service_name,
    )

    reader = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
    )
    df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
    )


@pytest.mark.parametrize(
    "mode, column",
    [
        ("range", "id_int"),
        ("hash", "text_string"),
        ("mod", "id_int"),
    ],
)
def test_oracle_reader_snapshot_partitioning_mode(mode, column, spark, processing, load_table_data):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
        sid=processing.sid,
        service_name=processing.service_name,
    )

    reader = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        options=Oracle.ReadOptions(
            partitioning_mode=mode,
            partition_column=column,
            num_partitions=5,
        ),
    )

    table_df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
        order_by="id_int",
    )

    assert table_df.rdd.getNumPartitions() == 5


def test_oracle_reader_snapshot_with_columns(spark, processing, load_table_data):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
        sid=processing.sid,
        service_name=processing.service_name,
    )

    reader1 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
    )
    table_df = reader1.run()

    columns = [
        "TEXT_STRING",
        "HWM_INT",
        "FLOAT_VALUE",
        "ID_INT",
        "HWM_DATE",
        "HWM_DATETIME",
    ]

    reader2 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        columns=columns,
    )
    table_df_with_columns = reader2.run()

    # columns order is same as expected
    assert table_df.columns != table_df_with_columns.columns
    assert table_df_with_columns.columns == columns
    # dataframe content is unchanged
    processing.assert_equal_df(table_df_with_columns, other_frame=table_df)

    reader3 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        columns=["COUNT(*) ABC"],
    )
    count_df = reader3.run()

    # expressions are allowed
    assert count_df.columns == ["ABC"]
    assert count_df.collect()[0][0] == table_df.count()


def test_oracle_reader_snapshot_with_where(spark, processing, load_table_data):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
        sid=processing.sid,
        service_name=processing.service_name,
    )

    reader = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
    )
    table_df = reader.run()

    reader1 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        where="id_int < 1000",
    )
    table_df1 = reader1.run()
    assert table_df1.count() == table_df.count()

    reader2 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        where="id_int < 1000 OR id_int = 1000",
    )
    table_df2 = reader2.run()
    assert table_df2.count() == table_df.count()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df1,
    )

    reader3 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        where="id_int = 50",
    )
    one_df = reader3.run()

    assert one_df.count() == 1

    reader4 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        where="id_int > 1000",
    )
    empty_df = reader4.run()

    assert not empty_df.count()


def test_oracle_reader_snapshot_with_columns_and_where(spark, processing, load_table_data):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
        sid=processing.sid,
        service_name=processing.service_name,
    )

    reader1 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        where="id_int < 80 AND id_int > 10",
    )
    table_df = reader1.run()

    reader2 = DBReader(
        connection=oracle,
        source=load_table_data.full_name,
        columns=["count(*)"],
        where="id_int < 80 AND id_int > 10",
    )
    count_df = reader2.run()

    assert count_df.collect()[0][0] == table_df.count()
