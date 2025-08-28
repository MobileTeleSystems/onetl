import pytest

from onetl.connection import Hive
from onetl.db import DBReader

pytestmark = [pytest.mark.hdfs, pytest.mark.iceberg]


def test_hive_iceberg_reader_snapshot(spark_with_hdfs_iceberg, processing, iceberg_table):
    hive = Hive(cluster="rnd-dwh", spark=spark_with_hdfs_iceberg)
    catalog, schema, table = iceberg_table.split(".")

    reader = DBReader(
        connection=hive,
        source=iceberg_table,
    )
    df = reader.run()

    processing.assert_equal_df(
        schema=f"{catalog}.{schema}",
        table=table,
        df=df,
        order_by="id_int",
    )


def test_hive_iceberg_reader_snapshot_with_columns(spark_with_hdfs_iceberg, processing, iceberg_table):
    hive = Hive(cluster="rnd-dwh", spark=spark_with_hdfs_iceberg)

    reader1 = DBReader(
        connection=hive,
        source=iceberg_table,
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
        connection=hive,
        source=iceberg_table,
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
        connection=hive,
        source=iceberg_table,
        columns=["count(*) as abc"],
    )
    count_df = reader3.run()

    # expressions are allowed
    assert count_df.columns == ["abc"]
    assert count_df.collect()[0][0] == table_df.count()


def test_hive_iceberg_reader_with_where(spark_with_hdfs_iceberg, processing, iceberg_table):
    hive = Hive(cluster="rnd-dwh", spark=spark_with_hdfs_iceberg)
    catalog, schema, table = iceberg_table.split(".")

    reader = DBReader(
        connection=hive,
        source=iceberg_table,
    )
    table_df = reader.run()

    reader1 = DBReader(
        connection=hive,
        source=iceberg_table,
        where="id_int < 1000",
    )
    table_df1 = reader1.run()
    assert table_df1.count() == table_df.count()

    reader2 = DBReader(
        connection=hive,
        source=iceberg_table,
        where="id_int < 1000 OR id_int = 1000",
    )
    table_df2 = reader2.run()
    assert table_df2.count() == table_df.count()

    processing.assert_equal_df(
        schema=f"{catalog}.{schema}",
        table=table,
        df=table_df1,
        order_by="id_int",
    )

    reader3 = DBReader(
        connection=hive,
        source=iceberg_table,
        where="id_int = 50",
    )
    one_df = reader3.run()

    assert one_df.count() == 1
