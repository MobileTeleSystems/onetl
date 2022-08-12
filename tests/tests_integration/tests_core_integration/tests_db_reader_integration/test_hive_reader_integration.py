import pytest

from onetl.connection import Hive
from onetl.core import DBReader


def test_hive_reader(spark, processing, load_table_data):
    hive = Hive(spark=spark)

    reader = DBReader(
        connection=hive,
        table=load_table_data.full_name,
    )
    df = reader.run()

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
    )


def test_hive_reader_snapshot_with_columns(spark, processing, load_table_data):
    hive = Hive(spark=spark)

    reader1 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
    )
    table_df = reader1.run()

    reader2 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
        columns=["count(*)"],
    )
    count_df = reader2.run()

    assert count_df.collect()[0][0] == table_df.count()


def test_hive_reader_snapshot_with_where(spark, processing, load_table_data):
    hive = Hive(spark=spark)

    reader1 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
        where="id_int < 1000",
    )
    table_df = reader1.run()

    assert table_df.count() == 100
    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=table_df,
    )

    reader2 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
        where="id_int = 50",
    )
    one_df = reader2.run()
    assert one_df.count() == 1

    reader3 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
        where="id_int > 1000",
    )
    empty_df = reader3.run()

    assert not empty_df.count()


def test_hive_reader_snapshot_with_columns_and_where(spark, processing, load_table_data):
    hive = Hive(spark=spark)

    reader1 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
        where="id_int < 80 AND id_int > 10",
    )
    table_df = reader1.run()

    reader2 = DBReader(
        connection=hive,
        table=load_table_data.full_name,
        columns=["count(*)"],
        where="id_int < 80 AND id_int > 10",
    )
    count_df = reader2.run()

    assert count_df.collect()[0][0] == table_df.count()


def test_hive_reader_non_existing_table(spark, get_schema_table):
    from pyspark.sql.utils import AnalysisException

    hive = Hive(spark=spark)
    reader = DBReader(
        connection=hive,
        table=get_schema_table.full_name,
    )

    with pytest.raises(AnalysisException) as excinfo:
        reader.run()
        assert "does not exists" in str(excinfo.value)
