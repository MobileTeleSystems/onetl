import pytest

from onetl.connection import Greenplum
from onetl.core import DBWriter

pytestmark = pytest.mark.greenplum


def test_greenplum_writer_snapshot(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    writer = DBWriter(
        connection=greenplum,
        table=prepare_schema_table.full_name,
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
        order_by="id_int",
    )


def test_greenplum_writer_mode_append(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
    df1 = df[df.id_int < 1001]
    df2 = df[df.id_int > 1000]

    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    writer = DBWriter(
        connection=greenplum,
        table=prepare_schema_table.full_name,
        options=Greenplum.WriteOptions(mode="append"),
    )

    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
        order_by="id_int",
    )


def test_greenplum_writer_mode(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
    df1 = df[df.id_int < 1001]
    df2 = df[df.id_int > 1000]

    greenplum = Greenplum(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra=processing.extra,
    )

    writer = DBWriter(
        connection=greenplum,
        table=prepare_schema_table.full_name,
        options=Greenplum.WriteOptions(mode="overwrite"),
    )

    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df2,
        order_by="id_int",
    )
