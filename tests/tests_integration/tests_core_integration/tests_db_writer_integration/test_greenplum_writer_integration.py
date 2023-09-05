import pytest

from onetl.connection import Greenplum
from onetl.db import DBWriter

pytestmark = pytest.mark.greenplum


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"if_exists": "append"},
        {"if_exists": "replace_entire_table"},
        {"if_exists": "error"},
        {"if_exists": "ignore"},
    ],
)
def test_greenplum_writer_snapshot(spark, processing, get_schema_table, options):
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
        target=get_schema_table.full_name,
        options=Greenplum.WriteOptions(**options),
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )


def test_greenplum_writer_if_exists_append(spark, processing, prepare_schema_table):
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
        target=prepare_schema_table.full_name,
        options=Greenplum.WriteOptions(if_exists="append"),
    )

    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
        order_by="id_int",
    )


def test_greenplum_writer_if_exists_overwrite(spark, processing, prepare_schema_table):
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
        target=prepare_schema_table.full_name,
        options=Greenplum.WriteOptions(if_exists="replace_entire_table"),
    )

    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df2,
        order_by="id_int",
    )


def test_greenplum_writer_if_exists_error(spark, processing, prepare_schema_table):
    from py4j.java_gateway import Py4JJavaError

    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)

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
        target=prepare_schema_table.full_name,
        options=Greenplum.WriteOptions(if_exists="error"),
    )

    with pytest.raises(
        Py4JJavaError,
        match=f'Table "{prepare_schema_table.schema}"."{prepare_schema_table.table}"'
        f" exists, and SaveMode.ErrorIfExists was specified",
    ):
        writer.run(df)


def test_greenplum_writer_if_exists_ignore(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)

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
        target=prepare_schema_table.full_name,
        options=Greenplum.WriteOptions(if_exists="ignore"),
    )

    writer.run(df)  # The write operation is ignored

    empty_df = spark.createDataFrame([], df.schema)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=empty_df,
    )
