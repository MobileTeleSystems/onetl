import pytest

from onetl.core import DBWriter
from onetl.connection import Postgres


def test_postgres_writer_snapshot(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=postgres,
        table=prepare_schema_table.full_name,
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


def test_postgres_writer_snapshot_with_dict_options(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=postgres,
        table=prepare_schema_table.full_name,
        options={"batchsize": "500"},
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


def test_postgres_writer_snapshot_with_pydantic_options(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=postgres,
        table=prepare_schema_table.full_name,
        options=Postgres.Options(batchsize=500),
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_postgres_writer_mode(spark, processing, prepare_schema_table, mode):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
    df1 = df[df.id_int < 1001]
    df2 = df[df.id_int > 1000]

    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=postgres,
        table=prepare_schema_table.full_name,
        options=Postgres.Options(mode=mode),
    )

    writer.run(df1)
    writer.run(df2)

    if mode == "append":
        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df,
        )

    if mode == "overwrite":
        processing.assert_equal_df(
            schema=prepare_schema_table.schema,
            table=prepare_schema_table.table,
            df=df2,
        )
