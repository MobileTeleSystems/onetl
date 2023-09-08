import pytest

from onetl.connection import Postgres
from onetl.db import DBWriter

pytestmark = pytest.mark.postgres


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
def test_postgres_writer_snapshot(spark, processing, get_schema_table, options):
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
        target=get_schema_table.full_name,
        options=Postgres.WriteOptions(**options),
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
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
        target=prepare_schema_table.full_name,
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
        target=prepare_schema_table.full_name,
        options=Postgres.WriteOptions(batchsize=500),
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


def test_postgres_writer_if_exists_append(spark, processing, prepare_schema_table):
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
        target=prepare_schema_table.full_name,
        options=Postgres.WriteOptions(if_exists="append"),
    )

    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )


def test_postgres_writer_if_exists_error(spark, processing, prepare_schema_table):
    from pyspark.sql.utils import AnalysisException

    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)

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
        target=prepare_schema_table.full_name,
        options=Postgres.WriteOptions(if_exists="error"),
    )

    with pytest.raises(
        AnalysisException,
        match=f"Table or view '{prepare_schema_table.full_name}' already exists. SaveMode: ErrorIfExists.",
    ):
        writer.run(df)

    empty_df = spark.createDataFrame([], df.schema)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=empty_df,
    )


def test_postgres_writer_if_exists_ignore(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)

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
        target=prepare_schema_table.full_name,
        options=Postgres.WriteOptions(if_exists="ignore"),
    )

    writer.run(df)  # The write operation is ignored
    empty_df = spark.createDataFrame([], df.schema)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=empty_df,
    )


def test_postgres_writer_if_exists_replace_entire_table(spark, processing, prepare_schema_table):
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
        target=prepare_schema_table.full_name,
        options=Postgres.WriteOptions(if_exists="replace_entire_table"),
    )

    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df2,
    )
