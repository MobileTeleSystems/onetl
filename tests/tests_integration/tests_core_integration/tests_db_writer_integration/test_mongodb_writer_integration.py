import logging
import re

import pytest

from onetl.connection import MongoDB
from onetl.db import DBWriter

pytestmark = pytest.mark.mongodb


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"if_exists": "append"},
        {"if_exists": "replace_entire_collection"},
        {"if_exists": "error"},
        {"if_exists": "ignore"},
    ],
)
@pytest.mark.flaky(reruns=2)
def test_mongodb_writer_snapshot(spark, processing, get_schema_table, options, caplog):
    df = processing.create_spark_df(spark=spark)

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=mongo,
        table=get_schema_table.table,
        options=MongoDB.WriteOptions(**options),
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

        assert f"|MongoDB| Collection '{get_schema_table.table}' does not exist" in caplog.text

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
    )


def test_mongodb_writer_if_exists_append(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
    df1 = df[df._id < 1001]
    df2 = df[df._id > 1000]

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=mongo,
        table=get_schema_table.table,
        options=MongoDB.WriteOptions(if_exists="append"),
    )
    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
    )


def test_mongodb_writer_if_exists_replace_entire_collection(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
    df1 = df[df._id < 1001]
    df2 = df[df._id > 1000]

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=mongo,
        table=get_schema_table.table,
        options=MongoDB.WriteOptions(if_exists="replace_entire_collection"),
    )
    writer.run(df1)
    writer.run(df2)

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df2,
    )


def test_mongodb_writer_if_exists_error(spark, processing, get_schema_table, caplog):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=mongo,
        table=get_schema_table.table,
        options=MongoDB.WriteOptions(if_exists="error"),
    )
    writer.run(df)

    with pytest.raises(
        ValueError,
        match=re.escape("Operation stopped due to MongoDB.WriteOptions(if_exists=...) is set to 'error'."),
    ):
        writer.run(df)

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
    )


def test_mongodb_writer_if_exists_ignore(spark, processing, get_schema_table, caplog):
    df = processing.create_spark_df(spark=spark, min_id=1, max_id=1500)
    df1 = df[df._id < 1001]
    df2 = df[df._id > 1000]

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=mongo,
        table=get_schema_table.table,
        options=MongoDB.WriteOptions(if_exists="ignore"),
    )
    writer.run(df1)

    with caplog.at_level(logging.INFO):
        writer.run(df2)  # The write operation is ignored

        assert f"|MongoDB| Collection '{get_schema_table.table}' exists" in caplog.text
        assert (
            "|MongoDB| Skip writing to existing collection because of MongoDB.WriteOptions(if_exists='ignore')"
            in caplog.text
        )

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1,
    )
