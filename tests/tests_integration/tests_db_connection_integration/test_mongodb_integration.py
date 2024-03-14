import logging

import pytest

from onetl.connection import MongoDB

pytestmark = pytest.mark.mongodb


def test_mongodb_connection_check(spark, processing, caplog):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert mongo.check() == mongo

    assert "|MongoDB|" in caplog.text
    assert f"host = '{processing.host}'" in caplog.text
    assert f"port = {processing.port}" in caplog.text
    assert f"database = '{processing.database}'" in caplog.text
    assert f"user = '{processing.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert processing.password not in caplog.text

    assert "package = " not in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_mongodb_connection_check_fail(processing, spark):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user="unknown",
        password="unknown",
        database=processing.database,
        spark=spark,
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        mongo.check()


def test_mongodb_connection_read_pipeline(
    spark,
    prepare_schema_table,
    processing,
):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    write_df = processing.create_pandas_df()

    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=write_df,
    )

    df = mongo.pipeline(
        collection=prepare_schema_table.table,
    )

    processing.assert_equal_df(
        df=df,
        other_frame=write_df,
    )


def test_mongodb_connection_read_pipeline_match(
    spark,
    prepare_schema_table,
    processing,
):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    write_df = processing.create_pandas_df()

    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=write_df,
    )

    df = mongo.pipeline(
        collection=prepare_schema_table.table,
        pipeline={"$match": {"_id": {"$eq": 1}}},
    )

    processing.assert_equal_df(
        df=df,
        other_frame=write_df[write_df._id == 1],
    )


def test_mongodb_connection_read_pipeline_match_with_df_schema(
    spark,
    prepare_schema_table,
    processing,
):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    write_df = processing.create_pandas_df()

    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=write_df,
    )

    df = mongo.pipeline(
        collection=prepare_schema_table.table,
        pipeline={"$match": {"_id": {"$eq": 1}}},
        df_schema=prepare_schema_table.schema,
    )

    processing.assert_equal_df(
        df=df,
        other_frame=write_df[write_df._id == 1],
    )


def test_mongodb_connection_read_pipeline_group(spark, prepare_schema_table, processing):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    write_df = processing.create_pandas_df(min_id=1, max_id=100)

    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=write_df,
    )

    df = mongo.pipeline(
        collection=prepare_schema_table.table,
        pipeline={"$group": {"_id": 1, "min": {"$min": "$hwm_int"}, "max": {"$max": "$hwm_int"}}},
        df_schema=prepare_schema_table.schema,
    )

    assert df
    assert df.count() == 1

    collected = df.collect()

    assert collected[0]["min"] == 1
    assert collected[0]["max"] == 100
