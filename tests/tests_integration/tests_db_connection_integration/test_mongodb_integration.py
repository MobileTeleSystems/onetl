from onetl.connection import MongoDB


def test_mongodb_connection_read(spark, processing, load_table_data):
    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    df = mongo.read_table(table=load_table_data.table)

    processing.assert_equal_df(
        schema=load_table_data.schema,
        table=load_table_data.table,
        df=df,
    )


def test_mongodb_connection_write(spark, prepare_schema_table, processing):
    df = processing.create_spark_df(spark=spark)

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    mongo.save_df(df, table=prepare_schema_table.table)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )
