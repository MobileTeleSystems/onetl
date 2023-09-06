import logging
import textwrap

import pytest

from onetl._util.spark import get_spark_version
from onetl.connection import Hive
from onetl.db import DBWriter

pytestmark = pytest.mark.hive


@pytest.mark.parametrize(
    "if_exists",
    [
        "append",
        "replace_overlapping_partitions",
        "replace_entire_table",
    ],
)
def test_hive_writer_target_does_not_exist(spark, processing, get_schema_table, if_exists, caplog):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,  # new table
        options=Hive.WriteOptions(if_exists=if_exists),
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

        assert f"|Hive| Saving data to a table '{get_schema_table.full_name}'" in caplog.text
        assert f"|Hive| Table '{get_schema_table.full_name}' is successfully created" in caplog.text

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
    )


@pytest.mark.parametrize(
    "options",
    [{"compression": "snappy"}, Hive.WriteOptions(compression="snappy")],
    ids=["options as dict", "options as model"],
)
def test_hive_writer_with_options(spark, processing, get_schema_table, options):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=options,
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    if get_spark_version(spark).major < 3:
        assert "`compression` 'snappy'" in response
    else:
        assert "'compression' = 'snappy'" in response


@pytest.mark.parametrize(
    "options, fmt",
    [
        (Hive.WriteOptions(format="orc"), "orc"),
        (Hive.WriteOptions(), "orc"),  # default
        (Hive.WriteOptions(format="parquet"), "parquet"),
    ],
)
def test_hive_writer_with_format(spark, processing, get_schema_table, options, fmt):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=options,
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    assert f"USING {fmt}" in response


@pytest.mark.parametrize(
    "bucket_number, bucket_columns",
    [
        (10, "id_int"),
        (5, ["id_int", "hwm_int"]),
    ],
    ids=["bucket columns as string.", "bucket columns as List."],
)
def test_hive_writer_with_bucket_by(
    spark,
    processing,
    get_schema_table,
    bucket_number,
    bucket_columns,
):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(bucketBy=(bucket_number, bucket_columns)),
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    if isinstance(bucket_columns, str):
        assert f"CLUSTERED BY ({bucket_columns})" in response
    else:
        assert f"CLUSTERED BY ({', '.join(bucket_columns)})" in response

    assert f"INTO {bucket_number} BUCKETS" in response


@pytest.mark.parametrize(
    "sort_by",
    ["id_int", ["id_int", "hwm_int"]],
    ids=["sortBy as string.", "sortBy as List."],
)
def test_hive_writer_with_bucket_by_and_sort_by(
    spark,
    processing,
    get_schema_table,
    sort_by,
):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(bucketBy=(10, "id_int"), sortBy=sort_by),
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    if isinstance(sort_by, str):
        assert f"SORTED BY ({sort_by})" in response
    else:
        assert f"SORTED BY ({', '.join(sort_by)})" in response

    assert "CLUSTERED BY (id_int)" in response
    assert "INTO 10 BUCKETS" in response


def test_hive_writer_default_not_bucketed(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    assert "SORTED BY" not in response
    assert "CLUSTERED BY" not in response
    assert "BUCKETS" not in response


@pytest.mark.parametrize(
    "partition_by",
    [
        "id_int",
        ["id_int", "hwm_int"],
    ],
    ids=["partitionBy as string.", "partitionBy as List."],
)
def test_hive_writer_with_partition_by(spark, processing, get_schema_table, partition_by):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(partitionBy=partition_by),
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    if isinstance(partition_by, str):
        assert f"PARTITIONED BY ({partition_by})" in response
    else:
        assert f"PARTITIONED BY ({', '.join(partition_by)})" in response


def test_hive_writer_default_not_partitioned(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )
    writer.run(df)

    response = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}")
    response = response.collect()[0][0]

    assert "PARTITIONED BY" not in response


@pytest.mark.parametrize(
    "options",
    [
        Hive.WriteOptions(if_exists="append"),
        Hive.WriteOptions(if_exists="replace_entire_table"),
        Hive.WriteOptions(if_exists="replace_overlapping_partitions"),
    ],
)
def test_hive_writer_create_table_if_exists(spark, processing, get_schema_table, options, caplog):
    df = processing.create_spark_df(spark=spark)

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=options,
    )

    with caplog.at_level(logging.INFO):
        writer.run(df)

    # table is created and content is the same as in the dataframe
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )


@pytest.mark.parametrize(
    "options, option_kv",
    [
        (Hive.WriteOptions(partitionBy="str"), "{'partitionBy': 'str'}"),
        (Hive.WriteOptions(bucketBy=(10, "id_int")), "{'bucketBy': (10, 'id_int')}"),
        (
            Hive.WriteOptions(bucketBy=(5, "id_int"), sortBy="hwm_int"),
            "{'bucketBy': (5, 'id_int'), 'sortBy': 'hwm_int'}",
        ),
        (Hive.WriteOptions(compression="snappy"), "{'compression': 'snappy'}"),
        (Hive.WriteOptions(format="parquet"), "{'format': 'parquet'}"),
    ],
)
def test_hive_writer_insert_into_with_options(spark, processing, get_schema_table, options, option_kv, caplog):
    df = processing.create_spark_df(spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    df1 = df[df.id_int <= 50]
    df2 = df[df.id_int > 50]

    writer1 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )
    # create & fill up the table without any options
    writer1.run(df1)
    old_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    writer2 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=options,
    )

    error_msg = f"|Hive| Options {option_kv} are not supported while inserting into existing table, ignoring"
    with caplog.at_level(logging.WARNING):
        # write to table with new options
        writer2.run(df2)
        assert error_msg in caplog.text

    new_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    # table DDL remains the same
    assert new_ddl == old_ddl
    assert "compression" not in new_ddl
    assert "USING parquet" not in new_ddl
    assert "PARTITIONED BY" not in new_ddl
    assert "SORTED BY" not in new_ddl
    assert "CLUSTERED BY" not in new_ddl
    assert "BUCKETS" not in new_ddl

    # table contains both old and new data
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )


@pytest.mark.parametrize(
    "original_options, new_options",
    [
        pytest.param({}, {"partitionBy": "id_int"}, id="table_not_partitioned_dataframe_is"),
        pytest.param({"partitionBy": "text_string"}, {}, id="table_partitioned_dataframe_is_not"),
        pytest.param({"partitionBy": "text_string"}, {"partitionBy": "id_int"}, id="different_partitioning_schema"),
        pytest.param({"partitionBy": "id_int"}, {"partitionBy": "id_int"}, id="same_partitioning_schema"),
    ],
)
def test_hive_writer_insert_into_append(spark, processing, get_schema_table, original_options, new_options, caplog):
    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 25]
    df2 = df.where("id_int > 25 AND id_int <= 50")
    df3 = df[df.id_int > 50]

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer1 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=original_options,
    )
    # create & fill up the table with some data
    writer1.run(df1.union(df2))
    old_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    writer2 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(if_exists="append", **new_options),
    )

    with caplog.at_level(logging.INFO):
        writer2.run(df1.union(df3))

        assert f"|Hive| Inserting data into existing table '{get_schema_table.full_name}' ..." in caplog.text
        assert f"|Hive| Data is successfully inserted into table '{get_schema_table.full_name}'." in caplog.text

    new_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    # table DDL remains the same
    assert new_ddl == old_ddl

    # table contains both old and new data
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1.union(df1).union(df2).union(df3),
        order_by="id_int",
    )


@pytest.mark.parametrize(
    "original_options, new_options",
    [
        pytest.param({}, {"partitionBy": "id_int"}, id="table_not_partitioned_dataframe_is"),
        pytest.param({"partitionBy": "text_string"}, {}, id="table_partitioned_dataframe_is_not"),
        pytest.param({"partitionBy": "text_string"}, {"partitionBy": "id_int"}, id="different_partitioning_schema"),
        pytest.param({"partitionBy": "id_int"}, {"partitionBy": "id_int"}, id="same_partitioning_schema"),
    ],
)
def test_hive_writer_insert_into_replace_entire_table(
    spark,
    processing,
    get_schema_table,
    original_options,
    new_options,
    caplog,
):
    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 50]
    df2 = df[df.id_int > 50]

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer1 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=original_options,
    )
    # create & fill up the table with some data
    writer1.run(df1)
    old_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    writer2 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(if_exists="replace_entire_table", **new_options),
    )
    with caplog.at_level(logging.INFO):
        # recreate table with different columns order to produce different DDL
        writer2.run(df2.select(*reversed(df2.columns)))

        # unlike other modes, this creates new table
        assert f"|Hive| Saving data to a table '{get_schema_table.full_name}' ..." in caplog.text
        assert f"|Hive| Table '{get_schema_table.full_name}' is successfully created." in caplog.text

    new_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    # database content is replaced by new data
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df2,
        order_by="id_int",
    )

    # table is recreated
    assert new_ddl != old_ddl


def test_hive_writer_insert_into_replace_overlapping_partitions_in_non_partitioned_table(
    spark,
    processing,
    get_schema_table,
    caplog,
):
    from pyspark.sql.functions import col

    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 50]
    df2 = df[df.id_int > 50]
    # to test that inserting dataframe into existing table is performed using column names, but not order
    df2_reversed = df2.select(*(col(column).alias(column.upper()) for column in reversed(df2.columns)))

    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer1 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )
    # create & fill up the table with some data
    writer1.run(df1)
    old_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    writer2 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(if_exists="replace_overlapping_partitions", partition_by="id_int"),
    )

    with caplog.at_level(logging.INFO):
        writer2.run(df2_reversed)

        assert f"|Hive| Inserting data into existing table '{get_schema_table.full_name}' ..." in caplog.text
        assert f"|Hive| Data is successfully inserted into table '{get_schema_table.full_name}'." in caplog.text

    new_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    # table DDL remains the same
    assert new_ddl == old_ddl

    # but database content is replaced by new data
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df2,
        order_by="id_int",
    )


@pytest.mark.parametrize(
    "partition_by",
    [
        pytest.param(None, id="table_partitioned_dataframe_is_not"),
        pytest.param("hwm_int", id="different_partitioning_schema"),
        pytest.param("id_int", id="same_partitioning_schema"),
    ],
)
def test_hive_writer_insert_into_replace_overlapping_partitions_in_partitioned_table(
    spark,
    processing,
    get_schema_table,
    partition_by,
):
    df = processing.create_spark_df(spark=spark)

    df1 = df[df.id_int <= 25]
    df2 = df.where("id_int > 25 AND id_int <= 50")
    df3 = df[df.id_int > 50]

    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer1 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(partition_by="id_int"),
    )

    # create & fill up the table with some data
    writer1.run(df1.union(df2))
    old_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    writer2 = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
        options=Hive.WriteOptions(if_exists="replace_overlapping_partitions", partition_by=partition_by),
    )

    writer2.run(df1.union(df3))
    new_ddl = hive.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]

    # table contains all the data from both existing (df2) and new partitions (df1, df3)
    # existing partition data is overwritten with data from dataframe (df1)
    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df1.union(df2).union(df3),
        order_by="id_int",
    )

    # table DDL remains the same
    assert new_ddl == old_ddl


@pytest.mark.parametrize("mode", ["append", "replace_overlapping_partitions"])
def test_hive_writer_insert_into_wrong_columns(spark, processing, prepare_schema_table, mode):
    df = processing.create_spark_df(spark=spark)
    hive = Hive(cluster="rnd-dwh", spark=spark)

    writer = DBWriter(
        connection=hive,
        target=prepare_schema_table.full_name,
        options=Hive.WriteOptions(if_exists=mode),
    )

    prefix = rf"""
        Inconsistent columns between a table and the dataframe!

        Table '{prepare_schema_table.full_name}' has columns:
            \['id_int', 'text_string', 'hwm_int', 'hwm_date', 'hwm_datetime', 'float_value'\]
        """.strip()

    # new column
    df2 = df.withColumn("unknown", df.id_int)
    error_msg2 = textwrap.dedent(
        rf"""
        {prefix}

        Dataframe has columns:
            \['id_int', 'text_string', 'hwm_int', 'hwm_date', 'hwm_datetime', 'float_value', 'unknown'\]

        These columns present only in dataframe:
            \['unknown'\]
        """,
    ).strip()
    with pytest.raises(ValueError, match=error_msg2):
        writer.run(df2)

    # too less columns
    df3 = df.select(df.id_int, df.hwm_int)
    error_msg3 = textwrap.dedent(
        rf"""
        {prefix}

        Dataframe has columns:
            \['id_int', 'hwm_int'\]

        These columns present only in table:
            \['text_string', 'hwm_date', 'hwm_datetime', 'float_value'\]
        """,
    ).strip()
    with pytest.raises(ValueError, match=error_msg3):
        writer.run(df3)

    # too many columns
    df4 = df.withColumn("unknown", df.id_int).select(df.id_int, df.hwm_int, "unknown")
    error_msg3 = textwrap.dedent(
        rf"""
        {prefix}

        Dataframe has columns:
            \['id_int', 'hwm_int', 'unknown'\]

        These columns present only in dataframe:
            \['unknown'\]

        These columns present only in table:
            \['text_string', 'hwm_date', 'hwm_datetime', 'float_value'\]
        """,
    ).strip()
    with pytest.raises(ValueError, match=error_msg3):
        writer.run(df4)
