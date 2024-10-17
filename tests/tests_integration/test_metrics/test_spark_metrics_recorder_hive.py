import time
from contextlib import suppress

import pytest

from onetl._metrics.recorder import SparkMetricsRecorder
from onetl.connection import Hive
from onetl.db import DBReader, DBWriter
from tests.util.rand import rand_str

pytestmark = [
    pytest.mark.hive,
    pytest.mark.db_connection,
    pytest.mark.connection,
    # SparkListener does not give guarantees of delivering execution metrics in time
    pytest.mark.flaky(reruns=5),
]


def test_spark_metrics_recorder_hive_read_count(spark, load_table_data):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    reader = DBReader(
        connection=hive,
        source=load_table_data.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        rows = df.count()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.input.read_rows == rows
        assert metrics.input.read_bytes
        # in some cases files are read, in some cases only metastore statistics is used


def test_spark_metrics_recorder_hive_read_collect(spark, load_table_data):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    reader = DBReader(
        connection=hive,
        source=load_table_data.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        rows = len(df.collect())

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.input.read_rows == rows
        assert metrics.input.read_bytes
        # file related metrics are too flaky to assert


def test_spark_metrics_recorder_hive_read_empty_source(spark, prepare_schema_table):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    reader = DBReader(
        connection=hive,
        source=prepare_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows
        assert not metrics.input.read_bytes


def test_spark_metrics_recorder_hive_read_no_data_after_filter(spark, load_table_data):
    hive = Hive(cluster="rnd-dwh", spark=spark)
    reader = DBReader(
        connection=hive,
        source=load_table_data.full_name,
        where="1=0",
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows
        assert not metrics.input.read_bytes


def test_spark_metrics_recorder_hive_sql(spark, load_table_data):
    hive = Hive(cluster="rnd-dwh", spark=spark)

    with SparkMetricsRecorder(spark) as recorder:
        df = hive.sql(f"SELECT * FROM {load_table_data.full_name}")
        rows = len(df.collect())

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.input.read_rows == rows
        assert metrics.input.read_bytes
        # file related metrics are too flaky to assert


def test_spark_metrics_recorder_hive_write(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark)

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        writer.run(df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.output.written_rows == df.count()
        assert metrics.output.written_bytes
        # file related metrics are too flaky to assert


def test_spark_metrics_recorder_hive_write_empty(spark, processing, get_schema_table):
    df = processing.create_spark_df(spark).limit(0)

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        writer.run(df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows


def test_spark_metrics_recorder_hive_write_driver_failed(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark).limit(0)

    mismatch_df = df.withColumn("mismatch", df.id_int)

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        target=prepare_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        with suppress(Exception):
            writer.run(mismatch_df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows


def test_spark_metrics_recorder_hive_write_executor_failed(spark, processing, get_schema_table):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    df = processing.create_spark_df(spark).limit(0)

    @udf(returnType=IntegerType())
    def raise_exception():
        raise ValueError("Force task failure")

    failing_df = df.select(raise_exception().alias("some"))

    hive = Hive(cluster="rnd-dwh", spark=spark)
    writer = DBWriter(
        connection=hive,
        target=get_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        with suppress(Exception):
            writer.run(failing_df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows


def test_spark_metrics_recorder_hive_execute(request, spark, processing, get_schema_table):
    df = processing.create_spark_df(spark)
    view_name = rand_str()
    df.createOrReplaceTempView(view_name)

    def finalizer():
        spark.sql(f"DROP VIEW IF EXISTS {view_name}")

    request.addfinalizer(finalizer)

    hive = Hive(cluster="rnd-dwh", spark=spark)

    with SparkMetricsRecorder(spark) as recorder:
        hive.execute(f"CREATE TABLE {get_schema_table.full_name} AS SELECT * FROM {view_name}")

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.output.written_rows == df.count()
        assert metrics.output.written_bytes
        # file related metrics are too flaky to assert
