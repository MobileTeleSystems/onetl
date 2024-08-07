import time

import pytest

from onetl._metrics.recorder import SparkMetricsRecorder
from onetl._util.spark import get_spark_version
from onetl.connection import Postgres
from onetl.db import DBReader, DBWriter

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.db_connection,
    pytest.mark.connection,
    # SparkListener does not give guarantees of delivering execution metrics in time
    pytest.mark.flaky(reruns=5),
]


def test_spark_metrics_recorder_postgres_read(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        source=load_table_data.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        rows = len(df.collect())

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.input.read_rows == rows
        # JDBC does not provide information about data size
        assert not metrics.input.read_bytes


def test_spark_metrics_recorder_postgres_read_empty_source(spark, processing, prepare_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        source=prepare_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows


def test_spark_metrics_recorder_postgres_read_no_data_after_filter(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(
        connection=postgres,
        source=load_table_data.full_name,
        where="1=0",
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows


def test_spark_metrics_recorder_postgres_sql(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = postgres.sql(f"SELECT * FROM {load_table_data.full_name}")
        rows = len(df.collect())

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.input.read_rows == rows


def test_spark_metrics_recorder_postgres_write(spark, processing, get_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    df = processing.create_spark_df(spark)

    writer = DBWriter(
        connection=postgres,
        target=get_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        writer.run(df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        spark_version = get_spark_version(spark)
        if spark_version.major >= 3:
            # Spark started collecting JDBC write bytes only since Spark 3.0:
            # https://issues.apache.org/jira/browse/SPARK-29461
            assert metrics.output.written_rows == df.count()
        else:
            assert not metrics.output.written_rows
        # JDBC does not provide information about data size
        assert not metrics.output.written_bytes


def test_spark_metrics_recorder_postgres_write_empty(spark, processing, get_schema_table):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )
    df = processing.create_spark_df(spark).limit(0)

    writer = DBWriter(
        connection=postgres,
        target=get_schema_table.full_name,
    )

    with SparkMetricsRecorder(spark) as recorder:
        writer.run(df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows


def test_spark_metrics_recorder_postgres_fetch(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with SparkMetricsRecorder(spark) as recorder:
        postgres.fetch(f"SELECT * FROM {load_table_data.full_name}")

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows


def test_spark_metrics_recorder_postgres_execute(spark, processing, load_table_data):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    new_table = load_table_data.full_name + "_new"

    with SparkMetricsRecorder(spark) as recorder:
        postgres.execute(f"CREATE TABLE {new_table} AS SELECT * FROM {load_table_data.full_name}")

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows
