import time
from contextlib import suppress
from pathlib import Path

import pytest

from onetl._metrics.recorder import SparkMetricsRecorder
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import CSV, JSON

pytestmark = [
    pytest.mark.local_fs,
    pytest.mark.file_df_connection,
    pytest.mark.connection,
    pytest.mark.csv,
    # SparkListener does not give guarantees of delivering execution metrics in time
    pytest.mark.flaky(reruns=5),
]


def test_spark_metrics_recorder_file_df_reader(
    spark,
    local_fs_file_df_connection_with_path_and_files,
):
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    files_path: Path = source_path / "csv/with_header"

    reader = FileDFReader(
        connection=local_fs,
        format=CSV(header=True),
        source_path=files_path,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.input.read_rows
        assert metrics.input.read_bytes
        # file related metrics are too flaky to assert


def test_spark_metrics_recorder_file_df_reader_no_files(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_schema,
):
    local_fs, source_path = local_fs_file_df_connection_with_path

    reader = FileDFReader(
        connection=local_fs,
        format=CSV(),
        source_path=source_path,
        df_schema=file_df_schema,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run()
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.input.read_rows
        assert not metrics.input.read_files


def test_spark_metrics_recorder_file_df_reader_no_data_after_filter(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_schema,
):
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    files_path = source_path / "csv/with_header"

    reader = FileDFReader(
        connection=local_fs,
        format=CSV(header=True),
        source_path=files_path,
        df_schema=file_df_schema,
    )

    with SparkMetricsRecorder(spark) as recorder:
        df = reader.run().where("str_value = 'unknown'")
        df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()

        # Spark does not include skipped rows to metrics
        assert not metrics.input.read_rows


def test_spark_metrics_recorder_file_df_reader_error(
    spark,
    local_fs_file_df_connection_with_path_and_files,
):
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    files_path: Path = source_path / "csv/with_header"

    reader = FileDFReader(
        connection=local_fs,
        format=JSON(),
        source_path=files_path,
    )

    with SparkMetricsRecorder(spark) as recorder:
        with suppress(Exception):
            df = reader.run()
            df.collect()

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        # some files metadata may be scanned, but file content was not read
        assert not metrics.input.raw_file_bytes


def test_spark_metrics_recorder_file_df_writer(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    local_fs, target_path = local_fs_file_df_connection_with_path

    writer = FileDFWriter(
        connection=local_fs,
        format=CSV(),
        target_path=target_path,
        options=FileDFWriter.Options(if_exists="append"),
    )

    with SparkMetricsRecorder(spark) as recorder:
        writer.run(file_df_dataframe)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert metrics.output.written_rows == file_df_dataframe.count()
        assert metrics.output.written_bytes
        # file related metrics are too flaky to assert


def test_spark_metrics_recorder_file_df_writer_empty_input(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    local_fs, target_path = local_fs_file_df_connection_with_path

    df = file_df_dataframe.limit(0)

    writer = FileDFWriter(
        connection=local_fs,
        format=CSV(),
        target_path=target_path,
        options=FileDFWriter.Options(if_exists="append"),
    )

    with SparkMetricsRecorder(spark) as recorder:
        writer.run(df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows
        assert not metrics.output.written_bytes


def test_spark_metrics_recorder_file_df_writer_driver_failed(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    local_fs, target_path = local_fs_file_df_connection_with_path

    df = file_df_dataframe

    writer = FileDFWriter(
        connection=local_fs,
        format=CSV(),
        target_path=target_path,
        options=FileDFWriter.Options(if_exists="error"),
    )

    with SparkMetricsRecorder(spark) as recorder:
        with suppress(Exception):
            writer.run(df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows
        assert not metrics.output.written_bytes


def test_spark_metrics_recorder_file_df_writer_executor_failed(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    @udf(returnType=IntegerType())
    def raise_exception():
        raise ValueError("Force task failure")

    local_fs, target_path = local_fs_file_df_connection_with_path

    failing_df = file_df_dataframe.select(raise_exception().alias("some"))

    writer = FileDFWriter(
        connection=local_fs,
        format=CSV(),
        target_path=target_path,
        options=FileDFWriter.Options(if_exists="append"),
    )

    with SparkMetricsRecorder(spark) as recorder:
        with suppress(Exception):
            writer.run(failing_df)

        time.sleep(0.1)  # sleep to fetch late metrics from SparkListener
        metrics = recorder.metrics()
        assert not metrics.output.written_rows
        assert not metrics.output.written_bytes
