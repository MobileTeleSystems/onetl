import textwrap

from onetl._metrics.input import SparkInputMetrics


def test_spark_metrics_input_is_empty():
    empty_metrics = SparkInputMetrics()
    assert empty_metrics.is_empty

    metrics1 = SparkInputMetrics(read_rows=1)
    assert not metrics1.is_empty

    metrics2 = SparkInputMetrics(read_files=1)
    assert not metrics2.is_empty

    metrics3 = SparkInputMetrics(read_bytes=1)
    assert not metrics3.is_empty


def test_spark_metrics_input_details():
    empty_metrics = SparkInputMetrics()
    assert empty_metrics.details == "No data"
    assert str(empty_metrics) == empty_metrics.details

    file_df_metrics = SparkInputMetrics(
        read_rows=1_000,
        read_partitions=4,
        read_files=4,
        read_bytes=2_000_000,
        raw_file_bytes=5_000_000,
    )

    expected = textwrap.dedent(
        """
        Read rows: 1000
        Read partitions: 4
        Read files: 4
        Read size: 2.0 MB
        Raw files size: 5.0 MB
        """,
    )
    assert file_df_metrics.details == expected.strip()
    assert str(file_df_metrics) == file_df_metrics.details

    jdbc_metrics = SparkInputMetrics(
        read_rows=1_000,
    )

    assert jdbc_metrics.details == "Read rows: 1000"
    assert str(jdbc_metrics) == jdbc_metrics.details
