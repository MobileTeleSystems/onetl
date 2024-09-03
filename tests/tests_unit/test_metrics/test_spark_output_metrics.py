import textwrap

from onetl._metrics.output import SparkOutputMetrics


def test_spark_metrics_output_is_empty():
    empty_metrics = SparkOutputMetrics()
    assert empty_metrics.is_empty

    metric1 = SparkOutputMetrics(written_rows=1)
    assert not metric1.is_empty

    metric2 = SparkOutputMetrics(written_bytes=1)
    assert not metric2.is_empty

    metric3 = SparkOutputMetrics(created_files=1)
    assert not metric3.is_empty


def test_spark_metrics_output_details():
    empty_metrics = SparkOutputMetrics()
    assert empty_metrics.details == "No data"
    assert str(empty_metrics) == empty_metrics.details

    hive_metrics = SparkOutputMetrics(
        written_rows=1_000,
        written_bytes=2_000_000,
        created_files=4,
        created_partitions=4,
    )

    expected = textwrap.dedent(
        """
        Written rows: 1000
        Written size: 2.0 MB
        Created files: 4
        Created partitions: 4
        """,
    )
    assert hive_metrics.details == expected.strip()
    assert str(hive_metrics) == hive_metrics.details

    jdbc_metrics = SparkOutputMetrics(written_rows=1_000)

    assert jdbc_metrics.details == "Written rows: 1000"
    assert str(jdbc_metrics) == jdbc_metrics.details
