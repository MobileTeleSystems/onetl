import textwrap
from datetime import timedelta

from onetl._metrics.executor import SparkExecutorMetrics


def test_spark_metrics_executor_is_empty():
    empty_metrics = SparkExecutorMetrics()
    assert empty_metrics.is_empty

    run_metrics = SparkExecutorMetrics(
        total_run_time=timedelta(microseconds=1),
    )
    assert not run_metrics.is_empty


def test_spark_metrics_executor_details():
    empty_metrics = SparkExecutorMetrics()
    assert empty_metrics.details == "No data"
    assert str(empty_metrics) == empty_metrics.details

    full_metrics = SparkExecutorMetrics(
        total_run_time=timedelta(hours=2),
        total_cpu_time=timedelta(hours=1),
        peak_memory_bytes=1_000_000_000,
        memory_spilled_bytes=2_000_000_000,
        disk_spilled_bytes=3_000_000_000,
    )

    assert (
        full_metrics.details
        == textwrap.dedent(
            """
        Total run time: 2 hours
        Total CPU time: 1 hour
        Peak memory: 1.0 GB
        Memory spilled: 2.0 GB
        Disk spilled: 3.0 GB
        """,
        ).strip()
    )
    assert str(full_metrics) == full_metrics.details

    minimal_metrics = SparkExecutorMetrics(
        total_run_time=timedelta(seconds=1),
        total_cpu_time=timedelta(seconds=1),
    )

    assert (
        minimal_metrics.details
        == textwrap.dedent(
            """
        Total run time: 1 second
        Total CPU time: 1 second
        """,
        ).strip()
    )
    assert str(minimal_metrics) == minimal_metrics.details
