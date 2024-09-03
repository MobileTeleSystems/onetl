import textwrap
from datetime import timedelta

from onetl._metrics.command import SparkCommandMetrics
from onetl._metrics.driver import SparkDriverMetrics
from onetl._metrics.executor import SparkExecutorMetrics
from onetl._metrics.input import SparkInputMetrics
from onetl._metrics.output import SparkOutputMetrics


def test_spark_metrics_command_is_empty():
    empty_metrics = SparkCommandMetrics()
    assert empty_metrics.is_empty

    no_input_output = SparkCommandMetrics(
        driver=SparkDriverMetrics(in_memory_bytes=1_000_000),
        executor=SparkExecutorMetrics(total_run_time=timedelta(microseconds=1)),
    )
    assert no_input_output.is_empty

    with_input = SparkCommandMetrics(
        input=SparkInputMetrics(read_rows=1),
    )
    assert not with_input.is_empty

    with_output = SparkCommandMetrics(
        output=SparkOutputMetrics(written_rows=1),
    )
    assert not with_output.is_empty


def test_spark_metrics_command_details():
    empty_metrics = SparkCommandMetrics()
    assert empty_metrics.details == "No data"
    assert str(empty_metrics) == empty_metrics.details

    jdbc_fetch_metrics = SparkCommandMetrics(
        input=SparkInputMetrics(read_rows=1_000),
        driver=SparkDriverMetrics(in_memory_bytes=1_000_000),
    )

    expected = textwrap.dedent(
        """
        Input:
            Read rows: 1000
        Driver:
            In-memory data (approximate): 1.0 MB
        """,
    )
    assert jdbc_fetch_metrics.details == expected.strip()
    assert str(jdbc_fetch_metrics) == jdbc_fetch_metrics.details

    jdbc_write_metrics = SparkCommandMetrics(
        output=SparkOutputMetrics(written_rows=1_000),
        executor=SparkExecutorMetrics(
            total_run_time=timedelta(seconds=2),
            total_cpu_time=timedelta(seconds=1),
        ),
    )
    expected = textwrap.dedent(
        """
        Output:
            Written rows: 1000
        Executor:
            Total run time: 2 seconds
            Total CPU time: 1 second
        """,
    )
    assert jdbc_write_metrics.details == expected.strip()
    assert str(jdbc_write_metrics) == jdbc_write_metrics.details
