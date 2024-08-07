from onetl._metrics.driver import SparkDriverMetrics


def test_spark_metrics_driver_is_empty():
    empty_metrics = SparkDriverMetrics()
    assert empty_metrics.is_empty

    metrics1 = SparkDriverMetrics(in_memory_bytes=1_000)
    assert metrics1.is_empty

    metrics2 = SparkDriverMetrics(in_memory_bytes=1_000_000)
    assert not metrics2.is_empty


def test_spark_metrics_driver_details():
    empty_metrics = SparkDriverMetrics()
    assert empty_metrics.details == "No data"
    assert str(empty_metrics) == empty_metrics.details

    jdbc_metrics = SparkDriverMetrics(in_memory_bytes=1_000_000)
    assert jdbc_metrics.details == "In-memory data (approximate): 1.0 MB"
    assert str(jdbc_metrics) == jdbc_metrics.details
