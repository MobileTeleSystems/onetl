from unittest.mock import Mock

import pytest


@pytest.fixture(
    scope="function",
    params=[pytest.param("mock-spark-stopped", marks=[pytest.mark.db_connection, pytest.mark.connection])],
)
def spark_stopped():
    import pyspark
    from pyspark.sql import SparkSession

    spark_conf = {}

    spark_context = Mock()
    spark_context.appName = "abc"
    spark_context.applicationId = "local-123"
    spark_context.getConf = Mock(return_value=spark_conf)
    spark_context._gateway = Mock()

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    spark.sparkContext.applicationId = "local-123"
    spark.version = pyspark.__version__
    spark.sparkContext = spark_context
    spark._sc = spark_context
    spark._conf = spark_conf

    spark._jsc = Mock()
    spark._jsc.sc = Mock()
    spark._jsc.sc().isStopped = Mock(return_value=True)
    return spark


@pytest.fixture(
    scope="function",
    params=[pytest.param("mock-spark-no-packages", marks=[pytest.mark.db_connection, pytest.mark.connection])],
)
def spark_no_packages():
    import pyspark
    from pyspark.sql import SparkSession

    spark_conf = {}

    spark_context = Mock()
    spark_context.appName = "abc"
    spark_context.applicationId = "local-123"
    spark_context.getConf = Mock(return_value=spark_conf)
    spark_context._gateway = Mock()

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    spark.sparkContext.applicationId = "local-123"
    spark.version = pyspark.__version__
    spark.sparkContext = spark_context
    # no spark._sc
    spark._conf = spark_conf

    spark._jsc = Mock()
    spark._jsc.sc = Mock()
    spark._jsc.sc().isStopped = Mock(return_value=False)
    return spark


@pytest.fixture(
    scope="function",
    params=[pytest.param("mock-spark", marks=[pytest.mark.db_connection, pytest.mark.connection])],
)
def spark_mock():
    import pyspark
    from pyspark.sql import SparkSession

    spark_conf = {}

    spark_context = Mock()
    spark_context.appName = "abc"
    spark_context.applicationId = "local-123"
    spark_context.getConf = Mock(return_value=spark_conf)
    spark_context._gateway = Mock()

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    spark.sparkContext.applicationId = "local-123"
    spark.version = pyspark.__version__
    spark.sparkContext = spark_context
    spark._sc = spark_context
    spark._conf = spark_conf

    spark._jsc = Mock()
    spark._jsc.sc = Mock()
    spark._jsc.sc().isStopped = Mock(return_value=False)
    return spark
