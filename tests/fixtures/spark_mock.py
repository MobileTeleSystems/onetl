from unittest.mock import Mock

import pytest


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("2.3.0", id="Spark 2.3.0"),
        pytest.param("3.3.0", id="Spark 3.3.0"),
    ],
)
def spark_mock(request):
    from pyspark.sql import SparkSession

    spark = Mock(spec=SparkSession)
    spark.version = request.param  # sets the version according to the params
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    return spark


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("yarn", marks=[pytest.mark.db_connection, pytest.mark.connection]),
        pytest.param("k8s", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def spark_cluster_mock(request):
    from pyspark.sql import SparkSession

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    spark.conf = Mock()
    spark.conf.get = Mock(return_value=request.param)
    return spark
