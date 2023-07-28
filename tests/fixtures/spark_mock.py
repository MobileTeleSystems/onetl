from unittest.mock import Mock

import pytest


@pytest.fixture(
    scope="function",
    params=[pytest.param("mock-spark-mo-packages", marks=[pytest.mark.db_connection, pytest.mark.connection])],
)
def spark_no_packages():
    from pyspark.sql import SparkSession

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    return spark


@pytest.fixture(
    scope="function",
    params=[pytest.param("mock-spark", marks=[pytest.mark.db_connection, pytest.mark.connection])],
)
def spark_mock():
    from pyspark.sql import SparkSession

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    spark._sc = Mock()
    spark._sc._gateway = Mock()
    return spark


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("mock-master-yarn", marks=[pytest.mark.db_connection, pytest.mark.connection]),
        pytest.param("mock-master-k8s", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def spark_cluster_mock(request, spark_mock):
    spark = spark_mock
    spark.conf = Mock()
    spark.conf.get = Mock(return_value=request.param)
    return spark
