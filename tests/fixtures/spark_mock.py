from unittest.mock import Mock

import pytest


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("mock", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def spark_mock():
    from pyspark.sql import SparkSession

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    return spark
