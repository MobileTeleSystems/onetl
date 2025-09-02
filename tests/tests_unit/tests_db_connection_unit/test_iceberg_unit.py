from __future__ import annotations

import pytest

from onetl.connection import Iceberg

pytestmark = [pytest.mark.iceberg, pytest.mark.db_connection, pytest.mark.connection]


def test_iceberg_missing_args(spark_mock):
    # no spark
    with pytest.raises(ValueError, match="field required"):
        Iceberg()

    # no catalog_name
    with pytest.raises(ValueError, match="field required"):
        Iceberg(spark=spark_mock)


def test_iceberg_instance_url(spark_mock):
    iceberg = Iceberg(catalog_name="my_catalog", spark=spark_mock)
    assert iceberg.instance_url == "iceberg://my_catalog"


def test_iceberg_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Iceberg(catalog_name="my_catalog", spark=spark_stopped)
