import pytest

from onetl.file.format import Iceberg

pytestmark = [pytest.mark.iceberg]


@pytest.mark.parametrize(
    "package_version,spark_version,scala_version,package",
    [
        ("1.4.0", "3.3", None, "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0"),
        ("1.9.2", "3.5", "2.12", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"),
    ],
)
def test_iceberg_get_packages(package_version, spark_version, scala_version, package):
    assert Iceberg.get_packages(
        package_version=package_version,
        spark_version=spark_version,
        scala_version=scala_version,
    ) == [package]


def test_iceberg_format_repr():
    iceberg = Iceberg()
    assert repr(iceberg) == "Iceberg()"


@pytest.mark.parametrize(
    "option",
    [
        "spark.sql.catalog",
        "spark.sql.extensions",
    ],
)
def test_iceberg_options_prohibited(option):
    msg = rf"Options \['{option}'\] are not allowed to use in a Iceberg"
    with pytest.raises(ValueError, match=msg):
        Iceberg.parse({option: "value"})


@pytest.mark.local_fs
def test_iceberg_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.apache.iceberg.spark.SparkSessionCatalog'"
    with pytest.raises(ValueError, match=msg):
        Iceberg().check_if_supported(spark_no_packages)
