import logging

import pytest

try:
    from pydantic.v1 import SecretStr
except (ImportError, AttributeError):
    from pydantic import SecretStr  # type: ignore[no-redef, assignment]

from onetl.file.format import Excel

pytestmark = [pytest.mark.excel]


@pytest.mark.parametrize(
    "spark_version",
    [
        "2.2.1",
        "2.3.1",
        "2.4.8",
    ],
)
def test_excel_get_packages_spark_version_not_supported(spark_version):
    with pytest.raises(ValueError, match=f"Spark version should be at least 3.2, got {spark_version}"):
        Excel.get_packages(spark_version=spark_version)


def test_excel_get_packages_scala_version_not_supported():
    with pytest.raises(ValueError, match="Scala version should be at least 2.12, got 2.11"):
        Excel.get_packages(spark_version="3.2.4", scala_version="2.11")


def test_excel_get_packages_package_version_not_supported():
    with pytest.raises(ValueError, match="Package version should be at least 0.15, got 0.13.7"):
        Excel.get_packages(spark_version="3.2.4", package_version="0.13.7")


@pytest.mark.parametrize(
    "spark_version, scala_version, package_version, packages",
    [
        # Detect Scala version by Spark version
        ("3.2.4", None, None, ["com.crealytics:spark-excel_2.12:3.2.4_0.20.4"]),
        ("3.5.5", None, None, ["com.crealytics:spark-excel_2.12:3.5.5_0.20.4"]),
        # Override Scala version
        ("3.2.4", "2.12", None, ["com.crealytics:spark-excel_2.12:3.2.4_0.20.4"]),
        ("3.2.4", "2.13", None, ["com.crealytics:spark-excel_2.13:3.2.4_0.20.4"]),
        ("3.5.5", "2.12", None, ["com.crealytics:spark-excel_2.12:3.5.5_0.20.4"]),
        ("3.5.5", "2.13", None, ["com.crealytics:spark-excel_2.13:3.5.5_0.20.4"]),
        # Override package version
        ("3.2.0", None, "0.16.0", ["com.crealytics:spark-excel_2.12:3.2.0_0.16.0"]),
        ("3.5.5", None, "0.18.0", ["com.crealytics:spark-excel_2.12:3.5.5_0.18.0"]),
        # Scala version contain three digits when only two needed
        ("3.5.5", "2.12.1", None, ["com.crealytics:spark-excel_2.12:3.5.5_0.20.4"]),
    ],
)
def test_excel_get_packages(caplog, spark_version, scala_version, package_version, packages):
    with caplog.at_level(level=logging.WARNING):
        result = Excel.get_packages(
            spark_version=spark_version,
            scala_version=scala_version,
            package_version=package_version,
        )

        if package_version:
            assert f"Passed custom package version '{package_version}', it is not guaranteed to be supported"

    assert result == packages


def test_excel_options_default():
    excel = Excel()
    assert not excel.header


def test_excel_options_default_override():
    excel = Excel(header=True)
    assert excel.header


@pytest.mark.parametrize(
    "known_option, value, expected_value",
    [
        ("dataAddress", "value", "value"),
        ("treatEmptyValuesAsNulls", True, True),
        ("setErrorCellsToFallbackValues", True, True),
        ("usePlainNumberFormat", True, True),
        ("inferSchema", True, True),
        ("timestampFormat", "value", "value"),
        ("maxRowsInMemory", 100, 100),
        ("maxByteArraySize", 1024, 1024),
        ("tempFileThreshold", 1024, 1024),
        ("excerptSize", 100, 100),
        ("workbookPassword", "value", SecretStr("value")),
        ("dateFormat", "value", "value"),
    ],
)
def test_excel_options_known(known_option, value, expected_value):
    excel = Excel.parse({known_option: value})
    assert getattr(excel, known_option) == expected_value


def test_excel_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        excel = Excel(unknown="abc")
        assert excel.unknown == "abc"

    assert ("Options ['unknown'] are not known by Excel, are you sure they are valid?") in caplog.text


def test_excel_options_repr():
    # There are too many options with default value None, hide them from repr
    excel = Excel(header=True, timestampFormat="yyyy-MM-dd HH:mm:ss", unknownOption="abc")
    assert repr(excel) == "Excel(header=True, timestampFormat='yyyy-MM-dd HH:mm:ss', unknownOption='abc')"


@pytest.mark.local_fs
def test_excel_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.crealytics.spark.excel.v2.ExcelDataSource'"
    with pytest.raises(ValueError, match=msg):
        Excel().check_if_supported(spark_no_packages)
