import logging

import pytest

from onetl.file.format import XML


@pytest.mark.parametrize(
    "spark_version, scala_version, package_version, expected_packages",
    [
        ("3.2.4", None, None, ["com.databricks:spark-xml_2.12:0.17.0"]),
        ("3.4.1", "2.12", "0.18.0", ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.0.0", None, None, ["com.databricks:spark-xml_2.12:0.17.0"]),
        ("3.0.0", "2.12", "0.17.0", ["com.databricks:spark-xml_2.12:0.17.0"]),
        ("3.1.2", None, None, ["com.databricks:spark-xml_2.12:0.17.0"]),
        ("3.1.2", "2.12", "0.16.0", ["com.databricks:spark-xml_2.12:0.16.0"]),
        ("3.2.0", "2.12", None, ["com.databricks:spark-xml_2.12:0.17.0"]),
        ("3.2.0", "2.12", "0.15.0", ["com.databricks:spark-xml_2.12:0.15.0"]),
        ("3.3.0", None, "0.16.0", ["com.databricks:spark-xml_2.12:0.16.0"]),
        ("3.3.0", "2.12", None, ["com.databricks:spark-xml_2.12:0.17.0"]),
    ],
)
def test_xml_get_packages(spark_version, scala_version, package_version, expected_packages):
    result = XML.get_packages(
        spark_version=spark_version,
        scala_version=scala_version,
        package_version=package_version,
    )
    assert result == expected_packages


@pytest.mark.parametrize(
    "spark_version, scala_version, package_version",
    [
        ("2.4.8", None, None),
        ("2.4.8", "2.11", "0.13.0"),
        ("2.3.4", None, None),
        ("2.3.4", "2.11", "0.12.0"),
        ("2.2.3", None, None),
        ("2.2.3", "2.11", "0.11.0"),
    ],
)
def test_xml_get_packages_restriction_for_spark_2x(spark_version, scala_version, package_version):
    with pytest.raises(ValueError, match=r"Spark version must be 3.x, got \d+\.\d+"):
        XML.get_packages(
            spark_version=spark_version,
            scala_version=scala_version,
            package_version=package_version,
        )


@pytest.mark.parametrize(
    "known_option",
    [
        "samplingRatio",
        "excludeAttribute",
        "treatEmptyValuesAsNulls",
        "mode",
        "inferSchema",
        "columnNameOfCorruptRecord",
        "attributePrefix",
        "valueTag",
        "charset",
        "ignoreSurroundingSpaces",
        "wildcardColName",
        "rowValidationXSDPath",
        "ignoreNamespace",
        "timestampFormat",
        "dateFormat",
        "rootTag",
        "declaration",
        "arrayElementName",
        "nullValue",
        "compression",
    ],
)
def test_xml_options_known(known_option):
    xml = XML.parse({known_option: "value", "row_tag": "item"})
    assert getattr(xml, known_option) == "value"


def test_xml_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        xml = XML(row_tag="item", unknownOption="abc")
        assert xml.unknownOption == "abc"
    assert "Options ['unknownOption'] are not known by XML, are you sure they are valid?" in caplog.text


@pytest.mark.local_fs
def test_xml_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.databricks.spark.xml.XmlReader'"
    with pytest.raises(ValueError, match=msg):
        XML(row_tag="item").check_if_supported(spark_no_packages)
