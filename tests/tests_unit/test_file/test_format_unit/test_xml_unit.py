import logging

import pytest

from onetl.file.format import XML

pytestmark = [pytest.mark.xml]


@pytest.mark.parametrize(
    "spark_version, scala_version, package_version, expected_packages",
    [
        ("3.2.4", None, None, ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.4.1", "2.12", "0.18.0", ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.0.0", None, None, ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.0.0", "2.12", "0.18.0", ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.1.2", None, None, ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.1.2", "2.12", "0.16.0", ["com.databricks:spark-xml_2.12:0.16.0"]),
        ("3.2.0", "2.12", None, ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.2.0", "2.12", "0.15.0", ["com.databricks:spark-xml_2.12:0.15.0"]),
        ("3.2.4", "2.13", None, ["com.databricks:spark-xml_2.13:0.18.0"]),
        ("3.4.1", "2.13", "0.18.0", ["com.databricks:spark-xml_2.13:0.18.0"]),
        ("3.4.1", "3.0", "0.18.0", ["com.databricks:spark-xml_3.0:0.18.0"]),
        ("3.3.0", None, "0.16.0", ["com.databricks:spark-xml_2.12:0.16.0"]),
        ("3.3.0", "2.12", None, ["com.databricks:spark-xml_2.12:0.18.0"]),
        ("3.2.4", "2.12.1", "0.15.0", ["com.databricks:spark-xml_2.12:0.15.0"]),
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
        ("2.3.4", None, None),
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
    "spark_version, scala_version, package_version",
    [
        ("3.2.4", "2.11", None),
        ("3.4.1", "2.10", None),
    ],
)
def test_xml_get_packages_scala_version_error(spark_version, scala_version, package_version):
    with pytest.raises(ValueError, match=f"Scala version must be at least 2.12, got {scala_version}"):
        XML.get_packages(
            spark_version=spark_version,
            scala_version=scala_version,
            package_version=package_version,
        )


@pytest.mark.parametrize(
    "spark_version, scala_version, package_version",
    [
        ("3.2.4", "2.12", "0.13.0"),
        ("3.4.1", "2.12", "0.10.0"),
    ],
)
def test_xml_get_packages_package_version_error(spark_version, scala_version, package_version):
    with pytest.raises(ValueError, match=r"Package version must be above 0.13, got \d+\.\d+\.\d+"):
        XML.get_packages(
            spark_version=spark_version,
            scala_version=scala_version,
            package_version=package_version,
        )


def test_xml_options_row_tag_case():
    xml1 = XML(row_tag="item")
    xml2 = XML(rowTag="item")
    assert xml1 == xml2
    assert xml1.row_tag == "item"
    assert xml2.row_tag == "item"


@pytest.mark.parametrize(
    "known_option, raw_value, expected_value",
    [
        ("samplingRatio", 0.1, 0.1),
        ("excludeAttribute", True, True),
        ("mode", "PERMISSIVE", "PERMISSIVE"),
        ("inferSchema", True, True),
        ("columnNameOfCorruptRecord", "value", "value"),
        ("attributePrefix", "value", "value"),
        ("valueTag", "value", "value"),
        ("charset", "value", "value"),
        ("ignoreSurroundingSpaces", True, True),
        ("wildcardColName", "value", "value"),
        ("rowValidationXSDPath", "value", "value"),
        ("ignoreNamespace", True, True),
        ("timestampFormat", "value", "value"),
        ("dateFormat", "value", "value"),
        ("rootTag", "value", "value"),
        ("declaration", "value", "value"),
        ("arrayElementName", "value", "value"),
        ("nullValue", "value", "value"),
        ("compression", "gzip", "gzip"),
    ],
)
def test_xml_options_known(known_option, raw_value, expected_value):
    xml = XML.parse({known_option: raw_value, "rowTag": "item"})
    assert getattr(xml, known_option) == expected_value


def test_xml_option_path_error():
    msg = r"Options \['path'\] are not allowed to use in a XML"
    with pytest.raises(ValueError, match=msg):
        XML(rowTag="item", path="/path")


def test_xml_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        xml = XML(rowTag="item", unknownOption="abc")
        assert xml.unknownOption == "abc"
    assert "Options ['unknownOption'] are not known by XML, are you sure they are valid?" in caplog.text


def test_xml_options_repr():
    # There are too many options with default value None, hide them from repr
    xml = XML(rowTag="item", mode="PERMISSIVE", unknownOption="abc")
    assert repr(xml) == "XML(mode='PERMISSIVE', rowTag='item', unknownOption='abc')"
