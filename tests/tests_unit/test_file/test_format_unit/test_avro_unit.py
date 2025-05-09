import logging

import pytest

from onetl.file.format import Avro

pytestmark = [pytest.mark.avro]


@pytest.mark.parametrize(
    "spark_version",
    [
        "2.2.1",
        "2.3.1",
    ],
)
def test_avro_get_packages_spark_version_not_supported(spark_version):
    with pytest.raises(ValueError, match=f"Spark version should be at least 2.4, got {spark_version}"):
        Avro.get_packages(spark_version=spark_version)


def test_avro_get_packages_scala_version_not_supported():
    with pytest.raises(ValueError, match="Scala version should be at least 2.11, got 2.10"):
        Avro.get_packages(spark_version="2.4.0", scala_version="2.10")


@pytest.mark.parametrize(
    "spark_version, scala_version, package",
    [
        # Detect Scala version by Spark version
        ("2.4.0", None, "org.apache.spark:spark-avro_2.11:2.4.0"),
        ("3.5.5", None, "org.apache.spark:spark-avro_2.12:3.5.5"),
        # Override Scala version
        ("2.4.0", "2.11", "org.apache.spark:spark-avro_2.11:2.4.0"),
        ("2.4.0", "2.12", "org.apache.spark:spark-avro_2.12:2.4.0"),
        ("3.5.5", "2.12", "org.apache.spark:spark-avro_2.12:3.5.5"),
        ("3.5.5", "2.13", "org.apache.spark:spark-avro_2.13:3.5.5"),
        # Scala version contain three digits when only two needed
        ("3.5.5", "2.12.1", "org.apache.spark:spark-avro_2.12:3.5.5"),
    ],
)
def test_avro_get_packages(spark_version, scala_version, package):
    assert Avro.get_packages(spark_version=spark_version, scala_version=scala_version) == [package]


@pytest.mark.parametrize(
    "value, real_value",
    [
        ({"name": "abc", "type": "string"}, {"name": "abc", "type": "string"}),
        ('{"name": "abc", "type": "string"}', {"name": "abc", "type": "string"}),
    ],
)
def test_avro_options_schema(value, real_value):
    avro = Avro(avroSchema=value)
    assert avro.schema_dict == real_value


@pytest.mark.parametrize(
    "name, real_name, value",
    [
        ("avroSchema", "schema_dict", {"name": "abc", "type": "string"}),
        ("avroSchemaUrl", "schema_url", "http://example.com"),
    ],
)
def test_avro_options_alias(name, real_name, value):
    avro = Avro.parse({name: value})
    assert getattr(avro, real_name) == value


@pytest.mark.parametrize(
    "known_option, value, expected_value",
    [
        ("positionalFieldMatching", True, True),
        ("mode", "PERMISSIVE", "PERMISSIVE"),
        ("datetimeRebaseMode", "CORRECTED", "CORRECTED"),
        ("recordName", "value", "value"),
        ("recordNamespace", "value", "value"),
        ("compression", "snappy", "snappy"),
        ("positionalFieldMatching", True, True),
        ("enableStableIdentifiersForUnionType", True, True),
    ],
)
def test_avro_options_known(known_option, value, expected_value):
    avro = Avro.parse({known_option: value})
    assert getattr(avro, known_option) == expected_value


def test_avro_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        avro = Avro(unknown="abc")
        assert avro.unknown == "abc"

    assert ("Options ['unknown'] are not known by Avro, are you sure they are valid?") in caplog.text


def test_avro_options_repr():
    # There are too many options with default value None, hide them from repr
    avro = Avro(
        avroSchema={"name": "abc", "type": "string"},
        compression="snappy",
        mode="PERMISSIVE",
        unknownOption="abc",
    )
    assert (
        repr(avro)
        == "Avro(avroSchema={'name': 'abc', 'type': 'string'}, compression='snappy', mode='PERMISSIVE', unknownOption='abc')"
    )


@pytest.mark.parametrize(
    "option",
    [
        "spark.sql.legacy.replaceDatabricksSparkAvro.enabled",
        "spark.sql.avro.compression.codec",
    ],
)
def test_avro_options_prohibited(option):
    msg = rf"Options \['{option}'\] are not allowed to use in a Avro"
    with pytest.raises(ValueError, match=msg):
        Avro.parse({option: "value"})


@pytest.mark.local_fs
def test_avro_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.apache.spark.sql.avro.AvroFileFormat'"
    with pytest.raises(ValueError, match=msg):
        Avro().check_if_supported(spark_no_packages)
