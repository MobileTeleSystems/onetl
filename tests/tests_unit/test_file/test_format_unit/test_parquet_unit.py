import logging

import pytest

from onetl.file.format import Parquet

pytestmark = [pytest.mark.parquet]


@pytest.mark.parametrize(
    "known_option, value, expected_value",
    [
        ("mergeSchema", True, True),
        ("compression", "snappy", "snappy"),
        ("parquet.bloom.filter.enabled#id", True, True),
        ("parquet.bloom.filter.expected.ndv#id", 100, 100),
    ],
)
def test_parquet_options_known(known_option, value, expected_value):
    parquet = Parquet.parse({known_option: value})
    assert getattr(parquet, known_option) == expected_value


def test_parquet_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        parquet = Parquet(unknown="abc")
        assert parquet.unknown == "abc"

    assert ("Options ['unknown'] are not known by Parquet, are you sure they are valid?") in caplog.text


def test_parquet_options_repr():  # There are too many options with default value None, hide them from repr
    parquet = Parquet(mergeSchema=True, compression="snappy", unknownOption="abc")
    assert repr(parquet) == "Parquet(compression='snappy', mergeSchema=True, unknownOption='abc')"

    parquet_with_dots = Parquet.parse({"parquet.bloom.filter.enabled#id": True, "unknownOption": "abc"})
    assert repr(parquet_with_dots) == "Parquet.parse({'parquet.bloom.filter.enabled#id': True, 'unknownOption': 'abc'})"


@pytest.mark.parametrize(
    "option",
    [
        "spark.sql.parquet.binaryAsString",
        "spark.sql.parquet.int96AsTimestamp",
    ],
)
def test_parquet_options_prohibited(option):
    msg = rf"Options \['{option}'\] are not allowed to use in a Parquet"
    with pytest.raises(ValueError, match=msg):
        Parquet.parse({option: "value"})
