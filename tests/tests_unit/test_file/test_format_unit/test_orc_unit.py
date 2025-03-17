import logging

import pytest

from onetl.file.format import ORC

pytestmark = [pytest.mark.orc]


@pytest.mark.parametrize(
    "known_option, value, expected_value",
    [
        ("mergeSchema", True, True),
        ("compression", "snappy", "snappy"),
        ("orc.bloom.filter.columns", "id,name", "id,name"),
        ("orc.bloom.filter.fpp", 0.01, 0.01),
        ("orc.column.encoding.direct", "highly_selective_column", "highly_selective_column"),
    ],
)
def test_orc_options_known(known_option, value, expected_value):
    orc = ORC.parse({known_option: value})
    assert getattr(orc, known_option) == expected_value


def test_orc_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        orc = ORC(unknown="abc")
        assert orc.unknown == "abc"

    assert ("Options ['unknown'] are not known by ORC, are you sure they are valid?") in caplog.text


def test_orc_options_repr():  # There are too many options with default value None, hide them from repr
    orc = ORC(compression="snappy", unknownOption="abc")
    assert repr(orc) == "ORC(compression='snappy', unknownOption='abc')"

    orc_with_dots = ORC.parse({"orc.bloom.filter.columns": "id,name", "unknownOption": "abc"})
    assert repr(orc_with_dots) == "ORC.parse({'orc.bloom.filter.columns': 'id,name', 'unknownOption': 'abc'})"


@pytest.mark.parametrize(
    "option",
    [
        "spark.sql.orc.impl",
        "spark.sql.orc.enableVectorizedReader",
    ],
)
def test_orc_options_prohibited(option):
    msg = rf"Options \['{option}'\] are not allowed to use in a ORC"
    with pytest.raises(ValueError, match=msg):
        ORC.parse({option: "value"})
