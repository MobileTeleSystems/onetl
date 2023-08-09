import logging

import pytest

from onetl.file.format import ORC


@pytest.mark.parametrize(
    "known_option",
    [
        "mergeSchema",
        "compression",
        "orc.bloom.filter.columns",
    ],
)
def test_orc_options_known(known_option):
    orc = ORC.parse({known_option: "value"})
    assert getattr(orc, known_option) == "value"


def test_orc_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        orc = ORC(unknown="abc")
        assert orc.unknown == "abc"

    assert ("Options ['unknown'] are not known by ORC, are you sure they are valid?") in caplog.text


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
