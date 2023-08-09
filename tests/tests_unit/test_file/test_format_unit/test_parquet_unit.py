import logging

import pytest

from onetl.file.format import Parquet


@pytest.mark.parametrize(
    "known_option",
    [
        "datetimeRebaseMode",
        "int96RebaseMode",
        "mergeSchema",
        "compression",
        "parquet.bloom.filter.enabled#favorite_color",
    ],
)
def test_parquet_options_known(known_option):
    parquet = Parquet.parse({known_option: "value"})
    assert getattr(parquet, known_option) == "value"


def test_parquet_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        parquet = Parquet(unknown="abc")
        assert parquet.unknown == "abc"

    assert ("Options ['unknown'] are not known by Parquet, are you sure they are valid?") in caplog.text


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
