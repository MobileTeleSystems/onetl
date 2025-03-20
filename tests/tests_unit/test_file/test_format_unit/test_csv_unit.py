import logging

import pytest

from onetl.file.format import CSV

pytestmark = [pytest.mark.csv]


def test_csv_options_default():
    csv = CSV()
    assert csv.delimiter == ","
    assert csv.quote == '"'
    assert csv.escape == "\\"


def test_csv_options_delimiter_alias():
    csv = CSV(sep=";")
    assert csv.delimiter == ";"


@pytest.mark.parametrize(
    "known_option, value, expected_value",
    [
        ("delimiter", ";", ";"),
        ("quote", "'", "'"),
        ("escape", "#", "#"),
        ("encoding", "value", "value"),
        ("header", True, True),
        ("lineSep", "\r\n", "\r\n"),
        ("charToEscapeQuoteEscaping", "\\", "\\"),
        ("dateFormat", "value", "value"),
        ("emptyValue", "value", "value"),
        ("ignoreLeadingWhiteSpace", True, True),
        ("ignoreTrailingWhiteSpace", True, True),
        ("nullValue", "value", "value"),
        ("timestampFormat", "value", "value"),
        ("timestampNTZFormat", "value", "value"),
        ("columnNameOfCorruptRecord", "value", "value"),
        ("comment", "#", "#"),
        ("enforceSchema", True, True),
        ("inferSchema", True, True),
        ("locale", "value", "value"),
        ("maxCharsPerColumn", 120, 120),
        ("mode", "PERMISSIVE", "PERMISSIVE"),
        ("multiLine", True, True),
        ("nanValue", "value", "value"),
        ("negativeInf", "value", "value"),
        ("positiveInf", "value", "value"),
        ("preferDate", True, True),
        ("samplingRatio", 0.1, 0.1),
        ("unescapedQuoteHandling", "STOP_AT_DELIMITER", "STOP_AT_DELIMITER"),
        ("compression", "gzip", "gzip"),
        ("escapeQuotes", True, True),
        ("quoteAll", True, True),
    ],
)
def test_csv_options_known(known_option, value, expected_value):
    csv = CSV.parse({known_option: value})
    assert getattr(csv, known_option) == expected_value


def test_csv_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        csv = CSV(unknown="abc")
        assert csv.unknown == "abc"

    assert ("Options ['unknown'] are not known by CSV, are you sure they are valid?") in caplog.text


def test_csv_options_repr():
    # There are too many options with default value None, hide them from repr
    csv = CSV(header=True, mode="PERMISSIVE", unknownOption="abc")
    assert repr(csv) == "CSV(escape='\\\\', header=True, mode='PERMISSIVE', quote='\"', sep=',', unknownOption='abc')"
