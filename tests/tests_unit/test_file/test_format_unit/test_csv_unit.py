import logging

import pytest

from onetl.file.format import CSV


def test_csv_default_options():
    csv = CSV()
    assert csv.delimiter == ","
    assert csv.encoding == "utf-8"
    assert csv.quote == '"'
    assert csv.escape == "\\"
    assert csv.header is False
    assert csv.lineSep == "\n"


def test_csv_default_options_override():
    csv = CSV(
        delimiter="value",
        encoding="value",
        quote="value",
        escape="value",
        header=True,
        lineSep="value",
    )
    assert csv.delimiter == "value"
    assert csv.encoding == "value"
    assert csv.quote == "value"
    assert csv.escape == "value"
    assert csv.header is True
    assert csv.lineSep == "value"


def test_csv_delimiter_alias():
    csv = CSV(sep="value")
    assert csv.delimiter == "value"


@pytest.mark.parametrize(
    "known_option",
    [
        "charToEscapeQuoteEscaping",
        "dateFormat",
        "emptyValue",
        "ignoreLeadingWhiteSpace",
        "ignoreTrailingWhiteSpace",
        "nullValue",
        "timestampFormat",
        "timestampNTZFormat",
        "columnNameOfCorruptRecord",
        "comment",
        "enableDateTimeParsingFallback",
        "enforceSchema",
        "inferSchema",
        "locale",
        "maxCharsPerColumn",
        "maxColumns",
        "mode",
        "multiLine",
        "nanValue",
        "negativeInf",
        "positiveInf",
        "preferDate",
        "samplingRatio",
        "unescapedQuoteHandling",
        "compression",
        "escapeQuotes",
        "quoteAll",
    ],
)
def test_csv_known_options(known_option):
    csv = CSV(**{known_option: "value"})
    assert getattr(csv, known_option) == "value"


def test_csv_unknown_options(caplog):
    with caplog.at_level(logging.WARNING):
        csv = CSV(unknown="abc")
        assert csv.unknown == "abc"

    assert ("Options ['unknown'] are not known by CSV, are you sure they are valid?") in caplog.text
