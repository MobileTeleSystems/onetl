import logging

import pytest

from onetl.file.format import JSONLine


def test_jsonline_options_default():
    jsonline = JSONLine()
    assert jsonline.encoding == "utf-8"
    assert jsonline.lineSep == "\n"
    assert jsonline.multiLine is False


def test_jsonline_options_default_override():
    jsonline = JSONLine(
        encoding="value",
        lineSep="value",
    )
    assert jsonline.encoding == "value"
    assert jsonline.lineSep == "value"


def test_jsonline_options_cannot_override_multiline():
    with pytest.raises(ValueError):
        JSONLine(multiLine=True)


@pytest.mark.parametrize(
    "known_option",
    [
        "dateFormat",
        "enableDateTimeParsingFallback",
        "timestampFormat",
        "timestampNTZFormat",
        "timeZone",
        "allowBackslashEscapingAnyCharacter",
        "allowComments",
        "allowNonNumericNumbers",
        "allowNumericLeadingZeros",
        "allowSingleQuotes",
        "allowUnquotedControlChars",
        "allowUnquotedFieldNames",
        "columnNameOfCorruptRecord",
        "dropFieldIfAllNull",
        "locale",
        "mode",
        "prefersDecimal",
        "primitivesAsString",
        "samplingRatio",
        "compression",
        "ignoreNullFields",
    ],
)
def test_jsonline_options_known(known_option):
    jsonline = JSONLine.parse({known_option: "value"})
    assert getattr(jsonline, known_option) == "value"


def test_jsonline_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        jsonline = JSONLine(unknown="abc")
        assert jsonline.unknown == "abc"

    assert ("Options ['unknown'] are not known by JSONLine, are you sure they are valid?") in caplog.text
