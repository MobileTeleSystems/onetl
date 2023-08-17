import logging

import pytest

from onetl.file.format import JSON


def test_json_options_default():
    json = JSON()
    assert json.encoding == "utf-8"
    assert json.lineSep == "\n"
    assert json.multiLine is True


def test_json_options_default_override():
    json = JSON(
        encoding="value",
        lineSep="value",
    )
    assert json.encoding == "value"
    assert json.lineSep == "value"


def test_json_options_cannot_override_multiline():
    with pytest.raises(ValueError):
        JSON(multiLine=False)


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
def test_json_options_known(known_option):
    json = JSON.parse({known_option: "value"})
    assert getattr(json, known_option) == "value"


def test_json_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        json = JSON(unknown="abc")
        assert json.unknown == "abc"

    assert ("Options ['unknown'] are not known by JSON, are you sure they are valid?") in caplog.text
