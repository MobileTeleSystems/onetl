import logging

import pytest

from onetl.file.format import JSON

pytestmark = [pytest.mark.json]


def test_json_options_default():
    json = JSON()
    assert json.multiLine is True


def test_json_options_cannot_override_multiline():
    with pytest.raises(ValueError):
        JSON(multiLine=False)


def test_json_options_timezone_alias():
    json = JSON(timeZone="value")
    assert json.timezone == "value"


@pytest.mark.parametrize(
    "known_option, value, expected",
    [
        ("encoding", "value", "value"),
        ("lineSep", "\r\n", "\r\n"),
        ("dateFormat", "value", "value"),
        ("timestampFormat", "value", "value"),
        ("timestampNTZFormat", "value", "value"),
        ("allowBackslashEscapingAnyCharacter", True, True),
        ("allowComments", True, True),
        ("allowNonNumericNumbers", True, True),
        ("allowNumericLeadingZeros", True, True),
        ("allowSingleQuotes", True, True),
        ("allowUnquotedControlChars", True, True),
        ("allowUnquotedFieldNames", True, True),
        ("columnNameOfCorruptRecord", "value", "value"),
        ("dropFieldIfAllNull", True, True),
        ("locale", "value", "value"),
        ("mode", "PERMISSIVE", "PERMISSIVE"),
        ("prefersDecimal", True, True),
        ("primitivesAsString", True, True),
        ("samplingRatio", 0.1, 0.1),
    ],
)
def test_json_options_known(known_option, value, expected):
    json = JSON.parse({known_option: value})
    assert getattr(json, known_option) == expected


def test_json_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        json = JSON(unknown="abc")
        assert json.unknown == "abc"

    assert ("Options ['unknown'] are not known by JSON, are you sure they are valid?") in caplog.text


def test_jsonline_options_repr():
    # There are too many options with default value None, hide them from repr
    json = JSON(encoding="UTF-8", mode="PERMISSIVE", unknownOption="abc")
    assert repr(json) == "JSON(encoding='UTF-8', mode='PERMISSIVE', unknownOption='abc')"
