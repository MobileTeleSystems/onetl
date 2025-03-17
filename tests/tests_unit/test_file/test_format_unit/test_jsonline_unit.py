import logging

import pytest

from onetl.file.format import JSONLine

pytestmark = [pytest.mark.json]


def test_jsonline_options_default():
    jsonline = JSONLine()
    assert jsonline.multiLine is False


def test_jsonline_options_cannot_override_multiline():
    with pytest.raises(ValueError):
        JSONLine(multiLine=True)


def test_jsonline_options_timezone_alias():
    json = JSONLine(timeZone="value")
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
        ("compression", "gzip", "gzip"),
        ("ignoreNullFields", True, True),
    ],
)
def test_jsonline_options_known(known_option, value, expected):
    jsonline = JSONLine.parse({known_option: value})
    assert getattr(jsonline, known_option) == expected


def test_jsonline_options_unknown(caplog):
    with caplog.at_level(logging.WARNING):
        jsonline = JSONLine(unknown="abc")
        assert jsonline.unknown == "abc"

    assert ("Options ['unknown'] are not known by JSONLine, are you sure they are valid?") in caplog.text


def test_jsonline_options_repr():
    # There are too many options with default value None, hide them from repr
    json = JSONLine(encoding="UTF-8", mode="PERMISSIVE", unknownOption="abc")
    assert repr(json) == "JSONLine(encoding='UTF-8', mode='PERMISSIVE', unknownOption='abc')"
