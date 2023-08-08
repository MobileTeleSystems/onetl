import re

import pytest

from onetl.file import FileMover
from onetl.impl.file_exist_behavior import FileExistsBehavior


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, FileExistsBehavior.ERROR),
        ({"if_exists": "error"}, FileExistsBehavior.ERROR),
        ({"if_exists": "ignore"}, FileExistsBehavior.IGNORE),
        ({"if_exists": "replace_file"}, FileExistsBehavior.REPLACE_FILE),
        ({"if_exists": "replace_entire_directory"}, FileExistsBehavior.REPLACE_ENTIRE_DIRECTORY),
    ],
)
def test_file_mover_options_if_exists(options, value):
    assert FileMover.Options(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "replace_file"},
            FileExistsBehavior.REPLACE_FILE,
            "Option `FileMover.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `FileMover.Options(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_directory"},
            FileExistsBehavior.REPLACE_ENTIRE_DIRECTORY,
            "Option `FileMover.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `FileMover.Options(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            FileExistsBehavior.REPLACE_FILE,
            "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. " "Use `replace_file` instead",
        ),
        (
            {"mode": "delete_all"},
            FileExistsBehavior.REPLACE_ENTIRE_DIRECTORY,
            "Mode `delete_all` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_directory` instead",
        ),
    ],
)
def test_file_mover_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = FileMover.Options(**options)
        assert options.if_exists == value


def test_file_mover_options_modes_wrong():
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        FileMover.Options(mode="wrong_mode")
