import re
import textwrap

import pytest

from onetl.file import FileUploader
from onetl.impl.file_exist_behavior import FileExistBehavior


def test_file_uploader_deprecated_import():
    msg = textwrap.dedent(
        """
        This import is deprecated since v0.8.0:

            from onetl.core import FileUploader

        Please use instead:

            from onetl.file import FileUploader
        """,
    )
    with pytest.warns(UserWarning, match=re.escape(msg)):
        from onetl.core import FileUploader as OldFileUploader

        assert OldFileUploader is FileUploader


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, FileExistBehavior.ERROR),
        ({"if_exists": "error"}, FileExistBehavior.ERROR),
        ({"if_exists": "ignore"}, FileExistBehavior.IGNORE),
        ({"if_exists": "replace_file"}, FileExistBehavior.REPLACE_FILE),
        ({"if_exists": "replace_entire_directory"}, FileExistBehavior.REPLACE_ENTIRE_DIRECTORY),
    ],
)
def test_file_uploader_options_if_exists(options, value):
    assert FileUploader.Options(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "replace_file"},
            FileExistBehavior.REPLACE_FILE,
            "Option `FileUploader.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `FileUploader.Options(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_directory"},
            FileExistBehavior.REPLACE_ENTIRE_DIRECTORY,
            "Option `FileUploader.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `FileUploader.Options(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            FileExistBehavior.REPLACE_FILE,
            "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. " "Use `replace_file` instead",
        ),
        (
            {"mode": "delete_all"},
            FileExistBehavior.REPLACE_ENTIRE_DIRECTORY,
            "Mode `delete_all` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_directory` instead",
        ),
    ],
)
def test_file_uploader_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = FileUploader.Options(**options)
        assert options.if_exists == value


def test_file_uploader_options_modes_wrong():
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        FileUploader.Options(mode="wrong_mode")
