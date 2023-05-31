import re
import textwrap

import pytest

from onetl.file import FileUploader


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
