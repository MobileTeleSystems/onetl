import os
from datetime import datetime, timezone
from pathlib import PurePath

import pytest

from onetl._util.file import generate_temp_path


@pytest.mark.flaky(reruns=5)
def test_generate_temp_path(mocker):
    root = PurePath("/path")

    dt_prefix = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M")  # up to minutes, not seconds

    mocker.patch("onetl._util.file.get_process_info", return_value=("me", "currenthost"))
    temp_path = os.fspath(generate_temp_path(root))
    expected = os.fspath(root / "onetl" / "currenthost" / "me" / dt_prefix)
    assert temp_path.startswith(expected)
