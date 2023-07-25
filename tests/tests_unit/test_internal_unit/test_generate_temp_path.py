import os
from datetime import datetime
from pathlib import PurePath

import pytest
from etl_entities import Process

from onetl._internal import generate_temp_path


@pytest.mark.flaky(reruns=5)
def test_generate_temp_path():
    root = PurePath("/path")

    dt_prefix = datetime.now().strftime("%Y%m%d%H%M")  # up to minutes, not seconds

    with Process(name="me", host="currenthost"):
        temp_path = os.fspath(generate_temp_path(root))
        expected = os.fspath(root / "onetl" / "currenthost" / "me" / dt_prefix)
        assert temp_path.startswith(expected)

    with Process(name="me", host="currenthost", dag="abc", task="cde"):
        temp_path = os.fspath(generate_temp_path(root))
        expected = os.fspath(root / "onetl" / "currenthost" / "abc.cde.me" / dt_prefix)
        assert temp_path.startswith(expected)
