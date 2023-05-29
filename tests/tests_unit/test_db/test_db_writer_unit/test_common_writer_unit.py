import re
import textwrap

import pytest

from onetl.connection import Hive
from onetl.db import DBWriter


def test_writer_deprecated_import():
    msg = textwrap.dedent(
        """
        This import is deprecated since v0.8.0:

            from onetl.core import DBWriter

        Please use instead:

            from onetl.db import DBWriter
        """,
    )
    with pytest.warns(UserWarning, match=re.escape(msg)):
        from onetl.core import DBWriter as OldDBWriter

        assert OldDBWriter is DBWriter


def test_writer_target_alias(spark_mock):
    writer1 = DBWriter(
        connection=Hive(cluster="rnd-dwh", spark=spark_mock),
        target="schema.table",
    )
    writer2 = DBWriter(
        connection=Hive(cluster="rnd-dwh", spark=spark_mock),
        table="schema.table",
    )

    assert writer1.target == writer2.target
