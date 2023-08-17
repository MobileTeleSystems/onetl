import textwrap

import pytest

from onetl._internal import get_sql_query


@pytest.mark.parametrize(
    "columns",
    [
        None,
        "*",
        ["*"],
        [],
    ],
)
def test_get_sql_query_no_columns(columns):
    result = get_sql_query(
        table="default.test",
        columns=columns,
    )

    expected = textwrap.dedent(
        """
        SELECT
               *
        FROM
               default.test
        """,
    ).strip()

    assert result == expected


def test_get_sql_query_columns():
    result = get_sql_query(
        table="default.test",
        columns=["d_id", "d_name", "d_age"],
    )
    expected = textwrap.dedent(
        """
        SELECT
               d_id,
               d_name,
               d_age
        FROM
               default.test
        """,
    ).strip()

    assert result == expected


def test_get_sql_query_where():
    result = get_sql_query(
        table="default.test",
        where="d_id > 100",
    )

    expected = textwrap.dedent(
        """
        SELECT
               *
        FROM
               default.test
        WHERE
               d_id > 100
        """,
    ).strip()

    assert result == expected


def test_get_sql_query_hint():
    result = get_sql_query(
        table="default.test",
        hint="NOWAIT",
    )

    expected = textwrap.dedent(
        """
        SELECT /*+ NOWAIT */
               *
        FROM
               default.test
        """,
    ).strip()

    assert result == expected


def test_get_sql_query_compact_false():
    result = get_sql_query(
        table="default.test",
        hint="NOWAIT",
        columns=["d_id", "d_name", "d_age"],
        where="d_id > 100",
        compact=False,
    )

    expected = textwrap.dedent(
        """
        SELECT /*+ NOWAIT */
               d_id,
               d_name,
               d_age
        FROM
               default.test
        WHERE
               d_id > 100
        """,
    ).strip()

    assert result == expected


def test_get_sql_query_compact_true():
    result = get_sql_query(
        table="default.test",
        hint="NOWAIT",
        columns=["d_id", "d_name", "d_age"],
        where="d_id > 100",
        compact=True,
    )

    expected = textwrap.dedent(
        """
        SELECT /*+ NOWAIT */ d_id, d_name, d_age
        FROM default.test
        WHERE d_id > 100
        """,
    ).strip()

    assert result == expected
