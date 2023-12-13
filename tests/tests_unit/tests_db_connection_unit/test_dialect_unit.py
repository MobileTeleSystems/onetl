import textwrap

import pytest

from onetl.connection import Oracle, Postgres

pytestmark = [pytest.mark.postgres]


@pytest.mark.parametrize(
    "columns",
    [
        None,
        "*",
        ["*"],
        [],
    ],
)
def test_db_dialect_get_sql_query_no_columns(spark_mock, columns):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
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


def test_db_dialect_get_sql_query_columns(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
        table="default.test",
        columns=["*", "d_id", "d_name", "d_age"],
    )
    expected = textwrap.dedent(
        """
        SELECT
               *,
               d_id,
               d_name,
               d_age
        FROM
               default.test
        """,
    ).strip()

    assert result == expected


def test_db_dialect_get_sql_query_columns_oracle(spark_mock):
    conn = Oracle(host="some_host", user="user", sid="database", password="passwd", spark=spark_mock)

    # same as for other databases
    result = conn.dialect.get_sql_query(
        table="default.test",
        columns=["*"],
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

    # but this is different
    result = conn.dialect.get_sql_query(
        table="default.test",
        columns=["*", "d_id", "d_name", "d_age"],
    )
    expected = textwrap.dedent(
        """
        SELECT
               default.test.*,
               d_id,
               d_name,
               d_age
        FROM
               default.test
        """,
    ).strip()

    assert result == expected


def test_db_dialect_get_sql_query_where_string(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
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


def test_db_dialect_get_sql_query_where_list(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
        table="default.test",
        where=["d_id > 100", "d_id < 200"],
    )

    expected = textwrap.dedent(
        """
        SELECT
               *
        FROM
               default.test
        WHERE
               (d_id > 100)
          AND
               (d_id < 200)
        """,
    ).strip()

    assert result == expected


def test_db_dialect_get_sql_query_hint(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
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


def test_db_dialect_get_sql_query_limit(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
        table="default.test",
        limit=5,
    )

    expected = textwrap.dedent(
        """
        SELECT
               *
        FROM
               default.test
        LIMIT
               5
        """,
    ).strip()

    assert result == expected


def test_db_dialect_get_sql_query_limit_0(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
        table="default.test",
        limit=0,
    )

    expected = textwrap.dedent(
        """
        SELECT
               *
        FROM
               default.test
        WHERE
               1 = 0
        """,
    ).strip()

    assert result == expected


def test_db_dialect_get_sql_query_compact_false(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
        table="default.test",
        hint="NOWAIT",
        columns=["d_id", "d_name", "d_age"],
        where=["d_id > 100", "d_id < 200"],
        limit=5,
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
               (d_id > 100)
          AND
               (d_id < 200)
        LIMIT
               5
        """,
    ).strip()

    assert result == expected


def test_db_dialect_get_sql_query_compact_true(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    result = conn.dialect.get_sql_query(
        table="default.test",
        hint="NOWAIT",
        columns=["d_id", "d_name", "d_age"],
        where=["d_id > 100", "d_id < 200"],
        limit=5,
        compact=True,
    )

    expected = textwrap.dedent(
        """
        SELECT /*+ NOWAIT */ d_id, d_name, d_age
        FROM default.test
        WHERE (d_id > 100)
          AND (d_id < 200)
        LIMIT 5
        """,
    ).strip()

    assert result == expected
