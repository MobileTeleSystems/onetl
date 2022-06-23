from unittest.mock import Mock

import pytest

from onetl.connection import Oracle, Postgres

spark = Mock()


def test_secure_str_and_repr():
    conn = Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=spark)

    assert "password=" not in str(conn)
    assert "password=" not in repr(conn)


def test_jdbc_connection_without_host_and_credentials():
    with pytest.raises(TypeError):
        Postgres(spark=spark)  # noqa: F841


def test_jdbc_default_fetchsize():
    conn = Oracle(host="some_host", user="user", password="passwd", sid="PE", spark=spark)
    options = conn.Options()

    assert options.fetchsize == 100000


def test_jdbc_wrong_mode_option():
    oracle = Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=spark)

    with pytest.raises(ValueError):
        oracle.Options(mode="wrong_mode")  # wrong mode


@pytest.mark.parametrize("hint, real_hint", [(None, ""), ("NOWAIT", " /*+ NOWAIT */")])
@pytest.mark.parametrize(
    "columns, real_columns",
    [
        (None, "*"),
        (["*"], "*"),
        (["d_id", "d_name", "d_age"], "d_id, d_name, d_age"),
    ],
)
@pytest.mark.parametrize("where, real_where", [(None, ""), ("d_id > 100", " WHERE d_id > 100")])
def test_jdbc_get_sql_query(
    hint,
    real_hint,
    columns,
    real_columns,
    where,
    real_where,
):
    conn = Postgres(host="some_host", user="user", password="passwd", database="abc", spark=spark)

    table_sql = conn.get_sql_query(
        table="default.test",
        hint=hint,
        columns=columns,
        where=where,
    )

    assert table_sql == f"SELECT{real_hint} {real_columns} FROM default.test{real_where}"
