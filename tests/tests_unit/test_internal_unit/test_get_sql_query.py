import pytest

from onetl._internal import get_sql_query  # noqa: WPS436


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
    table_sql = get_sql_query(
        table="default.test",
        hint=hint,
        columns=columns,
        where=where,
    )

    assert table_sql == f"SELECT{real_hint} {real_columns} FROM default.test{real_where}"
