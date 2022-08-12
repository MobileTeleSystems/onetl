from unittest.mock import Mock

import pytest

from onetl.connection import Hive
from onetl.core import DBReader

spark = Mock()


def test_reader_without_schema():
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="table",  # missing schema
        )


def test_reader_with_too_many_dots():
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table.abc",  # wrong input
        )


def test_reader_hive_with_read_options():
    with pytest.raises(TypeError, match=r"Hive does not implement ReadOptions, but \{'some': 'option'\} is passed"):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table",
            options={"some": "option"},
        )


@pytest.mark.parametrize(
    "columns",
    [  # noqa: WPS317
        [],
        (),
        {},
        set(),
        " \t\n",
        [""],
        [" \t\n"],
        ["", "abc"],
        [" \t\n", "abc"],
        "",
        " \t\n",
        ",abc",
        "abc,",
        "cde,,abc",
        "cde, ,abc",
        "*,*,cde",
        "abc,abc,cde",
        "abc,ABC,cde",
        ["*", "*", "cde"],
        ["abc", "abc", "cde"],
        ["abc", "ABC", "cde"],
    ],
)
def test_reader_invalid_columns(columns):
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table",
            columns=columns,
        )


@pytest.mark.parametrize(
    "columns, real_columns",
    [
        ("*", ["*"]),
        ("abc, cde", ["abc", "cde"]),
        ("*, abc", ["*", "abc"]),
        (["*"], ["*"]),
        (["abc", "cde"], ["abc", "cde"]),
        (["*", "abc"], ["*", "abc"]),
    ],
)
def test_reader_valid_columns(columns, real_columns):
    reader = DBReader(
        connection=Hive(spark=spark),
        table="schema.table",
        columns=columns,
    )

    assert reader.columns == real_columns


@pytest.mark.parametrize(
    "hwm_column",
    [  # noqa: WPS317
        "wrong/name",
        "wrong@name",
        "wrong=name",
        "wrong#name",
        [],
        {},
        (),
        set(),
        frozenset(),
        ("name",),
        ["name"],
        {"name"},
        ("wrong/name", "statement"),
        ("wrong@name", "statement"),
        ("wrong=name", "statement"),
        ("wrong#name", "statement"),
        ["wrong/name", "statement"],
        ["wrong@name", "statement"],
        ["wrong=name", "statement"],
        ["wrong#name", "statement"],
        ("wrong/name", "statement", "too", "many"),
        ("wrong@name", "statement", "too", "many"),
        ("wrong=name", "statement", "too", "many"),
        ("wrong#name", "statement", "too", "many"),
        ["wrong/name", "statement", "too", "many"],
        ["wrong@name", "statement", "too", "many"],
        ["wrong=name", "statement", "too", "many"],
        ["wrong#name", "statement", "too", "many"],
        {"wrong/name", "statement", "too", "many"},
        {"wrong@name", "statement", "too", "many"},
        {"wrong=name", "statement", "too", "many"},
        {"wrong#name", "statement", "too", "many"},
        (None, "statement"),
        [None, "statement"],
        # this is the same as hwm_column="name",
        # but if user implicitly passed a tuple
        # both of values should be set to avoid unexpected errors
        ("name", None),
        ["name", None],
    ],
)
def test_reader_invalid_hwm_column(hwm_column):
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table",
            hwm_column=hwm_column,
        )


@pytest.mark.parametrize(
    "hwm_column, real_hwm_column, real_hwm_expression",
    [
        ("hwm_column", "hwm_column", None),  # noqa: WPS317
        (("hwm_column", "expression"), "hwm_column", "expression"),  # noqa: WPS317
        (("hwm_column", "hwm_column"), "hwm_column", "hwm_column"),
    ],
)
def test_reader_valid_hwm_column(hwm_column, real_hwm_column, real_hwm_expression):
    reader = DBReader(
        connection=Hive(spark=spark),
        table="schema.table",
        hwm_column=hwm_column,
    )

    assert reader.hwm_column.name == real_hwm_column
    assert reader.hwm_expression == real_hwm_expression


@pytest.mark.parametrize(
    "columns, hwm_column",
    [  # noqa: WPS317
        (["a", "b", "c", "d"], "d"),
        (["a", "b", "c", "d"], "D"),
        (["a", "b", "c", "D"], "d"),
        ("a, b, c, d", "d"),
        ("a, b, c, d", "D"),
        ("a, b, c, D", "d"),
        (["*", "d"], "d"),
        (["*", "d"], "D"),
        (["*", "D"], "d"),
        ("*, d", "d"),
        ("*, d", "D"),
        ("*, D", "d"),
        (["*"], "d"),
        (["*"], "D"),
        (["*"], ("d", "cast")),
        (["*"], ("D", "cast")),
    ],
)
def test_reader_hwm_column_and_columns_are_not_in_conflict(columns, hwm_column):
    DBReader(
        connection=Hive(spark=spark),
        table="schema.table",
        columns=columns,
        hwm_column=hwm_column,
    )


@pytest.mark.parametrize(
    "columns, hwm_column",
    [  # noqa: WPS317
        (["a", "b", "c", "d"], ("d", "cast")),
        (["a", "b", "c", "d"], ("D", "cast")),
        (["a", "b", "c", "D"], ("d", "cast")),
        ("a, b, c, d", ("d", "cast")),
        ("a, b, c, d", ("D", "cast")),
        ("a, b, c, D", ("d", "cast")),
        (["*", "d"], ("d", "cast")),
        (["*", "d"], ("D", "cast")),
        (["*", "D"], ("d", "cast")),
        ("*, d", ("d", "cast")),
        ("*, d", ("D", "cast")),
        ("*, D", ("d", "cast")),
    ],
)
def test_reader_hwm_column_and_columns_are_in_conflict(columns, hwm_column):
    with pytest.raises(ValueError):
        DBReader(
            connection=Hive(spark=spark),
            table="schema.table",
            columns=columns,
            hwm_column=hwm_column,
        )
