"""
    Helpers
"""

from __future__ import annotations

from typing import Any


def clear_statement(statement: str) -> str:
    """
    Clear unnecessary spaces and semicolons at the statement end.

    Oracle-specific: adds semicolon after END statement.

    Examples
    --------

    .. code:: python

        assert clear_statement("SELECT * FROM mytable") == "SELECT * FROM mytable"
        assert clear_statement("SELECT * FROM mytable ; ") == "SELECT * FROM mytable"
        assert (
            clear_statement("CREATE TABLE mytable (id NUMBER)")
            == "CREATE TABLE mytable (id NUMBER)"
        )
        assert clear_statement("BEGIN ... END") == "BEGIN ... END;"
    """

    statement = statement.rstrip().rstrip(";")

    if statement.lower().strip().endswith("end"):
        statement += ";"

    return statement


def uniq_ignore_case(orig_list: list[str]) -> list[str]:
    """
    Return only uniq values from a list, case ignore.

    Examples
    --------

    .. code:: python

        assert uniq_ignore_case(["a", "c"]) == ["a", "c"]
        assert uniq_ignore_case(["a", "a", "c"]) == ["a", "c"]
        assert uniq_ignore_case(["a", "A", "c"]) == ["a", "c"]
    """

    result: list[str] = []
    result_lower: list[str] = []

    for orig_value in orig_list:
        if orig_value.lower() not in result_lower:
            result.append(orig_value)
            result_lower.append(orig_value.lower())

    return result


def stringify(inp: dict) -> dict[str, Any]:
    """
    Convert all dict keys and values (recursively) to strings.

    Values ``True``, ``False`` and ``None`` become ``"true"``, ``"false"`` and ``"null"``

    Examples
    --------

    .. code:: python

        inp = {"abc": 1, "cde": True, "def": "string"}
        out = {"abc": "1", "cde": "true", "def": "string"}

        assert stringify(inp) == out
    """

    result: dict[str, Any] = {}

    for key, value in inp.items():
        if isinstance(value, dict):
            result[str(key)] = stringify(value)
        else:
            str_val = str(value)
            value_lower = str_val.lower()
            result[str(key)] = str_val

            if value_lower in {"true", "false"}:
                result[str(key)] = value_lower

            if value is None:
                result[str(key)] = "null"

    return result
