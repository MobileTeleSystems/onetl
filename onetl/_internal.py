"""
    Helpers
"""

from __future__ import annotations

from datetime import datetime
from pathlib import PurePath
from typing import Any

from etl_entities import ProcessStackManager

# e.g. 20220524122150
DATETIME_FORMAT = "%Y%m%d%H%M%S"  # noqa: WPS323


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
        assert uniq_ignore_case(["A", "a", "c"]) == ["A", "c"]
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


def to_camel(string: str) -> str:
    """
    Convert ``snake_case`` strings to ``camelCase`` (with first symbol in lowercase)

    Examples
    --------

    .. code:: python

        assert to_camel("some_value") == "someValue"
    """

    return "".join(word.capitalize() if index > 0 else word for index, word in enumerate(string.split("_")))


def generate_temp_path(root: PurePath) -> PurePath:
    """
    Returns prefix which will be used for creating temp directory

    Returns
    -------
    RemotePath
        Temp path, containing current host name, process name and datetime

    Examples
    --------

    View files

    .. code:: python

        from etl_entities import Process

        from pathlib import Path

        assert generate_temp_path(Path("/tmp")) == Path(
            "/tmp/onetl/currenthost/myprocess/20220524122150",
        )

        with Process(dag="mydag", task="mytask"):
            assert generate_temp_path(Path("/abc")) == Path(
                "/abc/onetl/currenthost/mydag.mytask.myprocess/20220524122150",
            )
    """

    current_process = ProcessStackManager.get_current()
    current_dt = datetime.now().strftime(DATETIME_FORMAT)
    return root / "onetl" / current_process.host / current_process.full_name / current_dt


def get_sql_query(
    table: str,
    columns: list[str] | None = None,
    where: str | None = None,
    hint: str | None = None,
) -> str:
    """
    Generates a SQL query using input arguments
    """

    columns_str = ", ".join(columns) if columns else "*"
    hint = f"/*+ {hint} */" if hint else None
    where = f"WHERE {where}" if where else None

    return " ".join(
        filter(
            None,
            [
                "SELECT",
                hint,
                columns_str,
                "FROM",
                table,
                where,
            ],
        ),
    )
