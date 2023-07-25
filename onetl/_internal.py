#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
    Helpers
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from etl_entities import ProcessStackManager

if TYPE_CHECKING:
    from pathlib import PurePath

# e.g. 20230524122150
DATETIME_FORMAT = "%Y%m%d%H%M%S"


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

    statement = statement.rstrip().lstrip("\n\r").rstrip(";")
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
    already_visited: set[str] = set()

    for orig_value in orig_list:
        if orig_value.casefold() not in already_visited:
            result.append(orig_value)
            already_visited.add(orig_value.casefold())

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
            "/tmp/onetl/currenthost/myprocess/20230524122150",
        )

        with Process(dag="mydag", task="mytask"):
            assert generate_temp_path(Path("/abc")) == Path(
                "/abc/onetl/currenthost/mydag.mytask.myprocess/20230524122150",
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
    compact: bool = False,
) -> str:
    """
    Generates a SQL query using input arguments
    """

    if compact:
        indent = " "
    else:
        indent = os.linesep + " " * 7

    hint = f" /*+ {hint} */" if hint else ""

    columns_str = "*"
    if columns:
        columns_str = indent + f",{indent}".join(column for column in columns)

    if columns_str.strip() == "*":
        columns_str = indent + "*"

    where_str = ""
    if where:
        where_str = "WHERE" + indent + where

    return os.linesep.join(
        [
            f"SELECT{hint}{columns_str}",
            f"FROM{indent}{table}",
            where_str,
        ],
    ).strip()
