# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
"""
    Helpers
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from etl_entities.process import ProcessStackManager
from pydantic import SecretStr

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


def stringify(value: Any, quote: bool = False) -> Any:  # noqa: WPS212
    """
    Convert values to strings.

    Values ``True``, ``False`` and ``None`` become ``"true"``, ``"false"`` and ``"null"``.

    If input is dict, return dict with stringified values and keys (recursive).

    If input is list, return list with stringified values (recursive).

    If ``quote=True``, wrap string values with double quotes.

    Examples
    --------

    >>> assert stringify(1) == "1"
    >>> assert stringify(True) == "true"
    >>> assert stringify(False) == "false"
    >>> assert stringify(None) == "null"
    >>> assert stringify("string") == "string"
    >>> assert stringify("string", quote=True) == '"string"'
    >>> assert stringify({"abc": 1}) == {"abc": "1"}
    >>> assert stringify([1, True, False, None, "string"]) == ["1", "true", "false", "null", "string"]
    """

    if isinstance(value, dict):
        return {stringify(k): stringify(v, quote) for k, v in value.items()}

    if isinstance(value, list):
        return [stringify(v, quote) for v in value]

    if value is None:
        return "null"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, SecretStr):
        value = value.get_secret_value()

    if isinstance(value, os.PathLike):
        value = os.fspath(value)

    if isinstance(value, str):
        return f'"{value}"' if quote else value

    return str(value)


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

        from etl_entities.process import Process

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
