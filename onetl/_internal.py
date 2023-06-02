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
from math import inf
from typing import TYPE_CHECKING, Any

from etl_entities import ProcessStackManager

if TYPE_CHECKING:
    from pathlib import PurePath

    from pyspark.sql import SparkSession

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


def spark_max_cores_with_config(spark: SparkSession, include_driver: bool = False) -> tuple[int | float, dict]:
    """
    Calculate maximum number of cores which can be used by Spark

    Returns
    -------
    Tuple
        First item is an actual number of cores.
        Second is a dict with config options were used to calculate this value.
    """

    conf = spark.sparkContext.getConf()
    config = {}

    master = conf.get("spark.master", "local")

    if "local" in master:
        # no executors, only driver
        expected_cores = spark._jvm.Runtime.getRuntime().availableProcessors()  # type: ignore # noqa: WPS437
        config["spark.driver.cores"] = expected_cores
    else:
        cores = int(conf.get("spark.executor.cores", "1"))
        config["spark.executor.cores"] = cores

        dynamic_allocation = conf.get("spark.dynamicAllocation.enabled", "false") == "true"
        if dynamic_allocation:
            # https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation
            # We cannot rely on current executors count because this number depends on
            # current load - it increases while performing heavy calculations, and decreases if executors are idle.
            # If user haven't executed anything in current session, number of executors will be 0.
            #
            # Yes, scheduler can refuse to provide executors == maxExecutors, because
            # queue size limit is reached, or other application has higher priority, so executors were preempted.
            # But cluster health cannot rely on a chance, so pessimistic approach is preferred.

            dynamic_executors = conf.get("spark.dynamicAllocation.maxExecutors", "infinity")
            executors_ratio = int(conf.get("spark.dynamicAllocation.executorAllocationRatio", "1"))
            config["spark.dynamicAllocation.maxExecutors"] = dynamic_executors
            executors = inf if dynamic_executors == "infinity" else int(dynamic_executors * executors_ratio)
        else:
            fixed_executors: float = int(conf.get("spark.executor.instances", "0"))
            config["spark.executor.instances"] = fixed_executors
            executors = fixed_executors

        expected_cores = executors * cores
        if include_driver:
            expected_cores += 1

    return expected_cores, config
