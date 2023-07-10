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

from __future__ import annotations

import io
import json
import logging
from contextlib import redirect_stdout
from enum import Enum
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Iterable

from deprecated import deprecated

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = logging.getLogger(__name__)
onetl_log = logging.getLogger("onetl")
root_log = logging.getLogger()

HALF_SCREEN_SIZE = 45
BASE_LOG_INDENT = 8
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(threadName)s: %(message)s"
CLIENT_MODULES = {"hdfs", "paramiko", "ftputil", "smbclient"}

DISABLED = 9999  # CRITICAL is 50, we need even higher to disable all logs
logging.addLevelName(DISABLED, "DISABLED")

NOTICE = 5  # DEBUG is 10, we need lower value for less verbose logs even on debug level
logging.addLevelName(NOTICE, "NOTICE")


@deprecated(
    version="0.5.0",
    reason="Will be removed in 1.0.0, use 'setup_logging' instead",
    action="always",
)
def setup_notebook_logging(level: int = logging.INFO) -> None:
    """Set up onETL logging.

    What this function does:
        * Adds stderr logging handler
        * Changes root logger format to ``2023-05-31 11:22:33.456 [INFO]: message``
        * Changes root logger level to ``level``
        * Changes onETL logger level to ``level``
        * Disables loggers of underlying client modules

    .. note::

        Should be used only in IDEs (like Jupyter notebooks or PyCharm),
        or scripts (ETL pipelines).

    .. warning::

        Should **NOT** be used in applications, you should set up logging settings manually,
        according to your framework documentation.

    Parameters
    ----------
    level : ``int`` or ``str``, default ``INFO``
        Log level for onETL module
    """

    setup_logging(level)


def setup_logging(level: int = logging.INFO) -> None:
    """Set up onETL logging.

    What this function does:
        * Adds stderr logging handler
        * Changes root logger format to ``2023-05-31 11:22:33.456 [INFO]: message``
        * Changes root logger level to ``level``
        * Changes onETL logger level to ``level``
        * Disables loggers of underlying client modules

    .. note::

        Should be used only in IDEs (like Jupyter notebooks or PyCharm),
        or scripts (ETL pipelines).

    .. warning::

        Should **NOT** be used in applications, you should set up logging settings manually,
        according to your framework documentation.

    Parameters
    ----------
    level : ``int`` or ``str``, default ``INFO``
        Log level for onETL module
    """

    logging.basicConfig(level=level)
    set_default_logging_format()

    onetl_log.setLevel(level)
    disable_clients_logging()


def disable_clients_logging() -> None:
    """Disables logging of underlying client modules user by onETL.

    Affected modules:
        * ``paramiko``
        * ``hdfs``
        * ``ftputil``
        * ``smbclient``

    .. note::

        Can be used in applications, but it is recommended to disable these loggers
        according to your framework documentation.

    """

    for client_module in CLIENT_MODULES:
        logging.getLogger(client_module).setLevel(DISABLED)


def set_default_logging_format() -> None:
    """Sets default logging format to preferred by onETL.

    Example log message: ``2023-05-31 11:22:33.456 [INFO]: message``

    .. note::

        Should be used only in IDEs (like Jupyter notebooks or PyCharm),
        or scripts (ETL pipelines).

    .. warning::

        Should **NOT** in applications, you should set up logging settings manually,
        according to your framework documentation.
    """

    handlers = onetl_log.handlers or root_log.handlers
    for handler in handlers:
        handler.setFormatter(logging.Formatter(LOG_FORMAT))


def log_with_indent(inp: str, *args, indent: int = 0, level: int = logging.INFO, **kwargs) -> None:
    """Log a message with indentation.

    Supports all positional and keyword arguments which ``logging.log`` support.

    Example
    -------

    .. code:: python

        log_with_indent("message")
        log_with_indent("message with additional %s", "indent", indent=4, level=logging.DEBUG)

    .. code-block:: text

        INFO  onetl.module        message
        DEBUG onetl.module            message with additional indent

    """
    log.log(level, "%s" + inp, " " * (BASE_LOG_INDENT + indent), *args, **kwargs)


def log_lines(inp: str, name: str | None = None, indent: int = 0, level: int = logging.INFO):
    r"""Log input multiline string with indentation.

    Any input indentation is being stripped.

    Does NOT support variable substitution.

    Examples
    --------

    .. code:: python

        log_lines("line1\nline2")
        log_lines("  line1\n      line2\n  line3", level=logging.DEBUG)

    .. code-block:: text

        INFO  onetl.module        line1
        INFO  onetl.module        line2

        DEBUG onetl.module        line1
        DEBUG onetl.module            line2
        DEBUG onetl.module        line3

    """

    base_indent = " " * (BASE_LOG_INDENT + indent)
    for index, line in enumerate(dedent(inp).splitlines()):
        if name and not index:
            log.log(level, "%s%s = %s", base_indent, name, line)
        else:
            log.log(level, "%s%s", base_indent, line)


def log_json(inp: Any, name: str | None = None, indent: int = 0, level: int = logging.INFO):
    """Log input object in JSON notation.

    Does NOT support variable substitution.

    Examples
    --------

    .. code:: python

        log_json(["item1", {"item2": "value2"}, None])

        log_json({"item2": "value2"}, name="myvar", level=logging.DEBUG)

    .. code-block:: text

        INFO  onetl.module        [
        INFO  onetl.module            "item1",
        INFO  onetl.module            {"item2": "value2"},
        INFO  onetl.module            null
        INFO  onetl.module        ]

        DEBUG onetl.module        myvar = {
        DEBUG onetl.module            "item2": "value2"
        DEBUG onetl.module        }

    """

    log_lines(json.dumps(inp, indent=4), name, indent, level)


def log_collection(name: str, collection: Iterable, indent: int = 0, level: int = logging.INFO):
    """Log input collection.

    Does NOT support variable substitution.

    Examples
    --------

    .. code:: python

        log_collection("myvar", ["item1", {"item2": "value2"}, None])

        log_collection("myvar", ["item1", "item2"], level=logging.DEBUG)

    .. code-block:: text

        INFO  onetl.module        myvar = [
        INFO  onetl.module            'item1',
        INFO  onetl.module            {'item2': 'value2'},
        INFO  onetl.module            None,
        INFO  onetl.module        ]

        DEBUG onetl.module        myvar = [
        DEBUG onetl.module            'item1',
        DEBUG onetl.module            'item2',
        DEBUG onetl.module        ]

    """

    base_indent = " " * (BASE_LOG_INDENT + indent)
    nested_indent = " " * (BASE_LOG_INDENT + indent + 4)
    log.log(level, "%s%s = [", base_indent, name)
    for item in collection:
        log.log(level, "%s%r,", nested_indent, item)
    log.log(level, "%s]", base_indent)


def entity_boundary_log(msg: str, char: str = "=") -> None:
    """Prints message with boundary characters.

    Examples
    --------

    .. code:: python

        entity_boundary_log("Begin")
        entity_boundary_log("End", "-")

    .. code-block:: text

        =================== Begin ====================
        ------------------- End ----------------------

    """
    filing = char * (HALF_SCREEN_SIZE - len(msg) // 2)
    log.info("%s %s %s", filing, msg, filing)


def log_options(options: dict | None, indent: int = 0):
    """Log options dict in following format:

    Examples
    --------

    .. code:: python

        log_options(Options(some="value", abc=1, bcd=None, cde=True, feg=SomeEnum.VALUE))
        log_options(None)

    .. code-block:: text

        INFO  onetl.module        options = {
        INFO  onetl.module            'some': 'value',
        INFO  onetl.module            'abc': 1,
        INFO  onetl.module            'bcd': None,
        INFO  onetl.module            'cde': True,
        INFO  onetl.module            'feg': SomeEnum.VALUE,
        INFO  onetl.module        }

        INFO  onetl.module        options = None

    """

    if options:
        log_with_indent("options = {", indent=indent)
        for option, value in options.items():
            value_wrapped = f"'{value}'" if isinstance(value, Enum) else repr(value)
            log_with_indent("%r: %s,", option, value_wrapped, indent=indent + 4)
        log_with_indent("}")
    else:
        log_with_indent("options = %r", None)


def log_dataframe_schema(df: DataFrame):
    """Log dataframe schema in the following format:

    Examples
    --------

    .. code:: python

        log_dataframe_schema(df)

    .. code-block:: text

        root
        |-- age: integer (nullable = true)
        |-- name: string (nullable = true)

    """

    log_with_indent("df_schema:")

    schema_tree = io.StringIO()
    with redirect_stdout(schema_tree):
        # unfortunately, printSchema immediately prints tree instead of returning it
        # so we need a hack
        df.printSchema()

    for line in schema_tree.getvalue().splitlines():
        log_with_indent("%s", line, indent=4)
