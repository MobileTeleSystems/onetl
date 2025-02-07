# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import json
import logging
import sys
from contextlib import redirect_stdout
from enum import Enum
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Collection, Iterable, Mapping, Set

from etl_entities.hwm import HWM
from typing_extensions import deprecated

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

onetl_log = logging.getLogger("onetl")
root_log = logging.getLogger()

HALF_SCREEN_SIZE = 45
BASE_LOG_INDENT = 8
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(threadName)s: %(message)s"
CLIENT_MODULES = {"hdfs", "paramiko", "ftputil", "minio", "webdav3", "pyspark"}

DISABLED = 9999  # CRITICAL is 50, we need even higher to disable all logs
logging.addLevelName(DISABLED, "DISABLED")

NOTICE = 5  # DEBUG is 10, we need lower value for less verbose logs even on debug level
logging.addLevelName(NOTICE, "NOTICE")


@deprecated(
    "Deprecated in 0.5.0 and will be removed in 1.0.0. Use 'setup_logging' instead",
    category=UserWarning,
)
def setup_notebook_logging(level: int | str = logging.INFO) -> None:
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

    .. deprecated:: 0.5.0
        Use :obj:`~setup_logging` instead

    Parameters
    ----------
    level : ``int`` or ``str``, default ``INFO``
        Log level for onETL module
    """

    setup_logging(level)


def setup_logging(level: int | str = logging.INFO, enable_clients: bool = False) -> None:
    """Set up onETL logging.

    What this function does:
        * Adds stderr logging handler
        * Changes root logger format to ``2023-05-31 11:22:33.456 [INFO] MainThread: message``
        * Changes root logger level to ``level``
        * Changes onETL logger level to ``level``
        * Sets up logging level of underlying client modules

    .. note::

        Should be used only in IDEs (like Jupyter notebooks or PyCharm),
        or scripts (ETL pipelines).

    .. versionchanged:: 0.5.0
        Renamed ``setup_notebook_logging`` → ``setup_logging``

    Parameters
    ----------
    level : ``int`` or ``str``, default ``INFO``
        Log level for onETL module

    enable_clients : ``bool``, default ``False``
        If ``True``, enable logging of underlying client modules.
        Otherwise, set client modules log level to ``DISABLED``.

        .. note::

            For ``level="DEBUG"`` it is recommended to use ``enable_clients=True``

        .. versionadded:: 0.9.0
    """

    logging.basicConfig(level=level)
    set_default_logging_format()

    onetl_log.setLevel(level)
    setup_clients_logging(level if enable_clients else DISABLED)


def setup_clients_logging(level: int | str = DISABLED) -> None:
    """Set logging of underlying client modules used by onETL.

    Affected modules:
        * ``ftputil``
        * ``hdfs``
        * ``minio``
        * ``paramiko``
        * ``py4j``
        * ``pyspark``
        * ``webdav3``

    .. note::

        Can be used in applications, but it is recommended to set up these loggers
        according to your framework documentation.

    .. versionchanged:: 0.9.0
        Renamed ``disable_clients_logging`` → ``setup_clients_logging``

    Parameters
    ----------
    level : ``int`` or ``str``, default ``DISABLED``
        Log level for client modules

        .. note::

            For ``py4j``, logging level with maximum verbosity is ``INFO`` because ``DEBUG`` logs are
            totally unreadable.

        .. versionadded:: 0.9.0
    """

    for client_module in CLIENT_MODULES:
        logging.getLogger(client_module).setLevel(level)

    if isinstance(level, str):
        level = int(logging.getLevelName(level))
    logging.getLogger("py4j").setLevel(max(level, logging.INFO))


def set_default_logging_format() -> None:
    """Sets default logging format to preferred by onETL.

    Example log message: ``2023-05-31 11:22:33.456 [INFO] MainThread: message``

    .. note::

        Should be used only in IDEs (like Jupyter notebooks or PyCharm),
        or scripts (ETL pipelines).

    .. warning::

        Should **NOT** be used in applications, you should set up logging settings manually,
        according to your framework documentation.
    """

    handlers = onetl_log.handlers or root_log.handlers
    for handler in handlers:
        handler.setFormatter(logging.Formatter(LOG_FORMAT))


def _log(logger: logging.Logger, msg: str, *args, level: int = logging.INFO, stacklevel: int = 1, **kwargs) -> None:
    if sys.version_info >= (3, 8):
        # https://github.com/python/cpython/pull/7424
        logger.log(level, msg, *args, stacklevel=stacklevel + 1, **kwargs)  # noqa: WPS204
    else:
        logger.log(level, msg, *args, **kwargs)


def log_with_indent(
    logger: logging.Logger,
    inp: str,
    *args,
    indent: int = 0,
    level: int = logging.INFO,
    stacklevel: int = 1,
    **kwargs,
) -> None:
    """Log a message with indentation.

    Supports all positional and keyword arguments which ``logging.log`` support.

    Example
    -------

    .. code:: python

        log_with_indent(logger, "message")
        log_with_indent(
            logger,
            "message with additional %s",
            "indent",
            indent=4,
            level=logging.DEBUG,
        )

    .. code-block:: text

        INFO  onetl.module        message
        DEBUG onetl.module            message with additional indent

    """
    _log(logger, "%s" + inp, " " * (BASE_LOG_INDENT + indent), *args, level=level, stacklevel=stacklevel + 1, **kwargs)


def log_lines(
    logger: logging.Logger,
    inp: str,
    name: str | None = None,
    indent: int = 0,
    level: int = logging.INFO,
    stacklevel: int = 1,
):
    r"""Log input multiline string with indentation.

    Any input indentation is being stripped.

    Does NOT support variable substitution.

    Examples
    --------

    .. code:: python

        log_lines(logger, "line1\nline2")
        log_lines(logger, "  line1\n      line2\n  line3", level=logging.DEBUG)

    .. code-block:: text

        INFO  onetl.module        line1
        INFO  onetl.module        line2

        DEBUG onetl.module        line1
        DEBUG onetl.module            line2
        DEBUG onetl.module        line3

    """

    base_indent = " " * (BASE_LOG_INDENT + indent)
    stacklevel += 1
    for index, line in enumerate(dedent(inp).splitlines()):
        if name and not index:
            _log(logger, "%s%s = %s", base_indent, name, line, level=level, stacklevel=stacklevel)
        else:
            _log(logger, "%s%s", base_indent, line, level=level, stacklevel=stacklevel)


def log_json(
    logger: logging.Logger,
    inp: Any,
    name: str | None = None,
    indent: int = 0,
    level: int = logging.INFO,
    stacklevel: int = 1,
):
    """Log input object in JSON notation.

    Does NOT support variable substitution.

    Examples
    --------

    .. code:: python

        log_json(logger, ["item1", {"item2": "value2"}, None])

        log_json(logger, {"item2": "value2"}, name="myvar", level=logging.DEBUG)

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

    log_lines(logger, json.dumps(inp, indent=4), name, indent, level, stacklevel=stacklevel + 1)


def log_collection(
    logger: logging.Logger,
    name: str,
    collection: Iterable,
    max_items: int | None = None,
    indent: int = 0,
    level: int = logging.INFO,
    stacklevel: int = 1,
):
    """Log input collection.

    Does NOT support variable substitution.

    Examples
    --------

    .. code:: python

        log_collection(logger, "myvar", [])
        log_collection(logger, "myvar", ["item1", {"item2": "value2"}, None])
        log_collection(logger, "myvar", {"item1", "item2", None})
        log_collection(logger, "myvar", {"key1": "value1", "key2": None})
        log_collection(logger, "myvar", ["item1", "item2", "item3"], max_items=1)
        log_collection(
            logger,
            "myvar",
            ["item1", "item2", "item3"],
            max_items=1,
            level=logging.DEBUG,
        )

    .. code-block:: text

        INFO  onetl.module        myvar = []

        INFO  onetl.module        myvar = [
        INFO  onetl.module            'item1',
        INFO  onetl.module            {'item2': 'value2'},
        INFO  onetl.module            None,
        INFO  onetl.module        ]

        INFO  onetl.module        myvar = {
        INFO  onetl.module            'item1',
        INFO  onetl.module            'item2',
        INFO  onetl.module            None,
        INFO  onetl.module        }

        INFO  onetl.module        myvar = {
        INFO  onetl.module            'key1': 'value1',
        INFO  onetl.module            'key2': None,
        INFO  onetl.module        }

        INFO onetl.module        myvar = [
        INFO onetl.module            'item1',
        INFO onetl.module            # ... 2 more items of type <class 'str'>
        INFO onetl.module            # change level to 'DEBUG' to print all values
        INFO onetl.module        ]

        DEBUG onetl.module        myvar = [
        DEBUG onetl.module            'item1',
        DEBUG onetl.module            'item2',
        DEBUG onetl.module            'item3',
        DEBUG onetl.module        ]

    """

    base_indent = " " * (BASE_LOG_INDENT + indent)
    stacklevel += 1

    if not isinstance(collection, (Mapping, Set)):
        collection = list(collection)  # force convert all iterators to list to know size

    start_bracket = "["
    end_bracket = "]"
    if isinstance(collection, (Mapping, Set)):
        start_bracket = "{"
        end_bracket = "}"

    if not collection:
        _log(logger, "%s%s = %s%s", base_indent, name, start_bracket, end_bracket, level=level, stacklevel=stacklevel)
        return

    nested_indent = " " * (BASE_LOG_INDENT + indent + 4)
    _log(logger, "%s%s = %s", base_indent, name, start_bracket, level=level, stacklevel=stacklevel)

    for i, item in enumerate(collection, start=1):
        if max_items and i > max_items and level > logging.DEBUG:
            _log(
                logger,
                "%s# ... %d more items of type %r",
                nested_indent,
                len(collection) - max_items,
                type(item),
                level=level,
                stacklevel=stacklevel,
            )
            _log(
                logger,
                "%s# change level to 'DEBUG' to print all values",
                nested_indent,
                level=level,
                stacklevel=stacklevel,
            )
            break

        if isinstance(collection, Mapping):
            _log(logger, "%s%r: %r,", nested_indent, item, collection[item], level=level, stacklevel=stacklevel)
        else:
            _log(logger, "%s%r,", nested_indent, item, level=level, stacklevel=stacklevel)

    _log(logger, "%s%s", base_indent, end_bracket, level=level, stacklevel=stacklevel)


def entity_boundary_log(logger: logging.Logger, msg: str, char: str = "=", stacklevel: int = 1) -> None:
    """Prints message with boundary characters.

    Examples
    --------

    .. code:: python

        entity_boundary_log(logger, "Begin")
        entity_boundary_log(logger, "End", "-")

    .. code-block:: text

        =================== Begin ====================
        ------------------- End ----------------------

    """
    filing = char * (HALF_SCREEN_SIZE - len(msg) // 2)
    _log(logger, "%s %s %s", filing, msg, filing, stacklevel=stacklevel + 1)


def log_options(
    logger: logging.Logger,
    options: dict | None,
    name: str = "options",
    indent: int = 0,
    stacklevel: int = 1,
    **kwargs,
):
    """Log options dict in following format:

    Examples
    --------

    .. code:: python

        log_options(
            logger,
            Options(some="value", abc=1, bcd=None, cde=True, feg=SomeEnum.VALUE).dict(
                by_alias=True
            ),
        )
        log_options(logger, None)

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

    stacklevel += 1
    if options:
        log_with_indent(logger, "%s = {", name, indent=indent, stacklevel=stacklevel, **kwargs)
        for option, value in options.items():
            value_wrapped = f"'{value}'" if isinstance(value, Enum) else repr(value)
            log_with_indent(
                logger,
                "%r: %s,",
                option,
                value_wrapped,
                indent=indent + 4,
                stacklevel=stacklevel,
                **kwargs,
            )
        log_with_indent(logger, "}", indent=indent, stacklevel=stacklevel, **kwargs)
    else:
        log_with_indent(logger, "%s = %r", name, None, indent=indent, stacklevel=stacklevel, **kwargs)


def log_dataframe_schema(logger: logging.Logger, df: DataFrame, indent: int = 0, stacklevel: int = 1):
    """Log dataframe schema in the following format:

    Examples
    --------

    .. code:: python

        log_dataframe_schema(logger, df)

    .. code-block:: text

        root
        |-- age: integer (nullable = true)
        |-- name: string (nullable = true)

    """

    stacklevel += 1
    log_with_indent(logger, "df_schema:", indent=indent, stacklevel=stacklevel)

    schema_tree = io.StringIO()
    with redirect_stdout(schema_tree):
        # unfortunately, printSchema immediately prints tree instead of returning it
        # so we need a hack
        df.printSchema()

    for line in schema_tree.getvalue().splitlines():
        log_with_indent(logger, "%s", line, indent=indent + 4, stacklevel=stacklevel)


def log_hwm(logger: logging.Logger, hwm: HWM, indent: int = 0, stacklevel: int = 1):
    """Log HWM in the following format:

    Examples
    --------

    .. code:: python

        hwm = ColumnIntHWM(name="my_unique_name", source="my_source", value=123)
        log_hwm(logger, hwm)

    .. code-block:: text

        INFO  onetl.module        hwm = ColumnIntHWM(
        INFO  onetl.module            name = "my_unique_name",
        INFO  onetl.module            entity = "my_source",
        INFO  onetl.module            expression = None,
        INFO  onetl.module            value = 123,
        INFO  onetl.module        )

    .. code-block:: text

        INFO  onetl.module        hwm = FileListHWM(
        INFO  onetl.module            name = "my_unique_name",
        INFO  onetl.module            entity = "my_source",
        INFO  onetl.module            expression = None,
        INFO  onetl.module            value = [
        INFO  onetl.module                AbsolutePath("/some/file1.csv"),
        INFO  onetl.module                AbsolutePath("/some/file2.csv"),
        INFO  onetl.module                AbsolutePath("/some/file3.csv"),
        INFO  onetl.module            ]
        INFO  onetl.module        )
    """
    stacklevel += 1

    log_with_indent(logger, "hwm = %s(", type(hwm).__name__, indent=indent, stacklevel=stacklevel)
    log_with_indent(logger, "name = %r,", hwm.name, indent=indent + 4, stacklevel=stacklevel)
    if hwm.description:
        log_with_indent(logger, "description = %r,", hwm.name, indent=indent + 4, stacklevel=stacklevel)
    log_with_indent(logger, "entity = %r,", hwm.entity, indent=indent + 4, stacklevel=stacklevel)
    log_with_indent(logger, "expression = %r,", hwm.expression, indent=indent + 4, stacklevel=stacklevel)
    if hwm.value is not None:
        if isinstance(hwm.value, Collection):
            log_collection(logger, "value", hwm.value, max_items=10, indent=indent + 4, stacklevel=stacklevel)
        else:
            log_with_indent(logger, "value = %r,", hwm.value, indent=indent + 4, stacklevel=stacklevel)

    log_with_indent(logger, ")", indent=indent, stacklevel=stacklevel)
