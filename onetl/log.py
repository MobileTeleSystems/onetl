#  Copyright 2022 MTS (Mobile Telesystems)
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

import logging
import os
from textwrap import dedent
from typing import Collection

from deprecated import deprecated

log = logging.getLogger(__name__)
onetl_log = logging.getLogger("onetl")
root_log = logging.getLogger()

HALF_SCREEN_SIZE = 45
BASE_LOG_INDENT = 9
LOG_FORMAT = "{asctime} [{levelname:8s}] {message}"
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
        * Changes root logger format to ``2022-05-31 11:22:33.456 [INFO]: message``
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
        * Changes root logger format to ``2022-05-31 11:22:33.456 [INFO]: message``
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

    Example log message: ``2022-05-31 11:22:33.456 [INFO]: message``

    .. note::

        Should be used only in IDEs (like Jupyter notebooks or PyCharm),
        or scripts (ETL pipelines).

    .. warning::

        Should **NOT** in applications, you should set up logging settings manually,
        according to your framework documentation.
    """

    handlers = onetl_log.handlers or root_log.handlers
    for handler in handlers:
        handler.setFormatter(logging.Formatter(LOG_FORMAT, style="{"))


def log_with_indent(inp: str, *args, indent: int = 0, level: int = logging.INFO) -> None:
    for line in dedent(inp).splitlines():
        log.log(level, " " * (BASE_LOG_INDENT + indent) + line, *args)


def log_collection(name: str, value: Collection, indent: int = 4, level: int = logging.INFO):
    log_with_indent(f"{name} = [", level=level)
    log_with_indent(os.linesep.join(f"{item!r}," for item in value), indent=indent, level=level)
    log_with_indent("]", level=level)


def entity_boundary_log(msg: str, char: str = "=") -> None:
    msg = f" {msg} "
    log.info(char * (HALF_SCREEN_SIZE - len(msg) // 2) + msg + char * (HALF_SCREEN_SIZE - len(msg) // 2))  # noqa:WPS221
