import logging
from textwrap import dedent

log = logging.getLogger(__name__)
onetl_log = logging.getLogger("onetl")
root_log = logging.getLogger()

HALF_SCREEN_SIZE = 45
LOG_INDENT = " " * 9
LOG_FORMAT = "{asctime} [{levelname:8s}] {message}"
CLIENT_MODULES = {"hdfs", "paramiko", "ftputil", "smbclient"}

DISABLED = 9999
logging.addLevelName(DISABLED, "DISABLED")


def setup_notebook_logging(level: int = logging.INFO) -> None:
    """Set up onETL logging. Should bese used only in Jupyter notebooks or scripts.

    In application you should set up logging settings manually.

    * Adds stderr logging handler
    * Changes root logger format to ``2022-05-31 11:22:33.456 [INFO]: message``
    * Changes root logger level to ``level``
    * Changes onETL logger level to ``level``
    * Disables loggers of underlying client modules

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
    """Disables logging of underlying client modules user by onETL

    Affected modules:
        * ``paramiko``
        * ``hdfs``
        * ``ftputil``
        * ``smbclient``

    """

    for client_module in CLIENT_MODULES:
        logging.getLogger(client_module).setLevel(DISABLED)


def set_default_logging_format() -> None:
    """Sets default logging format to preferred by onETL

    Example: ``2022-05-31 11:22:33.456 [INFO]: message``

    It is recommended to use this function only in Jupyter notebooks and scripts.

    In application you should set up logging format manually.
    """

    handlers = onetl_log.handlers or root_log.handlers
    for handler in handlers:
        handler.setFormatter(logging.Formatter(LOG_FORMAT, style="{"))


def log_with_indent(inp: str, level: int = logging.INFO) -> None:
    for line in dedent(inp).splitlines():
        log.log(level, LOG_INDENT + line)


def entity_boundary_log(msg: str, char: str = "=") -> None:
    msg = f" {msg} "
    log.info(char * (HALF_SCREEN_SIZE - len(msg) // 2) + msg + char * (HALF_SCREEN_SIZE - len(msg) // 2))  # noqa:WPS221
