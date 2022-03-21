import logging
from textwrap import dedent

log = logging.getLogger(__name__)

HALF_SCREEN_SIZE = 45
LOG_INDENT = 9


def log_with_indent(inp: str, level: int = logging.INFO) -> None:
    for line in dedent(inp).splitlines():
        log.log(level, " " * LOG_INDENT + line)


def entity_boundary_log(msg: str, char: str = "=") -> None:
    msg = f" {msg} "
    log.info(char * (HALF_SCREEN_SIZE - len(msg) // 2) + msg + char * (HALF_SCREEN_SIZE - len(msg) // 2))  # noqa:WPS221
