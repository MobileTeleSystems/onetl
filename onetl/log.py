from logging import getLogger

log = getLogger(__name__)

HALF_SCREEN_SIZE = 45
LOG_INDENT = 9


def entity_boundary_log(msg: str, char: str = "=") -> None:
    msg = f" {msg} "
    log.info(char * (HALF_SCREEN_SIZE - len(msg) // 2) + msg + char * (HALF_SCREEN_SIZE - len(msg) // 2))  # noqa:WPS221
