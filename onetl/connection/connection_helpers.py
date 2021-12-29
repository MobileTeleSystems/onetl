from typing import Optional
from logging import getLogger


log = getLogger(__name__)

HALF_SCREEN_SIZE = 45
LOG_INDENT = 9


def decorated_log(msg: str, char: str = "="):
    msg = f" {msg} "
    log.info(char * (HALF_SCREEN_SIZE - len(msg) // 2) + msg + char * (HALF_SCREEN_SIZE - len(msg) // 2))  # noqa:WPS221


def get_sql_query(
    table: str,
    hint: Optional[str] = None,
    columns: Optional[str] = "*",
    where: Optional[str] = None,
) -> str:
    """
    Creates a sql query for the dbtable parameter in the jdbc function.
    For instance: spark.read.jdbc(dbtable=sql)
    """
    hint = f"/*+ {hint} */" if hint else None
    where = f"WHERE {where}" if where else None

    statements = [
        "SELECT",
        hint,
        columns,
        f"FROM {table}",
        where,
    ]

    # The elements of the array with the value None are removed
    state: list = [x for x in statements if x]
    return " ".join(state)
