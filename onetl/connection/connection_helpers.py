from typing import List, Optional
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
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
) -> str:
    """
    Creates a sql query for the dbtable parameter in the jdbc function.
    For instance: spark.read.jdbc(dbtable=sql)
    """

    columns_str = ", ".join(columns) if columns else "*"
    hint = f"/*+ {hint} */" if hint else None
    where = f"WHERE {where}" if where else None

    return " ".join(
        filter(
            None,
            [
                "SELECT",
                hint,
                columns_str,
                f"FROM {table}",
                where,
            ],
        ),
    )
