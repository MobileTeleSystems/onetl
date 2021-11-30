from typing import Optional
from logging import getLogger

log = getLogger(__name__)

HALF_SCREEN_SIZE = 45


def get_indent(msg):
    return len(f"{msg}") + 2


SPARK_INDENT = get_indent("|Spark|")
ONETL_INDENT = get_indent("|onETL|")


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


def method_runner(method, forbidden_parameter):
    def wrapped(*args, **kwargs):  # noqa: WPS430

        if kwargs.get(forbidden_parameter):
            raise ValueError(f"You passed a forbidden parameter {forbidden_parameter}.")

        return method(*args, **kwargs)

    return wrapped


def attribute_checker(forbidden_parameter):
    def wrapper(cls):
        """
        Checks the validity of the arguments passed to the class methods. If an extra argument is passed,
        an exception is thrown. Checking is needed because the methods of the Hive class have the ability
        to accept any number of parameters using kwargs.
        Parameters
        ----------
        cls

        Returns
        -------

        Check

        """

        class Checker:  # noqa: WPS431
            def __init__(self, *args, **kwargs):
                self._obj = cls(*args, **kwargs)

            def __getattribute__(self, method):

                # if the Checker class method was called then do nothing and return the attribute
                try:
                    x = super().__getattribute__(method)
                except AttributeError:
                    pass  # noqa: WPS420
                else:
                    return x

                attr = self._obj.__getattribute__(method)  # noqa: WPS609

                #  only method calls are processed.
                return method_runner(method=attr, forbidden_parameter=forbidden_parameter)

        return Checker

    return wrapper


def execute_query_without_partitioning(
    parameters: dict,
    spark: "pyspark.sql.SparkSession",
    sql: str,
) -> "pyspark.sql.DataFrame":
    jdbc_options = parameters.copy()
    jdbc_options["table"] = sql
    jdbc_options.pop("numPartitions", None)
    jdbc_options.pop("lowerBound", None)
    jdbc_options.pop("upperBound", None)
    jdbc_options.pop("column", None)

    return spark.read.jdbc(**jdbc_options)
