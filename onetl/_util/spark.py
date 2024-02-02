# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap
from contextlib import contextmanager
from math import inf
from typing import TYPE_CHECKING, Any

from onetl._util.version import Version

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.conf import RuntimeConfig


@contextmanager
def inject_spark_param(conf: RuntimeConfig, name: str, value: Any):
    """
    Inject a parameter into a Spark session, and return previous value after exit.

    If value is ``None``, parameter will be reset to default.
    """
    original_value = conf.get(name, None)

    try:
        conf.unset(name)
        if value is not None:
            conf.set(name, value)

        yield
    finally:
        conf.unset(name)
        if original_value is not None:
            conf.set(name, original_value)


def try_import_pyspark():
    """
    Try to import PySpark, or raise exception if missing.
    """
    try:
        import pyspark  # noqa: F401
    except (ImportError, NameError) as e:
        raise ImportError(
            textwrap.dedent(
                """
                Cannot import module "pyspark".

                You should install package as follows:
                    pip install onetl[spark]

                or inject PySpark to sys.path in some other way BEFORE creating class instance.
                """,
            ).strip(),
        ) from e


def get_pyspark_version() -> Version:
    """
    Get Spark version from ``pyspark.__version__``
    """
    try_import_pyspark()
    import pyspark

    return Version.parse(pyspark.__version__)


def get_spark_version(spark_session: SparkSession) -> Version:
    """
    Get Spark version from active Spark session
    """
    return Version.parse(spark_session.version)


def get_executor_total_cores(spark_session: SparkSession, include_driver: bool = False) -> tuple[int | float, dict]:
    """
    Calculate maximum number of cores which can be used by Spark on all executors.

    Returns
    -------
    Tuple
        First item is an actual number of cores.
        Second is a dict with config options were used to calculate this value.
    """

    conf = spark_session.sparkContext.getConf()
    config = {}

    master = conf.get("spark.master", "local")

    if "local" in master:
        # no executors, only driver
        expected_cores = spark_session._jvm.Runtime.getRuntime().availableProcessors()  # type: ignore
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
